use diesel::pg::PgConnection;
use diesel::prelude::*;
use diesel::result::Error as DieselError;
use log::{error, info};
use std::fmt::Write as _;
use std::process::{Command, Output, Stdio};
use std::str;
use std::sync::Arc;

use crate::config::{Config, RepoConfig};
use crate::errors::{JobError, JobResult};
use crate::metrics::Metrics;
use crate::models::{self, Job};
use crate::schema::*;

use super::job_queue::queue_update_job;

/// We generate flatpakref files for all `app/` refs and for `runtime/` refs that represent
/// actual runtimes or addons/plugins/extensions. The sub-refs that Flatpak generates
/// automatically (`.Debug`, `.Locale`, `.Sources`, `.Docs`) are excluded because they are
/// not directly installable by end-users.
///
/// **Both the commit job and the publish job use this predicate** to decide which refs get a
/// `.flatpakref` file.
pub fn should_generate_flatpakref(ref_name: &str) -> bool {
    const UNWANTED_EXTS: &[&str] = &[".Debug", ".Locale", ".Sources", ".Docs"];
    let ref_id = ref_name.split('/').nth(1).unwrap_or("");

    ref_name.starts_with("app/")
        || (ref_name.starts_with("runtime/")
            && !UNWANTED_EXTS.iter().any(|&ext| ref_id.ends_with(ext)))
}

pub fn generate_flatpakref(
    ref_name: &str,
    maybe_build_id: Option<i32>,
    config: &Config,
    repoconfig: &RepoConfig,
) -> (String, String) {
    let parts: Vec<&str> = ref_name.split('/').collect();

    let filename = if parts[0] == "app" {
        format!("{}.flatpakref", parts[1])
    } else {
        format!("{}.{}.flatpakref", parts[1], parts[3])
    };
    let app_id = &parts[1];
    let branch = &parts[3];

    let is_runtime = if parts[0] == "runtime" {
        "true"
    } else {
        "false"
    };

    let (url, maybe_gpg_content) = match maybe_build_id {
        Some(build_id) => (
            format!("{}/build-repo/{}", config.base_url, build_id),
            &config.build_gpg_key_content,
        ),
        None => (repoconfig.get_base_url(config), &repoconfig.gpg_key_content),
    };

    let title = if let Some(build_id) = maybe_build_id {
        format!("{} build nr {}", parts[1], build_id)
    } else {
        let reponame = match &repoconfig.suggested_repo_name {
            Some(suggested_name) => suggested_name,
            None => &repoconfig.name,
        };
        format!("{app_id} from {reponame}")
    };

    let mut contents = format!(
        r#"[Flatpak Ref]
Name={app_id}
Branch={branch}
Title={title}
IsRuntime={is_runtime}
Url={url}
"#
    );

    /* We only want to deploy the collection ID if the flatpakref is being generated for the main
     * repo not a build repo.
     */
    if let Some(collection_id) = &repoconfig.collection_id {
        if repoconfig.deploy_collection_id && maybe_build_id.is_none() {
            writeln!(contents, "DeployCollectionID={collection_id}").unwrap();
        }
    };

    if maybe_build_id.is_none() {
        if let Some(suggested_name) = &repoconfig.suggested_repo_name {
            writeln!(contents, "SuggestRemoteName={suggested_name}").unwrap();
        }
    }

    if let Some(gpg_content) = maybe_gpg_content {
        writeln!(contents, "GPGKey={gpg_content}").unwrap();
    }

    if let Some(runtime_repo_url) = &repoconfig.runtime_repo_url {
        writeln!(contents, "RuntimeRepo={runtime_repo_url}\n").unwrap();
    }

    (filename, contents)
}

pub fn add_gpg_args(
    cmd: &mut Command,
    maybe_gpg_key: &Option<Vec<String>>,
    maybe_gpg_homedir: &Option<String>,
) {
    if let Some(gpg_homedir) = maybe_gpg_homedir {
        cmd.arg(format!("--gpg-homedir={gpg_homedir}"));
    };

    if let Some(keys) = maybe_gpg_key {
        for key in keys {
            cmd.arg(format!("--gpg-sign={key}"));
        }
    };
}

pub fn job_log(job_id: i32, conn: &mut PgConnection, output: &str) {
    if let Err(e) = diesel::update(jobs::table)
        .filter(jobs::id.eq(job_id))
        .set((jobs::log.eq(jobs::log.concat(&output)),))
        .execute(conn)
    {
        error!("Error appending to job {} log: {}", job_id, e);
    }
}

macro_rules! job_log_and_info {
    ( $job_id:expr, $conn:expr, $output:expr $(,)? ) => {{
        let job_id = $job_id;
        let output = $output;
        info!("#{}: {}", job_id, output);
        crate::jobs::utils::job_log(job_id, $conn, &format!("{output}\n"));
    }};
}

macro_rules! job_log_and_error {
    ( $job_id:expr, $conn:expr, $output:expr $(,)? ) => {{
        let job_id = $job_id;
        let output = $output;
        error!("#{}: {}", job_id, output);
        crate::jobs::utils::job_log(job_id, $conn, &format!("{output}\n"));
    }};
}

/// Executes a command and returns its output. A JobError is returned if the command couldn't be executed, but not if
/// it exits with a status code.
pub fn do_command_with_output(cmd: &mut Command) -> JobResult<Output> {
    let output = cmd
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .output()
        .map_err(|e| JobError::new(&format!("Failed to run {:?}: {}", &cmd, e)))?;

    Ok(output)
}

/// Executes a command. A JobError is returned if the command exits with an unsuccessful status code.
pub fn do_command(mut cmd: Command) -> JobResult<()> {
    let output = do_command_with_output(&mut cmd)?;

    if !output.status.success() {
        return Err(JobError::new(&format!(
            "Command {:?} exited unsuccessfully: {}",
            &cmd,
            String::from_utf8_lossy(&output.stderr)
        )));
    }

    Ok(())
}

pub fn schedule_update_job(
    config: &Config,
    repoconfig: &RepoConfig,
    conn: &mut PgConnection,
    job_id: i32,
    metrics: Option<&Arc<Metrics>>,
) -> Result<Job, DieselError> {
    /* Create update repo job */
    let delay = config.delay_update_secs;
    let (is_new, update_job) =
        queue_update_job(delay, conn, &repoconfig.name, Some(job_id), metrics)?;
    if is_new {
        job_log_and_info!(
            job_id,
            conn,
            &format!(
                "Queued repository update job {}{}",
                update_job.id,
                match delay {
                    0 => "".to_string(),
                    _ => format!(" in {delay} secs"),
                }
            ),
        );
    } else {
        job_log_and_info!(
            job_id,
            conn,
            &format!("Piggy-backed on existing update job {}", update_job.id),
        );
    }

    Ok(update_job)
}

pub struct LoadedBuild<'a> {
    pub build: models::Build,
    pub repoconfig: &'a RepoConfig,
}

/// Loads a build and its associated repo config.
pub fn load_build_and_config<'a>(
    build_id: i32,
    config: &'a Config,
    conn: &mut PgConnection,
) -> JobResult<LoadedBuild<'a>> {
    let build = builds::table
        .filter(builds::id.eq(build_id))
        .get_result::<models::Build>(conn)
        .map_err(|e| JobError::new(&format!("Can't load build: {e}")))?;

    let repoconfig = config
        .get_repoconfig(&build.repo)
        .map_err(|e| JobError::new(&format!("Can't find repo {}: {e}", &build.repo)))?;

    Ok(LoadedBuild { build, repoconfig })
}

/// Loads all refs for a build. Returns an error if there are no refs.
pub fn load_build_refs(build_id: i32, conn: &mut PgConnection) -> JobResult<Vec<models::BuildRef>> {
    let refs = build_refs::table
        .filter(build_refs::build_id.eq(build_id))
        .get_results::<models::BuildRef>(conn)
        .map_err(|e| JobError::new(&format!("Can't load build refs: {e}")))?;

    if refs.is_empty() {
        return Err(JobError::new("No refs in build"));
    }

    Ok(refs)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_config(base_url: &str) -> Config {
        serde_json::from_str(&format!(
            r#"{{
                "database-url": "postgres://example",
                "secret": "c2VjcmV0",
                "repos": {{}},
                "build-repo-base": "/tmp/build-repo",
                "base-url": "{base_url}"
            }}"#
        ))
        .unwrap()
    }

    fn make_repoconfig(name: &str, base_url: Option<&str>) -> RepoConfig {
        let base_url_field = match base_url {
            Some(u) => format!(r#""base-url": "{u}","#),
            None => String::new(),
        };
        let mut repo: RepoConfig = serde_json::from_str(&format!(
            r#"{{
                "path": "/tmp/repo",
                "subsets": {{}},
                {base_url_field}
                "runtime-repo-url": null
            }}"#
        ))
        .unwrap();
        repo.name = name.to_string();
        repo
    }

    #[test]
    fn should_generate_flatpakref_cases() {
        // app/ refs always get a flatpakref
        assert!(should_generate_flatpakref(
            "app/org.gnome.eog/x86_64/stable"
        ));
        assert!(should_generate_flatpakref("app/org.gnome.eog/aarch64/beta"));
        // plain runtime refs get a flatpakref
        assert!(should_generate_flatpakref(
            "runtime/org.gnome.Platform/x86_64/46"
        ));
        // addon / plugin / extension refs get a flatpakref
        assert!(should_generate_flatpakref(
            "runtime/org.gnome.eog.Plugin/x86_64/stable"
        ));
        assert!(should_generate_flatpakref(
            "runtime/com.example.App.Addon/x86_64/stable"
        ));
        // auto-generated sub-refs do not get a flatpakref
        assert!(!should_generate_flatpakref(
            "runtime/org.gnome.eog.Debug/x86_64/stable"
        ));
        assert!(!should_generate_flatpakref(
            "runtime/org.gnome.eog.Locale/x86_64/stable"
        ));
        assert!(!should_generate_flatpakref(
            "runtime/org.gnome.eog.Sources/x86_64/stable"
        ));
        assert!(!should_generate_flatpakref(
            "runtime/org.gnome.eog.Docs/x86_64/stable"
        ));
        // unrelated ref kinds do not get a flatpakref
        assert!(!should_generate_flatpakref(
            "screenshots/org.gnome.eog/x86_64/stable"
        ));
        assert!(!should_generate_flatpakref("appstream/x86_64"));
    }

    #[test]
    fn generate_flatpakref_for_app_ref_in_main_repo() {
        let config = make_config("https://dl.example.com");
        let repoconfig = make_repoconfig("stable", Some("https://dl.example.com/repo/stable"));

        let (filename, contents) = generate_flatpakref(
            "app/org.gnome.eog/x86_64/stable",
            None,
            &config,
            &repoconfig,
        );

        assert_eq!(filename, "org.gnome.eog.flatpakref");
        assert!(contents.contains("Name=org.gnome.eog"));
        assert!(contents.contains("Branch=stable"));
        assert!(contents.contains("IsRuntime=false"));
        assert!(contents.contains("Url=https://dl.example.com/repo/stable"));
        assert!(contents.contains("org.gnome.eog from"));
    }

    #[test]
    fn generate_flatpakref_for_app_ref_in_build_repo() {
        let config = make_config("https://dl.example.com");
        let repoconfig = make_repoconfig("stable", None);

        let (filename, contents) = generate_flatpakref(
            "app/org.gnome.eog/x86_64/stable",
            Some(42),
            &config,
            &repoconfig,
        );

        assert_eq!(filename, "org.gnome.eog.flatpakref");
        assert!(contents.contains("IsRuntime=false"));
        assert!(contents.contains("Url=https://dl.example.com/build-repo/42"));
        assert!(contents.contains("Title=org.gnome.eog build nr 42"));
    }

    #[test]
    fn generate_flatpakref_for_runtime_ref_sets_is_runtime_true() {
        let config = make_config("https://dl.example.com");
        let repoconfig = make_repoconfig("stable", None);

        let (_filename, contents) = generate_flatpakref(
            "runtime/org.gnome.Platform/x86_64/46",
            None,
            &config,
            &repoconfig,
        );

        assert!(contents.contains("IsRuntime=true"));
        assert!(contents.contains("Name=org.gnome.Platform"));
        assert!(contents.contains("Branch=46"));
    }

    #[test]
    fn generate_flatpakref_for_addon_ref_sets_is_runtime_true() {
        let config = make_config("https://dl.example.com");
        let repoconfig = make_repoconfig("stable", None);

        let (filename, contents) = generate_flatpakref(
            "runtime/org.gnome.eog.Plugin/x86_64/stable",
            None,
            &config,
            &repoconfig,
        );

        assert_eq!(filename, "org.gnome.eog.Plugin.stable.flatpakref");
        assert!(contents.contains("IsRuntime=true"));
        assert!(contents.contains("Name=org.gnome.eog.Plugin"));
    }

    #[test]
    fn generate_flatpakref_includes_deploy_collection_id_for_main_repo_only() {
        let config = make_config("https://dl.example.com");
        let mut repoconfig = make_repoconfig("stable", None);
        repoconfig.collection_id = Some("org.example.Repo".to_string());
        repoconfig.deploy_collection_id = true;

        let (_filename, contents) = generate_flatpakref(
            "app/org.gnome.eog/x86_64/stable",
            None,
            &config,
            &repoconfig,
        );
        assert!(contents.contains("DeployCollectionID=org.example.Repo"));

        let (_filename, contents) = generate_flatpakref(
            "app/org.gnome.eog/x86_64/stable",
            Some(1),
            &config,
            &repoconfig,
        );
        assert!(!contents.contains("DeployCollectionID"));
    }

    #[test]
    fn generate_flatpakref_includes_suggested_remote_name_for_main_repo_only() {
        let config = make_config("https://dl.example.com");
        let mut repoconfig = make_repoconfig("stable", None);
        repoconfig.suggested_repo_name = Some("flathub".to_string());

        let (_filename, contents) = generate_flatpakref(
            "app/org.gnome.eog/x86_64/stable",
            None,
            &config,
            &repoconfig,
        );
        assert!(contents.contains("SuggestRemoteName=flathub"));
        assert!(contents.contains("org.gnome.eog from flathub"));

        let (_filename, contents) = generate_flatpakref(
            "app/org.gnome.eog/x86_64/stable",
            Some(7),
            &config,
            &repoconfig,
        );
        assert!(!contents.contains("SuggestRemoteName"));
    }

    #[test]
    fn different_branches_of_same_ref_produce_distinct_filenames() {
        let config = make_config("https://dl.example.com");
        let repoconfig = make_repoconfig("stable", None);

        let (stable_filename, _) = generate_flatpakref(
            "runtime/org.gnome.eog.Plugin/x86_64/stable",
            None,
            &config,
            &repoconfig,
        );
        let (beta_filename, _) = generate_flatpakref(
            "runtime/org.gnome.eog.Plugin/x86_64/beta",
            None,
            &config,
            &repoconfig,
        );

        assert_ne!(stable_filename, beta_filename);
        assert_eq!(stable_filename, "org.gnome.eog.Plugin.stable.flatpakref");
        assert_eq!(beta_filename, "org.gnome.eog.Plugin.beta.flatpakref");
    }
}
