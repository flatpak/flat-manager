use serde::Deserialize;
use std::collections::HashMap;
use std::path::Path;
use std::path::PathBuf;
use std::process::Command;

use crate::errors::ApiError;

#[derive(Deserialize, Debug, Clone)]
#[serde(rename_all = "kebab-case", deny_unknown_fields)]
pub struct DeltaConfig {
    pub id: Vec<String>,
    #[serde(default)]
    pub arch: Vec<String>,
    pub depth: u32,
}

impl DeltaConfig {
    pub fn matches_ref(&self, id: &str, arch: &str) -> bool {
        self.id.iter().any(|id_glob| match_glob(id_glob, id))
            && (self.arch.is_empty()
                || self
                    .arch
                    .iter()
                    .any(|arch_glob| match_glob(arch_glob, arch)))
    }
}

#[derive(Deserialize, Debug, Clone)]
#[serde(rename_all = "kebab-case", deny_unknown_fields)]
pub struct SubsetConfig {
    pub collection_id: String,
    pub base_url: Option<String>,
}

/// A command to run during the build/publish process. Given as an array of arguments, with the first argument being
/// the path to the program. Arguments are not processed by a shell; they are passed directly to the program.
#[derive(Deserialize, Debug, Default, Clone)]
pub struct ConfigHook(Vec<String>);

#[derive(Clone, Deserialize, Debug)]
/// A hook that runs after the commit stage. All check hooks must pass for the build to be publishable.
pub struct CheckHook {
    /// The command to run.
    pub command: ConfigHook,

    /// If reviewable is true, an nonzero exit code from the hook will put the check into ReviewRequired state instead
    /// of Failed.
    #[serde(default)]
    pub reviewable: bool,
}

/// Defines a set of hook commands to run at certain points in the build/publish process.
#[derive(Deserialize, Debug, Default, Clone)]
#[serde(rename_all = "kebab-case", deny_unknown_fields)]
pub struct ConfigHooks {
    /// Runs during publish jobs before the build is imported to the main repository. The hook is allowed to edit the
    /// repository. The current directory is set to the build directory.
    pub publish: Option<ConfigHook>,

    #[serde(default)]
    pub checks: HashMap<String, CheckHook>,
}

fn default_depth() -> u32 {
    5
}

#[derive(Deserialize, Debug, Clone)]
#[serde(rename_all = "kebab-case", deny_unknown_fields)]
pub struct RepoConfig {
    #[serde(skip)]
    pub name: String,
    pub suggested_repo_name: Option<String>,
    pub path: PathBuf,
    #[serde(default)]
    pub default_token_type: i32,
    #[serde(default)]
    pub require_auth_for_token_types: Vec<i32>,
    pub collection_id: Option<String>,
    #[serde(default)]
    pub deploy_collection_id: bool,
    pub gpg_key: Option<String>,
    #[serde(skip)]
    pub gpg_key_content: Option<String>,
    pub base_url: Option<String>,
    pub runtime_repo_url: Option<String>,
    pub subsets: HashMap<String, SubsetConfig>,
    pub post_publish_script: Option<String>,
    #[serde(default)]
    pub hooks: ConfigHooks,
    #[serde(default)]
    pub deltas: Vec<DeltaConfig>,
    #[serde(default = "default_depth")]
    pub appstream_delta_depth: u32,
}

fn default_host() -> String {
    "127.0.0.1".to_string()
}

fn default_port() -> i32 {
    8080
}

fn default_numcpu() -> u32 {
    num_cpus::get() as u32
}

#[derive(Deserialize, Debug, Clone)]
#[serde(rename_all = "kebab-case", deny_unknown_fields)]
pub struct Config {
    pub database_url: String,
    #[serde(default = "default_host")]
    pub host: String,
    #[serde(default = "default_port")]
    pub port: i32,
    #[serde(default)]
    pub base_url: String,
    pub gpg_homedir: Option<String>,
    #[serde(deserialize_with = "from_base64")]
    pub secret: Vec<u8>,
    #[serde(default, deserialize_with = "from_opt_base64")]
    pub repo_secret: Option<Vec<u8>>,
    pub repos: HashMap<String, RepoConfig>,
    pub build_repo_base: PathBuf,
    pub build_gpg_key: Option<String>,
    #[serde(skip)]
    pub build_gpg_key_content: Option<String>,
    #[serde(default)]
    pub delay_update_secs: u64,
    #[serde(default = "default_numcpu")]
    pub local_delta_threads: u32,
    pub storefront_info_endpoint: Option<String>,
}

impl ConfigHook {
    /// Creates a Command to execute this hook, if it is defined.
    pub fn build_command<P: AsRef<Path>>(&self, current_dir: P) -> Option<Command> {
        self.0.first().map(|program| {
            let mut command = Command::new(program);
            command.args(self.0.iter().skip(1)).current_dir(current_dir);
            command
        })
    }
}

impl RepoConfig {
    pub fn get_abs_repo_path(&self) -> PathBuf {
        let mut repo_path = std::env::current_dir().unwrap_or_else(|_e| PathBuf::from("/"));

        repo_path.push(&self.path);
        repo_path
    }

    pub fn get_base_url(&self, config: &Config) -> String {
        match &self.base_url {
            Some(base_url) => base_url.clone(),
            None => format!("{}/repo/{}", config.base_url, self.name),
        }
    }

    pub fn get_delta_depth_for_ref(&self, ref_name: &str) -> u32 {
        if ref_name == "ostree-metadata" {
            0
        } else if ref_name.starts_with("appstream/") {
            1 /* The old appstream format doesn't delta well, so not need for depth */
        } else if ref_name.starts_with("appstream2/") {
            self.appstream_delta_depth /* This updates often, so lets have some more */
        } else if ref_name.starts_with("app/") || ref_name.starts_with("runtime/") {
            let parts: Vec<&str> = ref_name.split('/').collect();
            if parts.len() == 4 {
                let id = parts[1];
                let arch = parts[2];
                for dc in &self.deltas {
                    if dc.matches_ref(id, arch) {
                        return dc.depth;
                    }
                }
            };
            0
        } else {
            0 /* weird ref? */
        }
    }
}

impl Config {
    pub fn get_repoconfig(&self, name: &str) -> Result<&RepoConfig, ApiError> {
        self.repos
            .get(name)
            .ok_or_else(|| ApiError::BadRequest("No such repo".to_string()))
    }

    pub fn get_repoconfig_from_path(&self, path: &Path) -> Result<&RepoConfig, ApiError> {
        for (repo, config) in self.repos.iter() {
            if path.starts_with(repo) {
                return Ok(config);
            }
        }
        Err(ApiError::BadRequest("No such repo".to_string()))
    }
}

fn match_glob(glob: &str, s: &str) -> bool {
    if let Some(index) = glob.find('*') {
        let (glob_start, glob_rest) = glob.split_at(index);
        if !s.starts_with(glob_start) {
            return false;
        }
        let (_, s_rest) = s.split_at(index);

        let mut glob_chars = glob_rest.chars();
        let mut s_chars = s_rest.chars();

        /* Consume '*' */
        glob_chars.next();
        let glob_after_star = glob_chars.as_str();

        /* Consume at least one, fail if none */
        if s_chars.next().is_none() {
            return false;
        }

        loop {
            if match_glob(glob_after_star, s_chars.as_str()) {
                return true;
            }
            if s_chars.next().is_none() {
                break;
            }
        }
        false
    } else {
        glob == s
    }
}

fn from_base64<'de, D>(deserializer: D) -> Result<Vec<u8>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    use serde::de::Error;
    String::deserialize(deserializer)
        .and_then(|string| base64::decode(string).map_err(|err| Error::custom(err.to_string())))
}

fn from_opt_base64<'de, D>(deserializer: D) -> Result<Option<Vec<u8>>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    use serde::de::Error;
    String::deserialize(deserializer)
        .and_then(|string| base64::decode(string).map_err(|err| Error::custom(err.to_string())))
        .map(Some)
}

#[cfg(test)]
mod tests {
    // Note this useful idiom: importing names from outer (for mod tests) scope.
    use super::*;

    #[test]
    fn test_glob() {
        assert!(match_glob("foo", "foo"));
        assert!(!match_glob("foo", "fooo"));
        assert!(!match_glob("fooo", "foo"));
        assert!(!match_glob("foo", "br"));
        assert!(!match_glob("foo", "bar"));
        assert!(!match_glob("foo", "baar"));
        assert!(match_glob("*", "foo"));
        assert!(!match_glob("*foo", "foo"));
        assert!(match_glob("fo*", "foo"));
        assert!(match_glob("foo*", "foobar"));
        assert!(!match_glob("foo*gazonk", "foogazonk"));
        assert!(match_glob("foo*gazonk", "foobgazonk"));
        assert!(match_glob("foo*gazonk", "foobagazonk"));
        assert!(match_glob("foo*gazonk", "foobargazonk"));
        assert!(!match_glob("foo*gazonk", "foobargazonkk"));
        assert!(!match_glob("foo*gazonk*test", "foobargazonk"));
        assert!(!match_glob("foo*gazonk*test", "foobargazonktest"));
        assert!(match_glob("foo*gazonk*test", "foobargazonkWOOtest"));
        assert!(!match_glob("foo*gazonk*test", "foobargazonkWOOtestXX"));
    }
}
