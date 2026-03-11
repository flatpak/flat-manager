use serde::{Deserialize, Serialize};
use std::fmt::Display;
use std::str::FromStr;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum ClaimsScope {
    // Permission to list all jobs in the system. Should not be given to untrusted parties.
    Jobs,
    // Permission to create, list, and purge builds, to get a build's jobs, and to commit uploaded files to the build.
    Build,
    // Permission to upload files and refs to builds.
    Upload,
    // Permission to publish builds.
    Publish,
    // Permission to upload deltas for a repo. Should not be given to untrusted parties.
    Generate,
    // Permission to list builds and to download a build repo.
    Download,
    // Permission to republish an app (take it from the repo, re-run the publish hook, and publish it back). Should not
    // be given to untrusted parties.
    Republish,
    // Permission to change the status of any build check (e.g. mark it as successful, failed, etc.) Should only be
    // given to reviewers or passed to the check scripts themselves.
    ReviewCheck,
    // Permission to get usage information for any token and to revoke any token. Should not be given to untrusted
    // parties.
    TokenManagement,

    #[serde(other)]
    Unknown,
}

impl Display for ClaimsScope {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", format!("{self:?}").to_ascii_lowercase())
    }
}

impl FromStr for ClaimsScope {
    type Err = String;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        let scope: ClaimsScope =
            serde_json::from_value(serde_json::Value::String(value.to_string()))
                .map_err(|_| format!("Unknown scope '{value}'"))?;
        if scope == ClaimsScope::Unknown {
            Err(format!("Unknown scope '{value}'"))
        } else {
            Ok(scope)
        }
    }
}

/* Claims are used in two forms, one for API calls, and one for
 * general repo access, the second one is simpler and just uses scope
 * for the allowed ids, and sub means the user doing the access (which
 * is not verified). */
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Claims {
    pub name: Option<String>,
    pub sub: String, // "build", "build/N", user id for repo tokens, or "" for certain management tokens
    pub exp: i64,
    pub jti: Option<String>, // an unique ID for the token, for revocation.

    #[serde(default)]
    pub scope: Vec<ClaimsScope>,
    #[serde(default)]
    pub prefixes: Vec<String>, // [''] => all, ['org.foo'] => org.foo + org.foo.bar (but not org.foobar)
    #[serde(default)]
    pub apps: Vec<String>, // like prefixes, but only exact matches
    #[serde(default)]
    pub repos: Vec<String>, // list of repo names or a '' for match all
    #[serde(default)]
    pub branches: Vec<String>, // list of allowed branches or a '' for match all
    #[serde(default)]
    pub token_type: Option<String>, // "app" to require at least one app ref
}

pub fn sub_has_prefix(required_sub: &str, claimed_sub: &str) -> bool {
    // Matches using a path-prefix style comparison:
    //  claimed_sub == "build" should match required_sub == "build" or "build/N[/...]"
    //  claimed_sub == "build/N" should only matchs required_sub == "build/N[/...]"
    if let Some(rest) = required_sub.strip_prefix(claimed_sub) {
        if rest.is_empty() || rest.starts_with('/') {
            return true;
        }
    };
    false
}

pub fn id_matches_prefix(id: &str, prefix: &str) -> bool {
    if prefix.is_empty() {
        return true;
    }
    if let Some(rest) = id.strip_prefix(prefix) {
        if rest.is_empty() || rest.starts_with('.') {
            return true;
        }
    };
    false
}

pub fn id_matches_one_prefix(id: &str, prefixes: &[String]) -> bool {
    prefixes.iter().any(|prefix| id_matches_prefix(id, prefix))
}

pub fn repo_matches_claimed(repo: &str, claimed_repo: &str) -> bool {
    if claimed_repo.is_empty() {
        return true;
    }
    repo == claimed_repo
}

pub fn repo_matches_one_claimed(repo: &str, claimed_repos: &[String]) -> bool {
    claimed_repos
        .iter()
        .any(|claimed_repo| repo_matches_claimed(repo, claimed_repo))
}
