pub use flat_manager_common::ostree::*;

use std::path::Path;
use tokio::process::Command;

use crate::jobs::SetsidCommandExt;
pub async fn generate_delta_async(repo_path: &Path, delta: &Delta) -> Result<(), OstreeError> {
    let mut cmd = Command::new("timeout");
    cmd.arg("3600").arg("flatpak");
    SetsidCommandExt::setsid(&mut cmd);

    cmd.arg("build-update-repo")
        .arg("--generate-static-delta-to")
        .arg(delta.to.clone());

    if let Some(ref from) = delta.from {
        cmd.arg("--generate-static-delta-from").arg(from.clone());
    };

    cmd.arg(repo_path);

    log::info!("Generating delta {}", delta);
    let output = cmd.output().await.map_err(|e| {
        OstreeError::ExecFailed("flatpak build-update-repo".to_string(), e.to_string())
    })?;
    result_from_output(output, "flatpak build-update-repo")
}
