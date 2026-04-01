pub use flat_manager_common::ostree::*;

use std::path::Path;
use std::process::Stdio;
use std::time::Duration;
use tokio::process::Command;
use tokio::time::timeout;

pub async fn generate_delta_async(repo_path: &Path, delta: &Delta) -> Result<(), OstreeError> {
    let command = "flatpak build-update-repo";
    let mut cmd = Command::new("flatpak");

    cmd.arg("build-update-repo")
        .arg("--generate-static-delta-to")
        .arg(delta.to.clone());

    if let Some(ref from) = delta.from {
        cmd.arg("--generate-static-delta-from").arg(from.clone());
    };

    cmd.arg(repo_path);
    cmd.stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .kill_on_drop(true);

    log::info!("Generating delta {}", delta);
    let child = cmd
        .spawn()
        .map_err(|e| OstreeError::ExecFailed(command.to_string(), e.to_string()))?;
    let output = timeout(Duration::from_secs(3600), child.wait_with_output())
        .await
        .map_err(|_| {
            OstreeError::CommandFailed(
                command.to_string(),
                format!("Timed out after 3600 seconds while generating delta {delta}"),
            )
        })?
        .map_err(|e| OstreeError::ExecFailed(command.to_string(), e.to_string()))?;
    result_from_output(output, command)
}
