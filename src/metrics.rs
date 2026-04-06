use prometheus_client::encoding::{text::encode, EncodeLabelSet};
use prometheus_client::metrics::{
    counter::Counter, family::Family, gauge::Gauge, histogram::Histogram,
};
use prometheus_client::registry::Registry;

use crate::models::JobKind;

#[derive(Clone, Debug, Hash, PartialEq, Eq, EncodeLabelSet)]
pub struct JobLabels {
    pub kind: String,
    pub repo: String,
}

#[derive(Clone, Debug, Hash, PartialEq, Eq, EncodeLabelSet)]
pub struct JobResultLabels {
    pub kind: String,
    pub status: String,
    pub repo: String,
}

pub struct Metrics {
    pub registry: Registry,
    pub jobs_total: Family<JobResultLabels, Counter>,
    pub jobs_in_progress: Family<JobLabels, Gauge>,
    pub jobs_queued: Family<JobLabels, Gauge>,
    pub job_duration_seconds: Family<JobResultLabels, Histogram>,
}

impl Metrics {
    pub fn new() -> Self {
        let mut registry = Registry::default();
        let flat_manager_registry = registry.sub_registry_with_prefix("flat_manager");

        let jobs_total = Family::<JobResultLabels, Counter>::default();
        let jobs_in_progress = Family::<JobLabels, Gauge>::default();
        let jobs_queued = Family::<JobLabels, Gauge>::default();
        let job_duration_seconds =
            Family::<JobResultLabels, Histogram>::new_with_constructor(|| {
                Histogram::new(
                    [
                        1.0, 5.0, 15.0, 30.0, 60.0, 120.0, 300.0, 600.0, 1800.0, 3600.0,
                    ]
                    .into_iter(),
                )
            });

        flat_manager_registry.register(
            "jobs_total",
            "Total number of finished jobs.",
            jobs_total.clone(),
        );
        flat_manager_registry.register(
            "jobs_in_progress",
            "Current number of jobs being processed.",
            jobs_in_progress.clone(),
        );
        flat_manager_registry.register(
            "jobs_queued",
            "Current number of queued jobs.",
            jobs_queued.clone(),
        );
        flat_manager_registry.register(
            "job_duration_seconds",
            "Duration of finished jobs in seconds.",
            job_duration_seconds.clone(),
        );

        Metrics {
            registry,
            jobs_total,
            jobs_in_progress,
            jobs_queued,
            job_duration_seconds,
        }
    }

    pub fn encode(&self) -> String {
        let mut encoded = String::new();
        encode(&mut encoded, &self.registry).expect("Failed to encode Prometheus metrics");
        encoded
    }
}

pub fn repo_label(repo: &Option<String>) -> String {
    repo.clone().unwrap_or_else(|| "global".to_string())
}

pub fn kind_label(kind: i16) -> String {
    match JobKind::from_db(kind) {
        Some(JobKind::Commit) => "commit",
        Some(JobKind::Publish) => "publish",
        Some(JobKind::UpdateRepo) => "update_repo",
        Some(JobKind::Republish) => "republish",
        Some(JobKind::Check) => "check",
        Some(JobKind::Prune) => "prune",
        None => "unknown",
    }
    .to_string()
}

#[cfg(test)]
mod tests {
    use super::Metrics;

    #[test]
    fn encode_contains_all_metric_families() {
        let metrics = Metrics::new();
        let encoded = metrics.encode();

        assert!(encoded.contains("flat_manager_jobs_total"));
        assert!(encoded.contains("flat_manager_jobs_in_progress"));
        assert!(encoded.contains("flat_manager_jobs_queued"));
        assert!(encoded.contains("flat_manager_job_duration_seconds"));
    }
}
