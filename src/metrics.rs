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
    pub webhook_delivery_failures: Counter,
    pub webhook_deliveries_exhausted: Counter,
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
                Histogram::new([
                    1.0, 5.0, 15.0, 30.0, 60.0, 120.0, 300.0, 600.0, 1800.0, 3600.0,
                ])
            });
        let webhook_delivery_failures = Counter::default();
        let webhook_deliveries_exhausted = Counter::default();

        flat_manager_registry.register(
            "jobs",
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
        flat_manager_registry.register(
            "webhook_delivery_failures",
            "Total number of failed webhook delivery attempts.",
            webhook_delivery_failures.clone(),
        );
        flat_manager_registry.register(
            "webhook_deliveries_exhausted",
            "Total number of webhook deliveries that ran out of retries or were rejected.",
            webhook_deliveries_exhausted.clone(),
        );

        Metrics {
            registry,
            jobs_total,
            jobs_in_progress,
            jobs_queued,
            job_duration_seconds,
            webhook_delivery_failures,
            webhook_deliveries_exhausted,
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
    use super::{JobLabels, JobResultLabels, Metrics};

    #[test]
    fn encode_contains_all_metric_families() {
        let metrics = Metrics::new();
        let labels = JobLabels {
            kind: "commit".to_string(),
            repo: "stable".to_string(),
        };
        let result_labels = JobResultLabels {
            kind: "commit".to_string(),
            status: "ended".to_string(),
            repo: "stable".to_string(),
        };
        metrics.jobs_total.get_or_create(&result_labels).inc();
        metrics.jobs_in_progress.get_or_create(&labels).inc();
        metrics.jobs_queued.get_or_create(&labels).inc();
        metrics
            .job_duration_seconds
            .get_or_create(&result_labels)
            .observe(1.0);
        let encoded = metrics.encode();

        assert!(encoded.contains("flat_manager_jobs_total{"));
        assert!(encoded.contains("flat_manager_jobs_in_progress"));
        assert!(encoded.contains("flat_manager_jobs_queued"));
        assert!(encoded.contains("flat_manager_job_duration_seconds"));
        assert!(encoded.contains("flat_manager_webhook_delivery_failures_total 0\n"));
        assert!(encoded.contains("flat_manager_webhook_deliveries_exhausted_total 0\n"));
    }

    #[test]
    fn webhook_counters_are_exposed() {
        let metrics = Metrics::new();
        metrics.webhook_delivery_failures.inc();
        metrics.webhook_delivery_failures.inc();
        metrics.webhook_deliveries_exhausted.inc();
        let encoded = metrics.encode();

        assert!(encoded.contains("flat_manager_webhook_delivery_failures_total 2\n"));
        assert!(encoded.contains("flat_manager_webhook_deliveries_exhausted_total 1\n"));
    }
}
