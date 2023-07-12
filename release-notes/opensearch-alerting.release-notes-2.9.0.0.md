## Version 2.9.0.0 2023-07-11
Compatible with OpenSearch 2.9.0

### Maintenance
* Increment version to 2.9.0-SNAPSHOT. ([#950](https://github.com/opensearch-project/alerting/pull/950))

### Feature
* Adds transport layer actions for CRUD workflows. ([#934](https://github.com/opensearch-project/alerting/pull/934))
* Added rest layer for the workflow. ([#963](https://github.com/opensearch-project/alerting/pull/963))
* [BucketLevelMonitor] Multi-term agg support. ([#964](https://github.com/opensearch-project/alerting/pull/964))
* Check if AD backend role is enabled. ([#968](https://github.com/opensearch-project/alerting/pull/968))
* Add workflow_id field in alert mapping json. ([#969](https://github.com/opensearch-project/alerting/pull/969))
* Adds chained alerts. ([#976](https://github.com/opensearch-project/alerting/pull/976))
* Implemented support for configuring a cluster metrics monitor to call cat/indices, and cat/shards. ([#992](https://github.com/opensearch-project/alerting/pull/992))

### Bug Fixes
* Fix schema version in tests and delegate monitor metadata construction in tests. ([#948](https://github.com/opensearch-project/alerting/pull/948))
* Fixed search monitor API to return alert counts. ([#978](https://github.com/opensearch-project/alerting/pull/978))
* Resolve string issues from core. ([#987](https://github.com/opensearch-project/alerting/pull/987))
* Fix getAlerts RBAC problem. ([#991](https://github.com/opensearch-project/alerting/pull/991))
* Fix alert constructor with noop trigger to use execution id and workflow id. ([#994](https://github.com/opensearch-project/alerting/pull/994))

### Refactoring
* Use strong password in security test. ([#933](https://github.com/opensearch-project/alerting/pull/933))

### Documentation
* Added 2.9 release notes. ([#1010](https://github.com/opensearch-project/alerting/pull/1010))