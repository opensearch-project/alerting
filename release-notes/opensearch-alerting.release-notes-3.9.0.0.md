## Version 3.9.0 Release Notes

Compatible with OpenSearch and OpenSearch Dashboards version 3.9.0

### Features

* Onboard Alerting plugin to centralized resource authorization ([#2180](https://github.com/opensearch-project/alerting/pull/2180))

### Bug Fixes

* Apply Index Monitor API input validation to the Execute Monitor API ([#2225](https://github.com/opensearch-project/alerting/pull/2225))

### Infrastructure

* Onboard code diff analyzer/reviewer and issue dedupe workflows ([#2185](https://github.com/opensearch-project/alerting/pull/2185))

### Maintenance

* Fix backend build against 3.x snapshot: declare jackson-core direct dependencies and pin httpcore5 ([#2228](https://github.com/opensearch-project/alerting/pull/2228))
* Fix codecoverage upload action ([#2229](https://github.com/opensearch-project/alerting/pull/2229))
* Rename resource sharing feature flag to the non-experimental key ([#2231](https://github.com/opensearch-project/alerting/pull/2231))
