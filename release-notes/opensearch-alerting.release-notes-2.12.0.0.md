## Version 2.12.0.0 2024-02-06
Compatible with OpenSearch 2.12.0

### Maintenance
* Increment version to 2.12.0-SNAPSHOT. ([#1239](https://github.com/opensearch-project/alerting/pull/1239))
* Removed default admin credentials for alerting ([#1399](https://github.com/opensearch-project/alerting/pull/1399))
* ipaddress lib upgrade as part of cve fix ([#1397](https://github.com/opensearch-project/alerting/pull/1397))

### Bug Fixes
* Don't attempt to parse workflow if it doesn't exist ([#1346](https://github.com/opensearch-project/alerting/pull/1346))
* Set docData to empty string if actual is null ([#1325](https://github.com/opensearch-project/alerting/pull/1325))

### Enhancements
* Optimize doc-level monitor execution workflow for datastreams ([#1302](https://github.com/opensearch-project/alerting/pull/1302))
* Inject namedWriteableRegistry during ser/deser of SearchMonitorAction ([#1382](https://github.com/opensearch-project/alerting/pull/1382))

### Refactoring
* Reference get monitor and search monitor action / request / responses from common-utils ([#1315](https://github.com/opensearch-project/alerting/pull/1315))

### Infrastructure
* Fix workflow security tests. ([#1310](https://github.com/opensearch-project/alerting/pull/1310))
* Upgrade to Gradle 8.5 ([#1369](https://github.com/opensearch-project/alerting/pull/1369))

### Documentation
* Added 2.12 release notes ([#1405](https://github.com/opensearch-project/alerting/pull/1405))