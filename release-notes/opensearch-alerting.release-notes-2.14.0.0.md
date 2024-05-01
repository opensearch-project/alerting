## Version 2.14.0.0 2024-04-30
Compatible with OpenSearch 2.14.0

### Maintenance
* Increment version to 2.14.0-SNAPSHOT. ([#1492](https://github.com/opensearch-project/alerting/pull/1492))

### Bug Fixes
* Fix fieldLimit exception in docLevelMonitor. ([#1368](https://github.com/opensearch-project/alerting/pull/1368))

### Enhancements
* Adding tracking_total_hits in search query for findings. ([#1487](https://github.com/opensearch-project/alerting/pull/1487))

### Refactoring
* Removed log entry regarding destination migration. ([#1356](https://github.com/opensearch-project/alerting/pull/1356))
* Set the cancelAfterTimeInterval parameter on SearchRequest object in all MonitorRunners. ([#1366](https://github.com/opensearch-project/alerting/pull/1366))
* Add validation check for doc level query name during monitor creation. ([#1506](https://github.com/opensearch-project/alerting/pull/1506))
* Added input validation, and fixed bug for cross cluster monitors. ([#1510](https://github.com/opensearch-project/alerting/pull/1510))
* Doc-level monitor fan-out approach ([#1521](https://github.com/opensearch-project/alerting/pull/1521))

### Infrastructure
* Adjusted maven publish workflow to execute automatically when merging a PR. ([#1531](https://github.com/opensearch-project/alerting/pull/1531))

### Documentation
* Dev guide update to include building/using local os-min distro. ([#757](https://github.com/opensearch-project/alerting/pull/757))
* Added 2.14 release notes. ([#1534](https://github.com/opensearch-project/alerting/pull/1534))