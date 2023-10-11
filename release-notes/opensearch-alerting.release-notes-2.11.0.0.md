## Version 2.11.0.0 2023-10-11
Compatible with OpenSearch 2.11.0

### Maintenance
* Increment version to 2.11.0-SNAPSHOT. ([#1116](https://github.com/opensearch-project/alerting/pull/1116))

### Bug Fixes
* Fix workflow execution for first run. ([#1227](https://github.com/opensearch-project/alerting/pull/1227))

### Enhancements
* Add logging for execution and indexes of monitors and workflows. ([#1223](https://github.com/opensearch-project/alerting/pull/1223))

### Refactoring
* Optimize doc-level monitor workflow for index patterns. ([#1122](https://github.com/opensearch-project/alerting/pull/1122))
* Add workflow null or empty check only when empty workflow id passed. ([#1139(https://github.com/opensearch-project/alerting/pull/1139))
* Add primary first calls for different monitor types. ([#1205](https://github.com/opensearch-project/alerting/pull/1205))

### Infrastructure
* Ignore flaky security test suites. ([#1188](https://github.com/opensearch-project/alerting/pull/1188))

### Documentation
* Added 2.11 release notes ([#1251](https://github.com/opensearch-project/alerting/pull/1251))