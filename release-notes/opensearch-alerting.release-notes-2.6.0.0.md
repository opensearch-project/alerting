## Version 2.6.0.0 2023-02-21
Compatible with OpenSearch 2.6.0

### Features
* Multiple indices support in DocLevelMonitorInput ([#784](https://github.com/opensearch-project/alerting/pull/784))

### Maintenance
* [AUTO] Increment version to 2.6.0-SNAPSHOT ([#738](https://github.com/opensearch-project/alerting/pull/738))

### Bug Fixes
* Added document _id as param for terms query when searching alerts by their ids ([#753](https://github.com/opensearch-project/alerting/pull/753))
* fix for ERROR alert state generation in doc-level monitors ([#768](https://github.com/opensearch-project/alerting/pull/768))
* [BUG] ExecuteMonitor inserting metadata doc during dry run ([#758](https://github.com/opensearch-project/alerting/pull/758))
* Adjusting max field index setting dynamically for query index ([#776](https://github.com/opensearch-project/alerting/pull/776))
* Fix setting default title only when no subject has been set ([#750](https://github.com/opensearch-project/alerting/pull/750))

### Infrastructure
* fixed security tests. ([#484](https://github.com/opensearch-project/alerting/pull/484))
* minot fix to prevent flaky tests in downstream plugins ([#804](https://github.com/opensearch-project/alerting/pull/804))
* Publish snapshots to maven via GHA ([#805](https://github.com/opensearch-project/alerting/pull/805))

### Documentation
* Added 2.6 release notes. ([#809](https://github.com/opensearch-project/alerting/pull/809))