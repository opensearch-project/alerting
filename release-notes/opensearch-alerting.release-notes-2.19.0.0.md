## Version 2.19.0.0 2025-02-03
Compatible with OpenSearch 2.19.0

### Maintenance
* Increment version to 2.19.0-SNAPSHOT. ([#1716](https://github.com/opensearch-project/alerting/pull/1716))
* Upgrade to upload-artifact v4 ([#1739](https://github.com/opensearch-project/alerting/pull/1739))

### Refactoring
* optimize execution of workflow consisting of bucket-level followed by doc-level monitors ([#1729](https://github.com/opensearch-project/alerting/pull/1729))

### Bug Fixes
* Force create last run context in monitor workflow metadata when workflow is re-enabled ([#1778](https://github.com/opensearch-project/alerting/pull/1778))
* Fix bucket selector aggregation writeable name. ([#1780](https://github.com/opensearch-project/alerting/pull/1780))

### Documentation
* Added 2.18.0 release notes. ([#1718](https://github.com/opensearch-project/alerting/pull/1718))