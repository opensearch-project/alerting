## Version 2.7.0.0 2023-04-17
Compatible with OpenSearch 2.7.0

### Maintenance
* Increment version to 2.7. ([#823](https://github.com/opensearch-project/alerting/pull/823))

### Refactoring
* Revert enabled field in source_to_query_index_mapping. ([#812](https://github.com/opensearch-project/alerting/pull/812))
* Fixed xContent dependencies due to OSCore changes. ([#839](https://github.com/opensearch-project/alerting/pull/839))

### Bug Fixes
* Issue with percolate query transforming documents with object type fields. ([#844](https://github.com/opensearch-project/alerting/issues/844))
* Notification security fix. ([#852](https://github.com/opensearch-project/alerting/pull/852))

### Infrastructure
* Use notification snapshot for integ test. ([#822](https://github.com/opensearch-project/alerting/pull/822))
* Use latest common-utils snapshot. ([#858](https://github.com/opensearch-project/alerting/pull/858))

### Documentation
* Added 2.7 release notes. ([#864](https://github.com/opensearch-project/alerting/pull/864))