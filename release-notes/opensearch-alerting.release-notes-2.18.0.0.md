## Version 2.17.0.0 2024-09-04
Compatible with OpenSearch 2.17.0

### Maintenance
* Increment version to 2.18.0-SNAPSHOT. ([#1653](https://github.com/opensearch-project/alerting/pull/1653))

### Refactoring
* Adding Alerting Comments system indices and Security ITs ([#1659](https://github.com/opensearch-project/alerting/pull/1659))
* add logging for remote monitor execution flows ([#1663](https://github.com/opensearch-project/alerting/pull/1663))
* separate doc-level monitor query indices for externally defined monitors ([#1664](https://github.com/opensearch-project/alerting/pull/1664))
* move deletion of query index before its creation ([#1668](https://github.com/opensearch-project/alerting/pull/1668))
* create query index at the time of monitor creation ([#1674](https://github.com/opensearch-project/alerting/pull/1674))


### Bug Fixes
* delete query index only if put mappings throws an exception ([#1685](https://github.com/opensearch-project/alerting/pull/1685))
* optimize bucket level monitor to resolve alias to query only those time-series indices that contain docs within timeframe of range query filter in search input ([#1701](https://github.com/opensearch-project/alerting/pull/1701))
* fix number of shards of query index to 0 and auto expand replicas to 0-1 ([#1702](https://github.com/opensearch-project/alerting/pull/1702))

### Documentation
* Added 2.18.0 release notes. ([#]())