## Version 2.13.0.0 2024-03-19
Compatible with OpenSearch 2.13.0

### Maintenance
* bump up the version to 2.13 (#[1460](https://github.com/opensearch-project/alerting/pull/1460))

### Bug Fixes
* Add an _exists_ check to document level monitor queries (#[1425](https://github.com/opensearch-project/alerting/pull/1425)) (#[1456](https://github.com/opensearch-project/alerting/pull/1456))
* optimize sequence number calculation and reduce search requests in doc level monitor execution (#[1445](https://github.com/opensearch-project/alerting/pull/1455))
* Clean up doc level queries on dry run (#[1430](https://github.com/opensearch-project/alerting/pull/1430))
* optimize to fetch only fields relevant to doc level queries in doc level monitor instead of entire _source for each doc #[1441](https://github.com/opensearch-project/alerting/pull/1441)
* Add jvm aware setting and max num docs settings for batching docs for percolate queries #[1435](https://github.com/opensearch-project/alerting/pull/1435)
* fix for MapperException[the [enabled] parameter can't be updated for the object mapping [metadata.source_to_query_index_mapping] (#[1432](https://github.com/opensearch-project/alerting/pull/1432)) (#[1434](https://github.com/opensearch-project/alerting/pull/1434))


### Enhancements
* Enhance per bucket, and per document monitor notification message ctx. (#[1450](https://github.com/opensearch-project/alerting/pull/1450)) (#[1477](https://github.com/opensearch-project/alerting/pull/1477))
* Findings API Enhancements changes and integ tests fix (#[1464](https://github.com/opensearch-project/alerting/pull/1464)) (#[1474](https://github.com/opensearch-project/alerting/pull/1474))
* Feature findings enhancement (#[1427](https://github.com/opensearch-project/alerting/pull/1427)) (#[1457](https://github.com/opensearch-project/alerting/pull/1457))
* add distributed locking to jobs in alerting (#[1403](https://github.com/opensearch-project/alerting/pull/1403)) (#[1458](https://github.com/opensearch-project/alerting/pull/1458))

### Documentation
* Added 2.13 release notes (#[1483](https://github.com/opensearch-project/alerting/pull/1483))