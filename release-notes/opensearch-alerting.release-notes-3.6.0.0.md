## Version 3.6.0 Release Notes

Compatible with OpenSearch and OpenSearch Dashboards version 3.6.0

### Features

* Add configurable `plugins.alerting.monitor.max_triggers` cluster setting to limit the number of triggers per monitor ([#2036](https://github.com/opensearch-project/alerting/pull/2036))

### Enhancements

* Set `cancelAfterTimeInterval` on all remaining `SearchRequest` constructions in monitor runners to prevent premature search cancellation ([#2042](https://github.com/opensearch-project/alerting/pull/2042))
* Limit verbose log output on scheduled migration cancellation to reduce log noise during index creation ([#1738](https://github.com/opensearch-project/alerting/pull/1738))

### Bug Fixes

* Preserve user authentication context when stashing thread context during alert notification sending, fixing SMTP STARTTLS failures ([#2027](https://github.com/opensearch-project/alerting/pull/2027))
* Fix NullPointerException when nested field type has no properties in doc-level monitor creation ([#2049](https://github.com/opensearch-project/alerting/pull/2049))
* Replace `_id` sort with `_seq_no` in JobSweeper to fix fielddata error when `indices.id_field_data.enabled` is false ([#2039](https://github.com/opensearch-project/alerting/pull/2039))

### Infrastructure

* Replace `Thread.sleep` with `OpenSearchTestCase.waitUntil` in integration tests for more reliable test execution ([#2041](https://github.com/opensearch-project/alerting/pull/2041))

### Maintenance

* Change Gradle wrapper from 9.2.0 to 9.4.0 ([#2040](https://github.com/opensearch-project/alerting/pull/2040))
* Update shadow plugin usage to replace deprecated API ([#2022](https://github.com/opensearch-project/alerting/pull/2022))
* Remove experimental PPL alerting feature assets pending refactoring for a future release ([#2017](https://github.com/opensearch-project/alerting/pull/2017))
* Inject SdkClient into transport actions for SDK persistence support ([#2052](https://github.com/opensearch-project/alerting/pull/2052))
* Integrate remote metadata SDK client with alerting plugin ([#2047](https://github.com/opensearch-project/alerting/pull/2047))
* Revert SdkClient changes merged during code freeze ([#2057](https://github.com/opensearch-project/alerting/pull/2057))
