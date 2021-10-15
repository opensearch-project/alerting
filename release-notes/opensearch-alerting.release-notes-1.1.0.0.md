## Version 1.1.0.0 2021-09-08

Compatible with OpenSearch 1.1.0

### Features

* Add BucketSelector pipeline aggregation extension ([#144](https://github.com/opensearch-project/alerting/pull/144))
* Add AggregationResultBucket ([#148](https://github.com/opensearch-project/alerting/pull/148))
* Add ActionExecutionPolicy ([#149](https://github.com/opensearch-project/alerting/pull/149))
* Refactor Monitor and Trigger to split into Query-Level and Bucket-Level Monitors ([#150](https://github.com/opensearch-project/alerting/pull/150))
* Update InputService for Bucket-Level Alerting ([#152](https://github.com/opensearch-project/alerting/pull/152))
* Update TriggerService for Bucket-Level Alerting ([#153](https://github.com/opensearch-project/alerting/pull/153))
* Update AlertService for Bucket-Level Alerting ([#154](https://github.com/opensearch-project/alerting/pull/154))
* Add worksheets to help with testing ([#151](https://github.com/opensearch-project/alerting/pull/151))
* Update MonitorRunner for Bucket-Level Alerting ([#155](https://github.com/opensearch-project/alerting/pull/155))
* Fix ktlint formatting issues ([#156](https://github.com/opensearch-project/alerting/pull/156))
* Execute Actions on runTrigger exceptions for Bucket-Level Monitor ([#157](https://github.com/opensearch-project/alerting/pull/157))
* Skip execution of Actions on ACKNOWLEDGED Alerts for Bucket-Level Monitors ([#158](https://github.com/opensearch-project/alerting/pull/158))
* Return first page of input results in MonitorRunResult for Bucket-Level Monitor ([#159](https://github.com/opensearch-project/alerting/pull/159))
* Add setting to limit per alert action executions and don't save Alerts for test Bucket-Level Monitors ([#161](https://github.com/opensearch-project/alerting/pull/161))
* Resolve default for ActionExecutionPolicy at runtime ([#165](https://github.com/opensearch-project/alerting/pull/165))

### Bug Fixes

* Removing All Usages of Action Get Method Calls and adding the listeners ([#130](https://github.com/opensearch-project/alerting/pull/130))
* Fix bug in paginating multiple bucket paths for Bucket-Level Monitor ([#163](https://github.com/opensearch-project/alerting/pull/163))
* Various bug fixes for Bucket-Level Alerting ([#164](https://github.com/opensearch-project/alerting/pull/164))
* Return only monitors for /monitors/_search ([#162](https://github.com/opensearch-project/alerting/pull/162))

### Infrastructure

* Add Integtest.sh for OpenSearch integtest setups ([#121](https://github.com/opensearch-project/alerting/pull/121))
* Fix snapshot build and increment to 1.1.0 ([#142](https://github.com/opensearch-project/alerting/pull/142))

### Documentation

* Update Bucket-Level Alerting RFC ([#145](https://github.com/opensearch-project/alerting/pull/145))

### Maintenance

* Remove default assignee ([#127](https://github.com/opensearch-project/alerting/pull/127))

### Refactoring

* Refactor MonitorRunner ([#143](https://github.com/opensearch-project/alerting/pull/143))
