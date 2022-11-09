## Version 2.4.0.0 2022-11-04

Compatible with OpenSearch 2.4.0

### Enhancements
* Support multiple data sources ([#558](https://github.com/opensearch-project/alerting/pull/558]))
* Use custom query index in update monitor flow ([#591](https://github.com/opensearch-project/alerting/pull/591]))
* Enhance Get Alerts and Get Findings for list of monitors in bulk ([#590](https://github.com/opensearch-project/alerting/pull/590]))
* Support fetching alerts by list of alert ids in Get Alerts Action ([#608](https://github.com/opensearch-project/alerting/pull/608]))
* Ack alerts - allow moving alerts to history index with custom datasources ([#626](https://github.com/opensearch-project/alerting/pull/626]))
* Enabled parsing of bucket level monitors ([#631](https://github.com/opensearch-project/alerting/pull/631]))
* Custom history indices ([#616](https://github.com/opensearch-project/alerting/pull/616]))
* adds filtering on owner field in search monitor action ([#641](https://github.com/opensearch-project/alerting/pull/641]))
* Support to specify backend roles for monitors ([#635](https://github.com/opensearch-project/alerting/pull/635]))
* Adds findings in bucket level monitor ([#636](https://github.com/opensearch-project/alerting/pull/636]))

### Refactoring
* moving over data models from alerting to common-utils ([#556](https://github.com/opensearch-project/alerting/pull/556]))
* expose delete monitor api from alerting ([#568](https://github.com/opensearch-project/alerting/pull/568]))
* Use findings and alerts models, dtos from common utils ([#569](https://github.com/opensearch-project/alerting/pull/569]))
* Recreate request object from writeable for Get alerts and get findings ([#577](https://github.com/opensearch-project/alerting/pull/577]))
* Use acknowledge alert request,response, actions from common-utils dependencies ([#606](https://github.com/opensearch-project/alerting/pull/606]))
* expose delete monitor api from alerting ([#568](https://github.com/opensearch-project/alerting/pull/568]))
* refactored DeleteMonitor Action to be synchronious ([#628](https://github.com/opensearch-project/alerting/pull/628]))

### Bug Fixes
* add tags to trigger condition of doc-level monitor ([#598](https://github.com/opensearch-project/alerting/pull/598]))
* searchAlert fix ([#613](https://github.com/opensearch-project/alerting/pull/598]))
* Fix Acknowledge Alert Request class loader issue ([#618](https://github.com/opensearch-project/alerting/pull/618]))
* fix alias exists check in findings index creation ([#622](https://github.com/opensearch-project/alerting/pull/622]))
* add tags to trigger condition of doc-level monitor ([#598](https://github.com/opensearch-project/alerting/pull/598]))
* fix for windows ktlint issue ([#585](https://github.com/opensearch-project/alerting/pull/585]))

### Infrastructure
* Disable ktlint for alerting as it has errors on Windows ([#570](https://github.com/opensearch-project/alerting/pull/570]))
* Remove plugin to OS min race condition ([#601](https://github.com/opensearch-project/alerting/pull/601]))

### Documentation
* Add 2.4 release notes ([#646](https://github.com/opensearch-project/alerting/pull/646))