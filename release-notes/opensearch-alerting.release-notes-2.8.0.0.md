## Version 2.8.0.0 2023-05-26
Compatible with OpenSearch 2.8.0

### Feature
* integrate security-analytics & alerting for correlation engine. ([#878](https://github.com/opensearch-project/alerting/pull/878))
* DocLevel Monitor - generate findings when 0 triggers. ([#856](https://github.com/opensearch-project/alerting/pull/856))
* DocLevelMonitor Error Alert - rework. ([#892](https://github.com/opensearch-project/alerting/pull/892))
* Update endtime for DocLevelMonitor Error State Alerts and move them to history index when monitor execution succeeds. ([#905](https://github.com/opensearch-project/alerting/pull/905))
* log error messages and clean up monitor when indexing doc level queries or metadata creation fails. ([#900](https://github.com/opensearch-project/alerting/pull/900))
* Adds transport layer actions for CRUD workflows. ([#934](https://github.com/opensearch-project/alerting/pull/934))

### Maintenance
* Baseline codeowners and maintainers. ([#818](https://github.com/opensearch-project/alerting/pull/818))
* upgrade gradle to 8.1.1. ([#893](https://github.com/opensearch-project/alerting/pull/893))
* Update codeowners and maintainers. ([#899](https://github.com/opensearch-project/alerting/pull/899))
* Updating the CODEOWNERS file with the right format. ([#911](https://github.com/opensearch-project/alerting/pull/911))
* Compile fix - Strings package change. ([#924](https://github.com/opensearch-project/alerting/pull/924))

### Bug Fixes
* Fix getAlerts API for standard Alerting monitors. ([#870](https://github.com/opensearch-project/alerting/issues/870))
* Fixed a bug that prevented alerts from being generated for doc level monitors that use wildcard characters in index names. ([#894](https://github.com/opensearch-project/alerting/issues/894))
* revert to deleting monitor metadata after deleting doc level queries to fix delete monitor regression. ([#931](https://github.com/opensearch-project/alerting/issues/931))

### Documentation
* Added 2.8 release notes. ([#939](https://github.com/opensearch-project/alerting/pull/939))