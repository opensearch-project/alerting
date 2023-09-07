## Version 2.10.0.0 2023-09-06
Compatible with OpenSearch 2.10.0

### Maintenance
* Increment version to 2.10.0-SNAPSHOT. ([#1018](https://github.com/opensearch-project/alerting/pull/1018))
* exclude <v32 version of google guava dependency from google java format and add google guava 32.0.1 to resolve CVE CVE-2023-2976 ([#1094](https://github.com/opensearch-project/alerting/pull/1094))

### Feature
* Add workflowIds field in getAlerts API  ([#1014](https://github.com/opensearch-project/alerting/pull/1014))
* add alertId parameter in get chained alert API and paginate associated alerts if alertId param is mentioned ([#1071](https://github.com/opensearch-project/alerting/pull/1071))

### Bug Fixes
* fix get alerts alertState query filter ([#1064](https://github.com/opensearch-project/alerting/pull/1064))

### Infrastructure
* Upgrade the backport workflow ([#1028](https://github.com/opensearch-project/alerting/pull/1029))

### Refactoring
* Update actionGet to SuspendUntil for ClusterMetrics ([#1067](https://github.com/opensearch-project/alerting/pull/1067))
* Resolve compile issues from core changes and update CIs ([#1100](https://github.com/opensearch-project/alerting/pull/1100))

### Documentation
* Added 2.10 release notes ([#1117](https://github.com/opensearch-project/alerting/pull/1117))