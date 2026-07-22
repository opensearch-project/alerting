## Version 3.8.0 Release Notes

Compatible with OpenSearch and OpenSearch Dashboards version 3.8.0

### Features

* Add filter by backend roles access strategy setting to control how backend role filtering determines access to alerting objects ([#2034](https://github.com/opensearch-project/alerting/pull/2034))

### Enhancements

* Onboard new backport-pr reusable GitHub workflow for alerting ([#2183](https://github.com/opensearch-project/alerting/pull/2183))
* Update maven2 mirror repository URL order ([#2188](https://github.com/opensearch-project/alerting/pull/2188))

### Bug Fixes

* Avoid ScriptService fallback when multi-tenant trigger evaluation is enabled and remote trigger evaluation cannot run due to search input failure ([#2179](https://github.com/opensearch-project/alerting/pull/2179))

### Infrastructure

* Pin GitHub Actions to commit SHAs for supply chain security ([#2156](https://github.com/opensearch-project/alerting/pull/2156))

### Maintenance

* Fix Jackson 3.x version conflict by aligning dependency versions with OpenSearch's jackson3 versions ([#2196](https://github.com/opensearch-project/alerting/pull/2196))
