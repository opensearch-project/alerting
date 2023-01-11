## Version 2.5.0.0 2023-01-10
Compatible with OpenSearch 2.5.0

### Maintenance
* [AUTO] Increment version to 2.5.0-SNAPSHOT ([#629](https://github.com/opensearch-project/alerting/pull/629))

### Refactoring
* Added exception check once the .opendistro-alerting-config index is being created. ([#650](https://github.com/opensearch-project/alerting/pull/650))
* Set lastnotification time as created time for new bucket level monitor. ([#675](https://github.com/opensearch-project/alerting/pull/675))
* Separate variables for alert and finding scheduled rollovers. ([#705](https://github.com/opensearch-project/alerting/pull/705))
* Overriding defaultTitle with subject for SNS notifications. ([#708](https://github.com/opensearch-project/alerting/pull/708))
* QueryIndex rollover when field mapping limit is reached. ([#725](https://github.com/opensearch-project/alerting/pull/725))
* Added unwrapping exceptions from core. ([#728](https://github.com/opensearch-project/alerting/pull/728)) 

### Bug Fixes
* Added support for "nested" mappings. ([#645](https://github.com/opensearch-project/alerting/pull/645))
* Create findingIndex bugfix. ([#653](https://github.com/opensearch-project/alerting/pull/653))
* Fix bucket level monitor findings to support term aggs in query. ([#666](https://github.com/opensearch-project/alerting/pull/666))
* Mappings traversal bug fix. ([#669](https://github.com/opensearch-project/alerting/pull/669))
* Set message when building LegacyCustomWebhookMessage. ([#670](https://github.com/opensearch-project/alerting/pull/670))
* Fix error message bug to show the correct destination ID that's missing. ([#685](https://github.com/opensearch-project/alerting/pull/685))
* Fix percolator mapping error when having field name 'type'. ([#726](https://github.com/opensearch-project/alerting/pull/726))

### Documentation
* Added 2.5 release notes. ([#743](https://github.com/opensearch-project/alerting/pull/743))