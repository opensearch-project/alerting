## Version 1.2.0.0 2021-11-05

Compatible with OpenSearch 1.2.0

### Enhancements

* Admin Users must be able to access all monitors #139 ([#220](https://github.com/opensearch-project/alerting/pull/220))
* Add valid search filters. ([#194](https://github.com/opensearch-project/alerting/pull/194))

### Bug Fixes

* Fixed a bug that was preventing the AcknowledgeAlerts API from acknowledging more than 10 alerts at once. ([#205](https://github.com/opensearch-project/alerting/pull/205))
* Remove user from the responses ([#201](https://github.com/opensearch-project/alerting/pull/201))

### Infrastructure

* Update build to use public Maven repo ([#184](https://github.com/opensearch-project/alerting/pull/184))
* Publish notification JARs checksums. ([#196](https://github.com/opensearch-project/alerting/pull/196))
* Updates testCompile mockito version to match OpenSearch changes ([#204](https://github.com/opensearch-project/alerting/pull/204))
* Update maven publication to include cksums. ([#224](https://github.com/opensearch-project/alerting/pull/224))
* Updates alerting version to 1.2 ([#192](https://github.com/opensearch-project/alerting/pull/192))

### Documentation

* Add release notes for 1.2.0.0 release ([#225](https://github.com/opensearch-project/alerting/pull/225))

### Maintenance

* Update copyright notice ([#222](https://github.com/opensearch-project/alerting/pull/222))

