## Version 1.0.0.0 2021-06-30

Compatible with OpenSearch 1.0.0

### Infrastructure
* Upgrading the Ktlint Version and applying the formatting to the project ([#20](https://github.com/opensearch-project/alerting/pull/20))
* Upgrade google-java-format to 1.10.0 to pick guava v30 ([#115](https://github.com/opensearch-project/alerting/pull/115))
### Enhancements
* Adding check for security enabled ([#24](https://github.com/opensearch-project/alerting/pull/24))
* Adding Workflow to test the Security Integration tests with the Security Plugin Installed on Opensearch Docker Image ([#25](https://github.com/opensearch-project/alerting/pull/25))
* Adding Ktlint formatting check to the Gradle build task ([#26](https://github.com/opensearch-project/alerting/pull/26))
* Updating UTs for the Destination Settings ([#102](https://github.com/opensearch-project/alerting/pull/102))
* Adding additional null checks for URL not present ([#112](https://github.com/opensearch-project/alerting/pull/112))
* Enable license headers check ([118](https://github.com/opensearch-project/alerting/pull/118))
### Documentation
* Update issue template with multiple labels ([#13](https://github.com/opensearch-project/alerting/pull/13))
* Update and add documentation files ([#117](https://github.com/opensearch-project/alerting/pull/117))
### Maintenance
* Adding Rest APIs Backward Compatibility with ODFE ([#16](https://github.com/opensearch-project/alerting/pull/16))
* Moving the ODFE Settings to Legacy Settings and adding the new settings compatible with Opensearch ([#18](https://github.com/opensearch-project/alerting/pull/18))
### Refactoring
* Rename namespaces from com.amazon.opendistroforelasticsearch to org.opensearch ([#15](https://github.com/opensearch-project/alerting/pull/15))