## Version 3.2.0 Release Notes

Compatible with OpenSearch and OpenSearch Dashboards version 3.2.0

### Bug Fixes
* Fix MGet bug, randomize fan out distribution ([#1885](https://github.com/opensearch-project/alerting/pull/1885))
* Refactored consistent responses and fixed unrelated exceptions ([#1818](https://github.com/opensearch-project/alerting/pull/1818))

### Infrastructure
* Update the maven snapshot publish endpoint and credential ([#1869](https://github.com/opensearch-project/alerting/pull/1869))

### Maintenance
* Bumped gradle to 8.14, support JDK 24; fixed backport branch deletion ([#1911](https://github.com/opensearch-project/alerting/pull/1911))
* Increment version to 3.2.0-SNAPSHOT ([#1872](https://github.com/opensearch-project/alerting/pull/1872))
* Revert "now publishes a list of findings instead of an individual one ([#1881](https://github.com/opensearch-project/alerting/pull/1881))
* Moved the commons-beanutils pinning to the core gradle file ([#1892](https://github.com/opensearch-project/alerting/pull/1892))
* Pinned the commons-beanutils dependency to 1.11.0 version ([#1887](https://github.com/opensearch-project/alerting/pull/1887))