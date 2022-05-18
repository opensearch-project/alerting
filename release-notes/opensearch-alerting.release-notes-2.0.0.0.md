## Version 2.0.0.0 2022-05-18

Compatible with OpenSearch 2.0.0

### Enhancements
* Add automated migration for Destinations to Notification configs ([#379](https://github.com/opensearch-project/alerting/pull/379]))
* Integrate with Notifications plugin for Alerting backend ([#401](https://github.com/opensearch-project/alerting/pull/401]))
* Integrate Document Level Alerting changes ([#410](https://github.com/opensearch-project/alerting/pull/410]))
* Alias support for Document Level Monitors ([#416](https://github.com/opensearch-project/alerting/pull/416]))

### Maintenance
* Upgrade kotlin to 1.16.10 ([#356](https://github.com/opensearch-project/alerting/pull/356]))
* Upgrade Alerting to 2.0 ([#357](https://github.com/opensearch-project/alerting/pull/357]))
* Remove rc1 qualifier from plugin version ([#442](https://github.com/opensearch-project/alerting/pull/442])) 

### Bug Fixes
* Completely fix docker pull and install plugin ([#376](https://github.com/opensearch-project/alerting/pull/376))
* Make sure alerting is using the build script in its own repo ([#377](https://github.com/opensearch-project/alerting/pull/377))
* fix security test workflow ([#407](https://github.com/opensearch-project/alerting/pull/407))
* Fixed a flaky test condition. ([#375](https://github.com/opensearch-project/alerting/pull/375]))
* Remove actionGet and fix minor bugs ([#424](https://github.com/opensearch-project/alerting/pull/424]))
* Fix UnsupportedOperation error while alert categorization in BucketLevel monitor ([#428](https://github.com/opensearch-project/alerting/pull/428]))
* Fix minor bugs and support per alert action execution for Document Level Monitors ([#441](https://github.com/opensearch-project/alerting/pull/441]))
* Fix minor bugs and pass in user context when retrieving user's notification channels ([#447](https://github.com/opensearch-project/alerting/pull/447]))
* Fix elevated security permission with Notification and minor bug fixes ([#449](https://github.com/opensearch-project/alerting/pull/449]))
* Improve error messaging on exceptions from notification channel retrieval and fix bug ([#451](https://github.com/opensearch-project/alerting/pull/451]))

### Refactoring
* Remove write Destination APIs ([#412](https://github.com/opensearch-project/alerting/pull/412]))
* Remove Alerting's notification subproject ([#413](https://github.com/opensearch-project/alerting/pull/413]))
* Skipping destination migration if alerting index is not initialized ([#417](https://github.com/opensearch-project/alerting/pull/417]))
* Fix Finding action naming and update release notes ([#432](https://github.com/opensearch-project/alerting/pull/432]))

### Infrastructure
* Removed the Beta label from the bug report template. ([#353](https://github.com/opensearch-project/alerting/pull/353))
* Update alerting with qualifier support in releases ([#366](https://github.com/opensearch-project/alerting/pull/366))
* Use OpenSearch 2.0.0-alpha1 ([#370](https://github.com/opensearch-project/alerting/pull/370))
* Add build qualifier default to alpha1 for 2.0.0 ([#373](https://github.com/opensearch-project/alerting/pull/373))
* Remove JDK 14 and Add JDK 17 ([#383](https://github.com/opensearch-project/alerting/pull/383))
* Updated issue templates from .github. ([#382](https://github.com/opensearch-project/alerting/pull/382))
* Incremented version to 2.0-rc1. ([#404](https://github.com/opensearch-project/alerting/pull/404))
* Replace checked-in ZIP for bwc tests with a dynamic dependency ([#411](https://github.com/opensearch-project/alerting/pull/411))
* Update integTest gradle scripts to run via remote cluster independently ([#418](https://github.com/opensearch-project/alerting/pull/418))

### Documentation
* Add Document Level Alerting RFC ([#388](https://github.com/opensearch-project/alerting/pull/388]))
* Deprecate the Master nomenclature in 2.0 ([#415](https://github.com/opensearch-project/alerting/pull/415]))
* Add release notes for version 2.0.0-rc1 ([#426](https://github.com/opensearch-project/alerting/pull/426))