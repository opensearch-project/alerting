## Version 3.10.0 Release Notes

Compatible with OpenSearch and OpenSearch Dashboards version 3.10.0

### Enhancements

* Deprecate `filter_by_backend_roles` setting, targeting removal in 4.0 in favor of the security plugin's resource sharing and access control ([#2234](https://github.com/opensearch-project/alerting/pull/2234))
* Register findings indices (`.opensearch-alerting-finding*`) as protected system indices when the security plugin is enabled ([#2310](https://github.com/opensearch-project/alerting/pull/2310))
* Repair history write aliases automatically after a snapshot restore to prevent alert, finding, and comment history write failures ([#2312](https://github.com/opensearch-project/alerting/pull/2312))
* Use the current query index for doc-level monitors when the saved query index no longer exists after a restore ([#2309](https://github.com/opensearch-project/alerting/pull/2309))

### Bug Fixes

* Fix `ClassCastException` when updating a doc-level monitor whose metadata has an empty last run context ([#2271](https://github.com/opensearch-project/alerting/pull/2271))
* Fix PPL monitor failures when the SQL/PPL plugin returns a response without a `total` field ([#2270](https://github.com/opensearch-project/alerting/pull/2270))
* Record the creator on a workflow's sharing record so workflows are discoverable and shareable under resource sharing ([#2300](https://github.com/opensearch-project/alerting/pull/2300))
* Reset stale last run context for doc-level monitors after a snapshot restore so new documents are not silently skipped ([#2287](https://github.com/opensearch-project/alerting/pull/2287))

### Infrastructure

* Restore BWC upgrade tests and CI checks for rolling and full-restart upgrades ([#2305](https://github.com/opensearch-project/alerting/pull/2305))
* Restore the alerting unit test suite by fixing node startup when the SQS poller is enabled but unconfigured, and resolve all 77 pre-existing test failures on main ([#2314](https://github.com/opensearch-project/alerting/pull/2314))
* Fix seed-dependent failure in the bucket-level acknowledged-alert integration test ([#2315](https://github.com/opensearch-project/alerting/pull/2315))
* Correct the cause recorded for the acknowledged-alert test and remove a redundant action ([#2316](https://github.com/opensearch-project/alerting/pull/2316))
