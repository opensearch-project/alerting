## Version 3.7.0 Release Notes

Compatible with OpenSearch and OpenSearch Dashboards version 3.7.0

### Features

* Migrate all alerting persistence operations to SdkClient for remote metadata support ([#2093](https://github.com/opensearch-project/alerting/pull/2093))
* Add EventBridge Scheduler CRUD for external monitor scheduling ([#2096](https://github.com/opensearch-project/alerting/pull/2096))
* Add bucket-level trigger evaluation using standard bucket_selector for multi-tenant environments ([#2098](https://github.com/opensearch-project/alerting/pull/2098))
* Block unsupported actions when multi-tenancy is enabled ([#2099](https://github.com/opensearch-project/alerting/pull/2099))
* Disable email, findings, and chained alert actions when multi-tenancy is enabled ([#2101](https://github.com/opensearch-project/alerting/pull/2101))
* Add MonitorJobPoller, ExternalSchedulerService, and SQS-based external monitor scheduling ([#2103](https://github.com/opensearch-project/alerting/pull/2103))
* Replace SchedulePayloadBuilder with ScheduleJobPayload for EB schedule target input ([#2104](https://github.com/opensearch-project/alerting/pull/2104))
* Add implementation for JobQueueAccountIdProvider ([#2105](https://github.com/opensearch-project/alerting/pull/2105))
* Use toXContentWithUser for SQS payload monitor serialization to include metadata and user context ([#2106](https://github.com/opensearch-project/alerting/pull/2106))
* Make MonitorJobPoller populate thread context for downstream request interception ([#2107](https://github.com/opensearch-project/alerting/pull/2107))
* Disable job scheduler indices when multi-tenancy is enabled ([#2108](https://github.com/opensearch-project/alerting/pull/2108))
* Add execution_role_arn setting for two-role EventBridge Scheduler model ([#2109](https://github.com/opensearch-project/alerting/pull/2109))
* Create SQS client in MonitorJobPoller.doStart when external scheduling is enabled ([#2110](https://github.com/opensearch-project/alerting/pull/2110))
* Add PPL alerting dependencies and cross-plugin communication utilities ([#2114](https://github.com/opensearch-project/alerting/pull/2114))
* Add AWS SDK dependencies with dynamic version ([#2116](https://github.com/opensearch-project/alerting/pull/2116))
* Preserve tenancy context across stashes and coroutines ([#2118](https://github.com/opensearch-project/alerting/pull/2118))
* Use role name and construct ARN at runtime for scheduler identities ([#2120](https://github.com/opensearch-project/alerting/pull/2120))
* Propagate tenant ID header in notification plugin client calls ([#2121](https://github.com/opensearch-project/alerting/pull/2121))
* Move delete monitor flow to remote metadata SDK ([#2125](https://github.com/opensearch-project/alerting/pull/2125))
* Add tenant ID to alerts service requests ([#2126](https://github.com/opensearch-project/alerting/pull/2126))
* Add PPL alerting monitor CRUD operations ([#2128](https://github.com/opensearch-project/alerting/pull/2128))
* Add PPL monitor execution support with RBAC checks for manual executions ([#2130](https://github.com/opensearch-project/alerting/pull/2130))
* Add tenant_id to monitor metadata and propagate to AlertService SDK calls ([#2131](https://github.com/opensearch-project/alerting/pull/2131))
* Block non-PPL monitor CRUD on pluggable dataformat domains ([#2141](https://github.com/opensearch-project/alerting/pull/2141))
* Propagate tenant headers for remote monitor execution via ARN parsing ([#2143](https://github.com/opensearch-project/alerting/pull/2143))

### Bug Fixes

* Set number_of_shards to 1 for plugin-created system indices ([#2091](https://github.com/opensearch-project/alerting/pull/2091))
* Keep alerting builds stable after PPL alerting common-utils changes ([#2111](https://github.com/opensearch-project/alerting/pull/2111))
* Gate system index lifecycle operations when multi-tenancy is enabled ([#2124](https://github.com/opensearch-project/alerting/pull/2124))
* Stash and re-inject thread context before executing monitor in poller ([#2132](https://github.com/opensearch-project/alerting/pull/2132))
* Handle typed_keys prefix and remote target in bucket-level monitors ([#2146](https://github.com/opensearch-project/alerting/pull/2146))
* Add reinjectHeaders for PPL monitor execution path to fix header propagation in multi-tenant mode ([#2151](https://github.com/opensearch-project/alerting/pull/2151))
* Fix PPL monitor execution to skip base query when only custom triggers are present ([#2155](https://github.com/opensearch-project/alerting/pull/2155))

### Infrastructure

* Add issues write permission to untriaged label workflow ([#2148](https://github.com/opensearch-project/alerting/pull/2148))

### Documentation

* Update bug report template with instructions to not include sensitive information ([#2119](https://github.com/opensearch-project/alerting/pull/2119))

### Maintenance

* Baselined maintainers list ([#2092](https://github.com/opensearch-project/alerting/pull/2092))
* Bumped gradle version to 9.4.1 ([#2122](https://github.com/opensearch-project/alerting/pull/2122))
