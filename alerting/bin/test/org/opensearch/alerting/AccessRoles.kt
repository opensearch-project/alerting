/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.alerting

import org.opensearch.alerting.action.ExecuteWorkflowAction
import org.opensearch.commons.alerting.action.AlertingActions

val ALL_ACCESS_ROLE = "all_access"
val READALL_AND_MONITOR_ROLE = "readall_and_monitor"
val ALERTING_FULL_ACCESS_ROLE = "alerting_full_access"
val ALERTING_READ_ONLY_ACCESS = "alerting_read_access"
val ALERTING_NO_ACCESS_ROLE = "no_access"
val ALERTING_GET_EMAIL_ACCOUNT_ACCESS = "alerting_get_email_account_access"
val ALERTING_SEARCH_EMAIL_ACCOUNT_ACCESS = "alerting_search_email_account_access"
val ALERTING_GET_EMAIL_GROUP_ACCESS = "alerting_get_email_group_access"
val ALERTING_SEARCH_EMAIL_GROUP_ACCESS = "alerting_search_email_group_access"
val ALERTING_INDEX_MONITOR_ACCESS = "alerting_index_monitor_access"
val ALERTING_GET_MONITOR_ACCESS = "alerting_get_monitor_access"
val ALERTING_GET_WORKFLOW_ACCESS = "alerting_get_workflow_access"
val ALERTING_DELETE_WORKFLOW_ACCESS = "alerting_delete_workflow_access"
val ALERTING_SEARCH_MONITOR_ONLY_ACCESS = "alerting_search_monitor_access"
val ALERTING_EXECUTE_MONITOR_ACCESS = "alerting_execute_monitor_access"
val ALERTING_EXECUTE_WORKFLOW_ACCESS = "alerting_execute_workflow_access"
val ALERTING_DELETE_MONITOR_ACCESS = "alerting_delete_monitor_access"
val ALERTING_GET_DESTINATION_ACCESS = "alerting_get_destination_access"
val ALERTING_GET_ALERTS_ACCESS = "alerting_get_alerts_access"
val ALERTING_INDEX_WORKFLOW_ACCESS = "alerting_index_workflow_access"

val ROLE_TO_PERMISSION_MAPPING = mapOf(
    ALERTING_NO_ACCESS_ROLE to "",
    ALERTING_GET_EMAIL_ACCOUNT_ACCESS to "cluster:admin/opendistro/alerting/destination/email_account/get",
    ALERTING_SEARCH_EMAIL_ACCOUNT_ACCESS to "cluster:admin/opendistro/alerting/destination/email_account/search",
    ALERTING_GET_EMAIL_GROUP_ACCESS to "cluster:admin/opendistro/alerting/destination/email_group/get",
    ALERTING_SEARCH_EMAIL_GROUP_ACCESS to "cluster:admin/opendistro/alerting/destination/email_group/search",
    ALERTING_INDEX_MONITOR_ACCESS to "cluster:admin/opendistro/alerting/monitor/write",
    ALERTING_GET_MONITOR_ACCESS to "cluster:admin/opendistro/alerting/monitor/get",
    ALERTING_GET_WORKFLOW_ACCESS to AlertingActions.GET_WORKFLOW_ACTION_NAME,
    ALERTING_SEARCH_MONITOR_ONLY_ACCESS to "cluster:admin/opendistro/alerting/monitor/search",
    ALERTING_EXECUTE_MONITOR_ACCESS to "cluster:admin/opendistro/alerting/monitor/execute",
    ALERTING_EXECUTE_WORKFLOW_ACCESS to ExecuteWorkflowAction.NAME,
    ALERTING_DELETE_MONITOR_ACCESS to "cluster:admin/opendistro/alerting/monitor/delete",
    ALERTING_GET_DESTINATION_ACCESS to "cluster:admin/opendistro/alerting/destination/get",
    ALERTING_GET_ALERTS_ACCESS to "cluster:admin/opendistro/alerting/alerts/get",
    ALERTING_INDEX_WORKFLOW_ACCESS to AlertingActions.INDEX_WORKFLOW_ACTION_NAME,
    ALERTING_DELETE_WORKFLOW_ACCESS to AlertingActions.DELETE_WORKFLOW_ACTION_NAME
)
