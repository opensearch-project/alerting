package org.opensearch.alerting

val ALERTING_ACK_ALERTS = "alerting_ack_alerts"
val ALL_ACCESS_ROLE = "all_access"
val ALERTING_FULL_ACCESS_ROLE = "alerting_full_access"
val ALERTING_READ_ONLY_ACCESS = "alerting_read_access"
val ALERTING_SEARCH_MONITOR_ONLY_ACCESS = "alerting_search_monitor_access"
val ALERTING_SEARCH_EMAIL_ACCOUNT_ACCESS = "alerting_search_email_account_access"
val ALERTING_INDEX_MONITOR_ACCESS = "alerting_index_monitor_access"
val ALERTING_INDEX_EMAIL_GROUP_ACCESS = "alerting_index_email_group_access"
val ALERTING_INDEX_EMAIL_ACCOUNT_ACCESS = "alerting_index_email_account_access"
val ALERTING_INDEX_DESTINATION_ACCESS = "alerting_index_destination_access"
val ALERTING_GET_MONITOR_ACCESS = "alerting_get_monitor_access"
val ALERTING_GET_EMAIL_GROUP_ACCESS = "alerting_get_email_group_access"
val ALERTING_GET_EMAIL_ACCOUNT_ACCESS = "alerting_get_email_account_access"
val ALERTING_GET_DESTINATION_ACCESS = "alerting_get_destination_access"
val ALERTING_GET_ALERTS_ACCESS = "alerting_get_alerts_access"
val ALERTING_EXECUTE_MONITOR_ACCESS = "alerting_execute_monitor_access"
val ALERTING_DELETE_MONITOR_ACCESS = "alerting_delete_monitor_access"
val ALERTING_DELETE_EMAIL_GROUP_ACCESS = "alerting_delete_email_group_access"
val ALERTING_DELETE_EMAIL_ACCOUNT_ACCESS = "alerting_delete_email_account_access"
val ALERTING_DELETE_DESTINATION_ACCESS = "alerting_delete_destination_access"
val ALERTING_ACKNOWLEDGE_ALERT_ACCESS = "alerting_acknowledge_alert_access"

val ROLE_TO_PERMISSION_MAPPING = mapOf(
    ALERTING_SEARCH_MONITOR_ONLY_ACCESS to "cluster:admin/opendistro/alerting/monitor/search",
    ALERTING_SEARCH_EMAIL_ACCOUNT_ACCESS to "cluster:admin/opendistro/alerting/destination/email_account/search",
    ALERTING_INDEX_MONITOR_ACCESS to "cluster:admin/opendistro/alerting/monitor/write",
    ALERTING_INDEX_EMAIL_GROUP_ACCESS to "cluster:admin/opendistro/alerting/destination/email_group/write",
    ALERTING_INDEX_EMAIL_ACCOUNT_ACCESS to "cluster:admin/opendistro/alerting/destination/email_account/write",
    ALERTING_INDEX_DESTINATION_ACCESS to "cluster:admin/opendistro/alerting/destination/write",
    ALERTING_GET_MONITOR_ACCESS to "cluster:admin/opendistro/alerting/monitor/get",
    ALERTING_GET_EMAIL_GROUP_ACCESS to "cluster:admin/opendistro/alerting/destination/email_group/get",
    ALERTING_GET_EMAIL_ACCOUNT_ACCESS to "cluster:admin/opendistro/alerting/destination/email_account/get",
    ALERTING_GET_DESTINATION_ACCESS to "cluster:admin/opendistro/alerting/destination/get",
    ALERTING_GET_ALERTS_ACCESS to "cluster:admin/opendistro/alerting/alerts/get",
    ALERTING_EXECUTE_MONITOR_ACCESS to "cluster:admin/opendistro/alerting/monitor/execute",
    ALERTING_DELETE_MONITOR_ACCESS to "cluster:admin/opendistro/alerting/monitor/delete",
    ALERTING_DELETE_EMAIL_GROUP_ACCESS to "cluster:admin/opendistro/alerting/destination/email_group/delete",
    ALERTING_DELETE_EMAIL_ACCOUNT_ACCESS to "cluster:admin/opendistro/alerting/destination/email_account/delete",
    ALERTING_DELETE_DESTINATION_ACCESS to "cluster:admin/opendistro/alerting/destination/delete",
    ALERTING_ACKNOWLEDGE_ALERT_ACCESS to "cluster:admin/opendistro/alerting/alerts/ack"
)
