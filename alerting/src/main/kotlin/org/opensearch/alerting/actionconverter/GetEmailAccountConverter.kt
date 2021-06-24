package org.opensearch.alerting.actionconverter

import org.opensearch.alerting.action.GetEmailAccountRequest
import org.opensearch.commons.notifications.action.GetNotificationConfigRequest
import org.opensearch.search.sort.SortOrder

class GetEmailAccountConverter {

    companion object {
        fun convertEmailAccountRequestToNotificationRequest(request: GetEmailAccountRequest): GetNotificationConfigRequest {
            val configIds: Set<String> = if(request.destinationId != null) setOf(request.destinationId) else emptySet()
            val table = request.table
            val fromIndex = table.startIndex
            val maxItems = table.size
            val sortField = table.sortString
            val sortOrder: SortOrder = SortOrder.fromString(table.sortOrder)
            val filterParams: Map<String, String> = emptyMap()

            return GetNotificationConfigRequest(configIds, fromIndex, maxItems, sortField, sortOrder, filterParams)
        }
    }
}
