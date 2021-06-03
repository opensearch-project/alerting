package com.amazon.opendistroforelasticsearch.alerting.actionconverter

import org.opensearch.alerting.action.GetDestinationsRequest
import org.opensearch.commons.notifications.action.GetFeatureChannelListRequest
import org.opensearch.commons.notifications.action.GetNotificationConfigRequest

class GetDestinationsConverter {

    companion object {
        fun convertAlertRequestToNotificationRequest(request: GetDestinationsRequest): GetNotificationConfigRequest {
            val configId = request.destinationId
            val table = request.table
            val fromIndex = table.startIndex
            val maxItems = table.size
            val sortField = table.sortString
            val sortOrder = table.sortOrder


            return GetNotificationConfigRequest()
        }
    }
}
