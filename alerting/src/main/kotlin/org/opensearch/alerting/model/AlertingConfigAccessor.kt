/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.alerting.model

import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.withContext
import org.opensearch.action.get.GetRequest
import org.opensearch.action.get.GetResponse
import org.opensearch.alerting.core.model.ScheduledJob
import org.opensearch.alerting.model.destination.Destination
import org.opensearch.alerting.model.destination.email.EmailAccount
import org.opensearch.alerting.model.destination.email.EmailGroup
import org.opensearch.alerting.opensearchapi.suspendUntil
import org.opensearch.client.Client
import org.opensearch.common.bytes.BytesReference
import org.opensearch.common.xcontent.LoggingDeprecationHandler
import org.opensearch.common.xcontent.NamedXContentRegistry
import org.opensearch.common.xcontent.XContentHelper
import org.opensearch.common.xcontent.XContentParser
import org.opensearch.common.xcontent.XContentParserUtils
import org.opensearch.common.xcontent.XContentType
import org.opensearch.index.IndexNotFoundException

/**
 * This is an accessor class to retrieve documents/information from the Alerting config index.
 */
class AlertingConfigAccessor {
    companion object {

        suspend fun getMonitorInfo(client: Client, xContentRegistry: NamedXContentRegistry, monitorId: String): Monitor {
            val jobSource = getAlertingConfigDocumentSource(client, "Monitor", monitorId)
            return withContext(Dispatchers.IO) {
                val xcp = XContentHelper.createParser(
                    xContentRegistry, LoggingDeprecationHandler.INSTANCE,
                    jobSource, XContentType.JSON
                )
                val monitor = Monitor.parse(xcp)
                monitor
            }
        }

        suspend fun getMonitorMetadata(client: Client, xContentRegistry: NamedXContentRegistry, metadataId: String): MonitorMetadata? {
            return try {
                val jobSource = getAlertingConfigDocumentSource(client, "Monitor Metadata", metadataId)
                withContext(Dispatchers.IO) {
                    val xcp = XContentHelper.createParser(
                        xContentRegistry, LoggingDeprecationHandler.INSTANCE,
                        jobSource, XContentType.JSON
                    )
                    XContentParserUtils.ensureExpectedToken(XContentParser.Token.START_OBJECT, xcp.nextToken(), xcp)
                    MonitorMetadata.parse(xcp)
                }
            } catch (e: IllegalStateException) {
                if (e.message?.equals("Monitor Metadata document with id $metadataId not found or source is empty") == true) {
                    return null
                } else throw e
            } catch (e: IndexNotFoundException) {
                if (e.message?.equals("no such index [.opendistro-alerting-config]") == true) {
                    return null
                } else throw e
            }
        }

        suspend fun getDestinationInfo(client: Client, xContentRegistry: NamedXContentRegistry, destinationId: String): Destination {
            val jobSource = getAlertingConfigDocumentSource(client, "Destination", destinationId)
            return withContext(Dispatchers.IO) {
                val xcp = XContentHelper.createParser(
                    xContentRegistry, LoggingDeprecationHandler.INSTANCE,
                    jobSource, XContentType.JSON
                )
                val destination = Destination.parseWithType(xcp)
                destination
            }
        }

        suspend fun getEmailAccountInfo(client: Client, xContentRegistry: NamedXContentRegistry, emailAccountId: String): EmailAccount {
            val source = getAlertingConfigDocumentSource(client, "Email account", emailAccountId)
            return withContext(Dispatchers.IO) {
                val xcp = XContentHelper.createParser(xContentRegistry, LoggingDeprecationHandler.INSTANCE, source, XContentType.JSON)
                val emailAccount = EmailAccount.parseWithType(xcp)
                emailAccount
            }
        }

        suspend fun getEmailGroupInfo(client: Client, xContentRegistry: NamedXContentRegistry, emailGroupId: String): EmailGroup {
            val source = getAlertingConfigDocumentSource(client, "Email group", emailGroupId)
            return withContext(Dispatchers.IO) {
                val xcp = XContentHelper.createParser(xContentRegistry, LoggingDeprecationHandler.INSTANCE, source, XContentType.JSON)
                val emailGroup = EmailGroup.parseWithType(xcp)
                emailGroup
            }
        }

        private suspend fun getAlertingConfigDocumentSource(
            client: Client,
            type: String,
            docId: String
        ): BytesReference {
            val getRequest = GetRequest(ScheduledJob.SCHEDULED_JOBS_INDEX, docId).routing(docId)
            val getResponse: GetResponse = client.suspendUntil { client.get(getRequest, it) }
            if (!getResponse.isExists || getResponse.isSourceEmpty) {
                throw IllegalStateException("$type document with id $docId not found or source is empty")
            }

            val finalResponse: GetResponse = client.suspendUntil { client.get(getRequest, it) }

            return finalResponse.sourceAsBytesRef
        }
    }
}
