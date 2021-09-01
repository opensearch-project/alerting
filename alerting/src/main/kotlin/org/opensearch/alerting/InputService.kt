/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 * Modifications Copyright OpenSearch Contributors. See
 * GitHub history for details.
 */

package org.opensearch.alerting

import org.apache.logging.log4j.LogManager
import org.opensearch.action.search.SearchRequest
import org.opensearch.action.search.SearchResponse
import org.opensearch.alerting.core.model.SearchInput
import org.opensearch.alerting.elasticapi.convertToMap
import org.opensearch.alerting.elasticapi.suspendUntil
import org.opensearch.alerting.model.InputRunResults
import org.opensearch.alerting.model.Monitor
import org.opensearch.alerting.model.TriggerAfterKey
import org.opensearch.alerting.util.AggregationQueryRewriter
import org.opensearch.alerting.util.addUserBackendRolesFilter
import org.opensearch.client.Client
import org.opensearch.common.io.stream.BytesStreamOutput
import org.opensearch.common.io.stream.NamedWriteableAwareStreamInput
import org.opensearch.common.io.stream.NamedWriteableRegistry
import org.opensearch.common.xcontent.LoggingDeprecationHandler
import org.opensearch.common.xcontent.NamedXContentRegistry
import org.opensearch.common.xcontent.XContentType
import org.opensearch.script.Script
import org.opensearch.script.ScriptService
import org.opensearch.script.ScriptType
import org.opensearch.script.TemplateScript
import org.opensearch.search.builder.SearchSourceBuilder
import java.time.Instant

/** Service that handles the collection of input results for Monitor executions */
class InputService(
    val client: Client,
    val scriptService: ScriptService,
    val namedWriteableRegistry: NamedWriteableRegistry,
    val xContentRegistry: NamedXContentRegistry
) {

    private val logger = LogManager.getLogger(InputService::class.java)

    suspend fun collectInputResults(
        monitor: Monitor,
        periodStart: Instant,
        periodEnd: Instant,
        prevResult: InputRunResults? = null
    ): InputRunResults {
        return try {
            val results = mutableListOf<Map<String, Any>>()
            val aggTriggerAfterKey: MutableMap<String, TriggerAfterKey> = mutableMapOf()

            // TODO: If/when multiple input queries are supported for Bucket-Level Monitor execution, aggTriggerAfterKeys will
            //  need to be updated to account for it
            monitor.inputs.forEach { input ->
                when (input) {
                    is SearchInput -> {
                        // TODO: Figure out a way to use SearchTemplateRequest without bringing in the entire TransportClient
                        val searchParams = mapOf(
                            "period_start" to periodStart.toEpochMilli(),
                            "period_end" to periodEnd.toEpochMilli()
                        )
                        // Deep copying query before passing it to rewriteQuery since otherwise, the monitor.input is modified directly
                        // which causes a strange bug where the rewritten query persists on the Monitor across executions
                        val rewrittenQuery = AggregationQueryRewriter.rewriteQuery(deepCopyQuery(input.query), prevResult, monitor.triggers)
                        val searchSource = scriptService.compile(
                            Script(
                                ScriptType.INLINE, Script.DEFAULT_TEMPLATE_LANG,
                                rewrittenQuery.toString(), searchParams
                            ),
                            TemplateScript.CONTEXT
                        )
                            .newInstance(searchParams)
                            .execute()

                        val searchRequest = SearchRequest().indices(*input.indices.toTypedArray())
                        XContentType.JSON.xContent().createParser(xContentRegistry, LoggingDeprecationHandler.INSTANCE, searchSource).use {
                            searchRequest.source(SearchSourceBuilder.fromXContent(it))
                        }
                        val searchResponse: SearchResponse = client.suspendUntil { client.search(searchRequest, it) }
                        aggTriggerAfterKey += AggregationQueryRewriter.getAfterKeysFromSearchResponse(
                            searchResponse,
                            monitor.triggers,
                            prevResult?.aggTriggersAfterKey
                        )
                        results += searchResponse.convertToMap()
                    }
                    else -> {
                        throw IllegalArgumentException("Unsupported input type: ${input.name()}.")
                    }
                }
            }
            InputRunResults(results.toList(), aggTriggersAfterKey = aggTriggerAfterKey)
        } catch (e: Exception) {
            logger.info("Error collecting inputs for monitor: ${monitor.id}", e)
            InputRunResults(emptyList(), e)
        }
    }

    private fun deepCopyQuery(query: SearchSourceBuilder): SearchSourceBuilder {
        val out = BytesStreamOutput()
        query.writeTo(out)
        val sin = NamedWriteableAwareStreamInput(out.bytes().streamInput(), namedWriteableRegistry)
        return SearchSourceBuilder(sin)
    }

    /**
     * We moved anomaly result index to system index list. So common user could not directly query
     * this index any more. This method will stash current thread context to pass security check.
     * So monitor job can access anomaly result index. We will add monitor user roles filter in
     * search query to only return documents the monitor user can access.
     *
     * On alerting Kibana, monitor users can only see detectors that they have read access. So they
     * can't create monitor on other user's detector which they have no read access. Even they know
     * other user's detector id and use it to create monitor, this method will only return anomaly
     * results they can read.
     */
    suspend fun collectInputResultsForADMonitor(monitor: Monitor, periodStart: Instant, periodEnd: Instant): InputRunResults {
        return try {
            val results = mutableListOf<Map<String, Any>>()
            val input = monitor.inputs[0] as SearchInput

            val searchParams = mapOf("period_start" to periodStart.toEpochMilli(), "period_end" to periodEnd.toEpochMilli())
            val searchSource = scriptService.compile(
                Script(
                    ScriptType.INLINE, Script.DEFAULT_TEMPLATE_LANG,
                    input.query.toString(), searchParams
                ),
                TemplateScript.CONTEXT
            )
                .newInstance(searchParams)
                .execute()

            val searchRequest = SearchRequest().indices(*input.indices.toTypedArray())
            XContentType.JSON.xContent().createParser(xContentRegistry, LoggingDeprecationHandler.INSTANCE, searchSource).use {
                searchRequest.source(SearchSourceBuilder.fromXContent(it))
            }

            // Add user role filter for AD result
            client.threadPool().threadContext.stashContext().use {
                // Currently we have no way to verify if user has AD read permission or not. So we always add user
                // role filter here no matter AD backend role filter enabled or not. If we don't add user role filter
                // when AD backend filter disabled, user can run monitor on any detector and get anomaly data even
                // they have no AD read permission. So if domain disabled AD backend role filter, monitor runner
                // still can't get AD result with different user backend role, even the monitor user has permission
                // to read AD result. This is a short term solution to trade off between user experience and security.
                //
                // Possible long term solution:
                // 1.Use secure rest client to send request to AD search result API. If no permission exception,
                // that mean user has read access on AD result. Then don't need to add user role filter when query
                // AD result if AD backend role filter is disabled.
                // 2.Security provide some transport action to verify if user has permission to search AD result.
                // Monitor runner will send transport request to check permission first. If security plugin response
                // is yes, user has permission to query AD result. If AD role filter enabled, we will add user role
                // filter to protect data at user role level; otherwise, user can query any AD result.
                addUserBackendRolesFilter(monitor.user, searchRequest.source())
                val searchResponse: SearchResponse = client.suspendUntil { client.search(searchRequest, it) }
                results += searchResponse.convertToMap()
            }
            InputRunResults(results.toList())
        } catch (e: Exception) {
            logger.info("Error collecting anomaly result inputs for monitor: ${monitor.id}", e)
            InputRunResults(emptyList(), e)
        }
    }
}
