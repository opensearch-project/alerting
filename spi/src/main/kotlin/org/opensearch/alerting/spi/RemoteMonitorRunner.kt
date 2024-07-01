/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.alerting.spi

import org.opensearch.action.ActionListenerResponseHandler
import org.opensearch.action.support.GroupedActionListener
import org.opensearch.cluster.node.DiscoveryNode
import org.opensearch.cluster.service.ClusterService
import org.opensearch.commons.alerting.action.DocLevelMonitorFanOutAction
import org.opensearch.commons.alerting.action.DocLevelMonitorFanOutRequest
import org.opensearch.commons.alerting.action.DocLevelMonitorFanOutResponse
import org.opensearch.commons.alerting.model.IndexExecutionContext
import org.opensearch.commons.alerting.model.Monitor
import org.opensearch.commons.alerting.model.MonitorMetadata
import org.opensearch.commons.alerting.model.MonitorRunResult
import org.opensearch.commons.alerting.model.TriggerRunResult
import org.opensearch.commons.alerting.model.WorkflowRunContext
import org.opensearch.commons.alerting.util.AlertingException
import org.opensearch.core.action.ActionListener
import org.opensearch.core.common.breaker.CircuitBreakingException
import org.opensearch.core.common.io.stream.Writeable
import org.opensearch.core.index.shard.ShardId
import org.opensearch.node.NodeClosedException
import org.opensearch.transport.ActionNotFoundTransportException
import org.opensearch.transport.ConnectTransportException
import org.opensearch.transport.RemoteTransportException
import org.opensearch.transport.TransportException
import org.opensearch.transport.TransportRequestOptions
import org.opensearch.transport.TransportService
import java.time.Instant
import kotlin.coroutines.resume
import kotlin.coroutines.resumeWithException
import kotlin.coroutines.suspendCoroutine

open class RemoteMonitorRunner {

    open fun runMonitor(
        monitor: Monitor,
        periodStart: Instant,
        periodEnd: Instant,
        dryrun: Boolean,
        executionId: String,
        transportService: TransportService
    ): MonitorRunResult<TriggerRunResult> {
        return MonitorRunResult(monitor.name, periodStart, periodEnd)
    }

    open fun getFanOutAction(): String {
        return DocLevelMonitorFanOutAction.NAME
    }

    open suspend fun doFanOut(
        clusterService: ClusterService,
        monitor: Monitor,
        monitorMetadata: MonitorMetadata,
        executionId: String,
        concreteIndices: List<String>,
        workflowRunContext: WorkflowRunContext?,
        dryrun: Boolean,
        transportService: TransportService,
        nodeMap: Map<String, DiscoveryNode>,
        nodeShardAssignments: Map<String, MutableSet<ShardId>>
    ): MutableList<DocLevelMonitorFanOutResponse> {
        return suspendCoroutine { cont ->
            val listener = GroupedActionListener(
                object : ActionListener<Collection<DocLevelMonitorFanOutResponse>> {
                    override fun onResponse(response: Collection<DocLevelMonitorFanOutResponse>) {
                        cont.resume(response.toMutableList())
                    }

                    override fun onFailure(e: Exception) {
                        if (e.cause is Exception)
                            cont.resumeWithException(e.cause as Exception)
                        else
                            cont.resumeWithException(e)
                    }
                },
                nodeShardAssignments.size
            )
            val responseReader = Writeable.Reader {
                DocLevelMonitorFanOutResponse(it)
            }
            for (node in nodeMap) {
                if (nodeShardAssignments.containsKey(node.key)) {
                    val docLevelMonitorFanOutRequest = DocLevelMonitorFanOutRequest(
                        monitor,
                        dryrun,
                        monitorMetadata,
                        executionId,
                        indexExecutionContext = IndexExecutionContext(
                            listOf(),
                            mutableMapOf(),
                            mutableMapOf(),
                            "",
                            "",
                            listOf(),
                            listOf(),
                            listOf()
                        ),
                        nodeShardAssignments[node.key]!!.toList(),
                        concreteIndices,
                        workflowRunContext
                    )

                    transportService.sendRequest(
                        node.value,
                        getFanOutAction(),
                        docLevelMonitorFanOutRequest,
                        TransportRequestOptions.EMPTY,
                        object : ActionListenerResponseHandler<DocLevelMonitorFanOutResponse>(
                            listener,
                            responseReader
                        ) {
                            override fun handleException(e: TransportException) {
                                val cause = e.unwrapCause()
                                if (cause is ConnectTransportException ||
                                    (
                                            e is RemoteTransportException &&
                                                    (
                                                            cause is NodeClosedException ||
                                                                    cause is CircuitBreakingException ||
                                                                    cause is ActionNotFoundTransportException
                                                            )
                                            )
                                ) {
                                    val localNode = clusterService.localNode()
                                    // retry in local node
                                    transportService.sendRequest(
                                        localNode,
                                        DocLevelMonitorFanOutAction.NAME,
                                        docLevelMonitorFanOutRequest,
                                        TransportRequestOptions.EMPTY,
                                        object :
                                            ActionListenerResponseHandler<DocLevelMonitorFanOutResponse>(
                                                listener,
                                                responseReader
                                            ) {
                                            override fun handleException(e: TransportException) {
                                                listener.onResponse(
                                                    DocLevelMonitorFanOutResponse(
                                                        "",
                                                        "",
                                                        "",
                                                        mutableMapOf(),
                                                        exception = if (e.cause is AlertingException) {
                                                            e.cause as AlertingException
                                                        } else {
                                                            AlertingException.wrap(e) as AlertingException
                                                        }
                                                    )
                                                )
                                            }

                                            override fun handleResponse(response: DocLevelMonitorFanOutResponse) {
                                                listener.onResponse(response)
                                            }
                                        }
                                    )
                                } else {
                                    listener.onResponse(
                                        DocLevelMonitorFanOutResponse(
                                            "",
                                            "",
                                            "",
                                            mutableMapOf(),
                                            exception = if (e.cause is AlertingException) {
                                                e.cause as AlertingException
                                            } else {
                                                AlertingException.wrap(e) as AlertingException
                                            }
                                        )
                                    )
                                }
                            }

                            override fun handleResponse(response: DocLevelMonitorFanOutResponse) {
                                listener.onResponse(response)
                            }
                        }
                    )
                }
            }
        }
    }
}