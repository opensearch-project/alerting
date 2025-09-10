package org.opensearch.alerting.transport

import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import org.apache.logging.log4j.LogManager
import org.opensearch.OpenSearchStatusException
import org.opensearch.action.get.GetRequest
import org.opensearch.action.get.GetResponse
import org.opensearch.action.support.ActionFilters
import org.opensearch.action.support.HandledTransportAction
import org.opensearch.alerting.actionv2.GetMonitorV2Action
import org.opensearch.alerting.actionv2.GetMonitorV2Request
import org.opensearch.alerting.actionv2.GetMonitorV2Response
import org.opensearch.alerting.core.modelv2.MonitorV2
import org.opensearch.alerting.settings.AlertingSettings
import org.opensearch.cluster.service.ClusterService
import org.opensearch.common.inject.Inject
import org.opensearch.common.settings.Settings
import org.opensearch.common.xcontent.LoggingDeprecationHandler
import org.opensearch.common.xcontent.XContentHelper
import org.opensearch.common.xcontent.XContentType
import org.opensearch.commons.alerting.model.ScheduledJob
import org.opensearch.commons.alerting.util.AlertingException
import org.opensearch.core.action.ActionListener
import org.opensearch.core.rest.RestStatus
import org.opensearch.core.xcontent.NamedXContentRegistry
import org.opensearch.tasks.Task
import org.opensearch.transport.TransportService
import org.opensearch.transport.client.Client

private val log = LogManager.getLogger(TransportGetMonitorAction::class.java)
private val scope: CoroutineScope = CoroutineScope(Dispatchers.IO)

class TransportGetMonitorV2Action @Inject constructor(
    transportService: TransportService,
    val client: Client,
    actionFilters: ActionFilters,
    val xContentRegistry: NamedXContentRegistry,
    val clusterService: ClusterService,
    settings: Settings,
) : HandledTransportAction<GetMonitorV2Request, GetMonitorV2Response>(
    GetMonitorV2Action.NAME,
    transportService,
    actionFilters,
    ::GetMonitorV2Request
),
    SecureTransportAction {

    @Volatile
    override var filterByEnabled = AlertingSettings.FILTER_BY_BACKEND_ROLES.get(settings)

    init {
        listenFilterBySettingChange(clusterService)
    }

    override fun doExecute(task: Task, request: GetMonitorV2Request, actionListener: ActionListener<GetMonitorV2Response>) {
        val getRequest = GetRequest(ScheduledJob.SCHEDULED_JOBS_INDEX, request.monitorV2Id)
            .version(request.version)
            .fetchSourceContext(request.srcContext)

//        if (!validateUserBackendRoles(user, actionListener)) {
//            return
//        }

//        client.threadPool().threadContext.stashContext().use {
        client.get(
            getRequest,
            object : ActionListener<GetResponse> {
                override fun onResponse(response: GetResponse) {
                    if (!response.isExists) {
                        actionListener.onFailure(
                            AlertingException.wrap(OpenSearchStatusException("MonitorV2 not found.", RestStatus.NOT_FOUND))
                        )
                        return
                    }

                    var monitorV2: MonitorV2? = null
                    if (!response.isSourceEmpty) {
                        XContentHelper.createParser(
                            xContentRegistry,
                            LoggingDeprecationHandler.INSTANCE,
                            response.sourceAsBytesRef,
                            XContentType.JSON
                        ).use { xcp ->
                            monitorV2 = ScheduledJob.parse(xcp, response.id, response.version) as MonitorV2
//
//                            // security is enabled and filterby is enabled
//                            if (!checkUserPermissionsWithResource(
//                                    user,
//                                    monitor?.user,
//                                    actionListener,
//                                    "monitor",
//                                    transformedRequest.monitorId
//                                )
//                            ) {
//                                return
//                            }
                        }
                    }

                    actionListener.onResponse(
                        GetMonitorV2Response(
                            response.id,
                            response.version,
                            response.seqNo,
                            response.primaryTerm,
                            monitorV2
                        )
                    )
                }

                override fun onFailure(t: Exception) {
                    actionListener.onFailure(AlertingException.wrap(t))
                }
            }
        )
//        }
    }
}
