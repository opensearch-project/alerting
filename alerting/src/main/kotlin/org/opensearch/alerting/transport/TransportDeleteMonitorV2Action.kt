package org.opensearch.alerting.transport

import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.launch
import org.apache.logging.log4j.LogManager
import org.opensearch.OpenSearchStatusException
import org.opensearch.action.get.GetRequest
import org.opensearch.action.get.GetResponse
import org.opensearch.action.support.ActionFilters
import org.opensearch.action.support.HandledTransportAction
import org.opensearch.alerting.actionv2.DeleteMonitorV2Action
import org.opensearch.alerting.actionv2.DeleteMonitorV2Request
import org.opensearch.alerting.actionv2.DeleteMonitorV2Response
import org.opensearch.alerting.core.modelv2.MonitorV2
import org.opensearch.alerting.opensearchapi.suspendUntil
import org.opensearch.alerting.service.DeleteMonitorService
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

private val scope: CoroutineScope = CoroutineScope(Dispatchers.IO)
private val log = LogManager.getLogger(TransportDeleteMonitorAction::class.java)

class TransportDeleteMonitorV2Action @Inject constructor(
    transportService: TransportService,
    val client: Client,
    actionFilters: ActionFilters,
    val clusterService: ClusterService,
    settings: Settings,
    val xContentRegistry: NamedXContentRegistry
) : HandledTransportAction<DeleteMonitorV2Request, DeleteMonitorV2Response>(
    DeleteMonitorV2Action.NAME, transportService, actionFilters, ::DeleteMonitorV2Request
),
    SecureTransportAction {

    @Volatile override var filterByEnabled = AlertingSettings.FILTER_BY_BACKEND_ROLES.get(settings)

    init {
        listenFilterBySettingChange(clusterService)
    }

    override fun doExecute(task: Task, request: DeleteMonitorV2Request, actionListener: ActionListener<DeleteMonitorV2Response>) {
        val user = readUserFromThreadContext(client)

        if (!validateUserBackendRoles(user, actionListener)) {
            return
        }

        scope.launch {
            try {
                val monitorV2 = getMonitorV2(request.monitorV2Id, actionListener) ?: return@launch

                val canDelete = user == null || !doFilterForUser(user) ||
                    checkUserPermissionsWithResource(user, monitorV2!!.user, actionListener, "monitor_v2", request.monitorV2Id)

                if (canDelete) {
                    val deleteResponse = DeleteMonitorService.deleteMonitorV2(request.monitorV2Id, request.refreshPolicy)
                    actionListener.onResponse(deleteResponse)
                } else {
                    actionListener.onFailure(
                        AlertingException("Not allowed to delete this monitor_v2", RestStatus.FORBIDDEN, IllegalStateException())
                    )
                }
            } catch (e: Exception) {
                actionListener.onFailure(e)
            }

            // we do not expire the alerts associated with the deleted monitor, but instead let its expiration time delete it
        }
    }

    private suspend fun getMonitorV2(monitorV2Id: String, actionListener: ActionListener<DeleteMonitorV2Response>): MonitorV2? {
        val getRequest = GetRequest(ScheduledJob.SCHEDULED_JOBS_INDEX, monitorV2Id)

        val getResponse: GetResponse = client.suspendUntil { get(getRequest, it) }
        if (!getResponse.isExists) {
            actionListener.onFailure(
                AlertingException.wrap(
                    OpenSearchStatusException("MonitorV2 with $monitorV2Id is not found", RestStatus.NOT_FOUND)
                )
            )
            return null
        }
        val xcp = XContentHelper.createParser(
            xContentRegistry, LoggingDeprecationHandler.INSTANCE,
            getResponse.sourceAsBytesRef, XContentType.JSON
        )

        val monitorV2: MonitorV2?
        try {
            monitorV2 = ScheduledJob.parse(xcp, getResponse.id, getResponse.version) as MonitorV2
        } catch (e: ClassCastException) {
            // if ScheduledJob parsed the object and could not cast it to MonitorV2, we must
            // have gotten a Monitor V1 from the given ID
            actionListener.onFailure(
                AlertingException.wrap(
                    IllegalArgumentException(
                        "The ID given corresponds to a V1 Monitor, please pass in the ID of a V2 Monitor"
                    )
                )
            )
            return null
        }

        return monitorV2
    }
}
