package org.opensearch.alerting.transport

import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.launch
import org.apache.logging.log4j.LogManager
import org.opensearch.action.support.ActionFilters
import org.opensearch.action.support.HandledTransportAction
import org.opensearch.alerting.service.DeleteMonitorService
import org.opensearch.alerting.settings.AlertingSettings
import org.opensearch.cluster.service.ClusterService
import org.opensearch.common.inject.Inject
import org.opensearch.common.settings.Settings
import org.opensearch.commons.alerting.action.AlertingActions
import org.opensearch.commons.alerting.action.DeleteMonitorV2Request
import org.opensearch.commons.alerting.action.DeleteMonitorV2Response
import org.opensearch.core.action.ActionListener
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
    AlertingActions.DELETE_MONITOR_V2_ACTION_NAME, transportService, actionFilters, ::DeleteMonitorV2Request
),
    SecureTransportAction {

    @Volatile override var filterByEnabled = AlertingSettings.FILTER_BY_BACKEND_ROLES.get(settings)

    init {
        listenFilterBySettingChange(clusterService)
    }

    override fun doExecute(task: Task, request: DeleteMonitorV2Request, actionListener: ActionListener<DeleteMonitorV2Response>) {
//        val user = readUserFromThreadContext(client)
//
//        if (!validateUserBackendRoles(user, actionListener)) {
//            return
//        }
        // TOOD: when monitor is deleted, immediately expire all alerts it generated
        scope.launch {
            try {
                val deleteResponse = DeleteMonitorService.deleteMonitorV2(request.monitorV2Id, request.refreshPolicy)
                actionListener.onResponse(deleteResponse)
            } catch (e: Exception) {
                actionListener.onFailure(e)
            }

            // we do not expire the alerts associated with the deleted monitor, but instead let its expiration time delete it
        }
    }
}
