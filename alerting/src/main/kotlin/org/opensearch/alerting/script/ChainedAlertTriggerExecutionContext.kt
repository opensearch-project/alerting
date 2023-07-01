package org.opensearch.alerting.script

import org.opensearch.alerting.model.WorkflowRunResult
import org.opensearch.commons.alerting.model.ChainedAlertTrigger
import org.opensearch.commons.alerting.model.Workflow
import java.time.Instant

data class ChainedAlertTriggerExecutionContext(
    val workflow: Workflow,
    val workflowRunResult: WorkflowRunResult,
    val periodStart: Instant,
    val periodEnd: Instant?,
    val error: Exception? = null,
    val trigger: ChainedAlertTrigger,
    val alertGeneratingMonitors: Set<String>,
    val monitorIdToAlertIdsMap: Map<String, Set<String>>
) {

    constructor(
        workflow: Workflow,
        workflowRunResult: WorkflowRunResult,
        trigger: ChainedAlertTrigger,
        alertGeneratingMonitors: Set<String>,
        monitorIdToAlertIdsMap: Map<String, Set<String>>
    ) :
        this(
            workflow,
            workflowRunResult,
            workflowRunResult.executionStartTime,
            workflowRunResult.executionEndTime,
            workflowRunResult.error,
            trigger,
            alertGeneratingMonitors,
            monitorIdToAlertIdsMap
        )

    /**
     * Mustache templates need special permissions to reflectively introspect field names. To avoid doing this we
     * translate the context to a Map of Strings to primitive types, which can be accessed without reflection.
     */
    open fun asTemplateArg(): Map<String, Any?> {
        return mapOf(
            "monitor" to workflow.asTemplateArg(),
            "results" to workflowRunResult,
            "periodStart" to periodStart,
            "error" to error,
            "alertGeneratingMonitors" to alertGeneratingMonitors,
            "monitorIdToAlertIdsMap" to monitorIdToAlertIdsMap
        )
    }
}
