/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.alerting.util

import org.apache.logging.log4j.LogManager
import org.opensearch.alerting.model.BucketLevelTriggerRunResult
import org.opensearch.alerting.model.destination.Destination
import org.opensearch.alerting.settings.DestinationSettings
import org.opensearch.common.util.concurrent.ThreadContext
import org.opensearch.commons.alerting.model.AggregationResultBucket
import org.opensearch.commons.alerting.model.Monitor
import org.opensearch.commons.alerting.model.action.Action
import org.opensearch.commons.alerting.model.action.ActionExecutionPolicy
import org.opensearch.commons.alerting.model.action.ActionExecutionScope
import org.opensearch.commons.alerting.util.isBucketLevelMonitor

private val logger = LogManager.getLogger("AlertingUtils")

/**
 * RFC 5322 compliant pattern matching: https://www.ietf.org/rfc/rfc5322.txt
 * Regex was based off of this post: https://stackoverflow.com/a/201378
 */
fun isValidEmail(email: String): Boolean {
    val validEmailPattern = Regex(
        "(?:[a-z0-9!#\$%&'*+\\/=?^_`{|}~-]+(?:\\.[a-z0-9!#\$%&'*+\\/=?^_`{|}~-]+)*" +
            "|\"(?:[\\x01-\\x08\\x0b\\x0c\\x0e-\\x1f\\x21\\x23-\\x5b\\x5d-\\x7f]|\\\\[\\x01-\\x09\\x0b\\x0c\\x0e-\\x7f])*\")" +
            "@(?:(?:[a-z0-9](?:[a-z0-9-]*[a-z0-9])?\\.)+[a-z0-9](?:[a-z0-9-]*[a-z0-9])?" +
            "|\\[(?:(?:(2(5[0-5]|[0-4][0-9])|1[0-9][0-9]|[1-9]?[0-9]))\\.){3}" +
            "(?:(2(5[0-5]|[0-4][0-9])|1[0-9][0-9]|[1-9]?[0-9])|[a-z0-9-]*[a-z0-9]:" +
            "(?:[\\x01-\\x08\\x0b\\x0c\\x0e-\\x1f\\x21-\\x5a\\x53-\\x7f]|\\\\[\\x01-\\x09\\x0b\\x0c\\x0e-\\x7f])+)\\])",
        RegexOption.IGNORE_CASE
    )

    return validEmailPattern.matches(email)
}

/** Allowed Destinations are ones that are specified in the [DestinationSettings.ALLOW_LIST] setting. */
fun Destination.isAllowed(allowList: List<String>): Boolean = allowList.contains(this.type.value)

fun Destination.isTestAction(): Boolean = this.type == DestinationType.TEST_ACTION

fun Monitor.isDocLevelMonitor(): Boolean = this.monitorType == Monitor.MonitorType.DOC_LEVEL_MONITOR

/**
 * Since buckets can have multi-value keys, this converts the bucket key values to a string that can be used
 * as the key for a HashMap to easily retrieve [AggregationResultBucket] based on the bucket key values.
 */
fun AggregationResultBucket.getBucketKeysHash(): String = this.bucketKeys.joinToString(separator = "#")

fun Action.getActionExecutionPolicy(monitor: Monitor): ActionExecutionPolicy? {
    // When the ActionExecutionPolicy is null for an Action, the default is resolved at runtime
    // so it can be chosen based on the Monitor type at that time.
    // The Action config is not aware of the Monitor type which is why the default was not stored during
    // the parse.
    return this.actionExecutionPolicy ?: if (monitor.isBucketLevelMonitor()) {
        ActionExecutionPolicy.getDefaultConfigurationForBucketLevelMonitor()
    } else if (monitor.isDocLevelMonitor()) {
        ActionExecutionPolicy.getDefaultConfigurationForDocumentLevelMonitor()
    } else {
        null
    }
}

fun BucketLevelTriggerRunResult.getCombinedTriggerRunResult(
    prevTriggerRunResult: BucketLevelTriggerRunResult?
): BucketLevelTriggerRunResult {
    if (prevTriggerRunResult == null) return this

    // The aggregation results and action results across to two trigger run results should not have overlapping keys
    // since they represent different pages of aggregations so a simple concatenation will combine them
    val mergedAggregationResultBuckets = prevTriggerRunResult.aggregationResultBuckets + this.aggregationResultBuckets
    val mergedActionResultsMap = (prevTriggerRunResult.actionResultsMap + this.actionResultsMap).toMutableMap()

    // Update to the most recent error if it's not null, otherwise keep the old one
    val error = this.error ?: prevTriggerRunResult.error

    return this.copy(aggregationResultBuckets = mergedAggregationResultBuckets, actionResultsMap = mergedActionResultsMap, error = error)
}

fun defaultToPerExecutionAction(
    maxActionableAlertCount: Long,
    monitorId: String,
    triggerId: String,
    totalActionableAlertCount: Int,
    monitorOrTriggerError: Exception?
): Boolean {
    // If the monitorId or triggerResult has an error, then also default to PER_EXECUTION to communicate the error
    if (monitorOrTriggerError != null) {
        logger.debug(
            "Trigger [$triggerId] in monitor [$monitorId] encountered an error. Defaulting to " +
                "[${ActionExecutionScope.Type.PER_EXECUTION}] for action execution to communicate error."
        )
        return true
    }

    // If the MAX_ACTIONABLE_ALERT_COUNT is set to -1, consider it unbounded and proceed regardless of actionable Alert count
    if (maxActionableAlertCount < 0) return false

    // If the total number of Alerts to execute Actions on exceeds the MAX_ACTIONABLE_ALERT_COUNT setting then default to
    // PER_EXECUTION for less intrusive Actions
    if (totalActionableAlertCount > maxActionableAlertCount) {
        logger.debug(
            "The total actionable alerts for trigger [$triggerId] in monitor [$monitorId] is [$totalActionableAlertCount] " +
                "which exceeds the maximum of [$maxActionableAlertCount]. " +
                "Defaulting to [${ActionExecutionScope.Type.PER_EXECUTION}] for action execution."
        )
        return true
    }

    return false
}

/**
 * Executes the given [block] function on this resource and then closes it down correctly whether an exception
 * is thrown or not.
 *
 * In case if the resource is being closed due to an exception occurred in [block], and the closing also fails with an exception,
 * the latter is added to the [suppressed][java.lang.Throwable.addSuppressed] exceptions of the former.
 *
 * @param block a function to process this [AutoCloseable] resource.
 * @return the result of [block] function invoked on this resource.
 */
inline fun <T : ThreadContext.StoredContext, R> T.use(block: (T) -> R): R {
    var exception: Throwable? = null
    try {
        return block(this)
    } catch (e: Throwable) {
        exception = e
        throw e
    } finally {
        closeFinally(exception)
    }
}

/**
 * Closes this [AutoCloseable], suppressing possible exception or error thrown by [AutoCloseable.close] function when
 * it's being closed due to some other [cause] exception occurred.
 *
 * The suppressed exception is added to the list of suppressed exceptions of [cause] exception.
 */
fun ThreadContext.StoredContext.closeFinally(cause: Throwable?) = when (cause) {
    null -> close()
    else -> try {
        close()
    } catch (closeException: Throwable) {
        cause.addSuppressed(closeException)
    }
}
