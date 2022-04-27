/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.alerting.util

object TriggersActionThresholdUtils {

    /**
     Get threshold value
     */
    fun getThreshold(
        p: TriggersActionThresholdParams,
        currentTriggerActionSize: Int,
        triggerMaxActions: Int,
        triggerTotalMaxActions: Int
    ): Int {
        var threshold = currentTriggerActionSize
        if (triggerTotalMaxActions == 0) {
            threshold = 0
        } else if (triggerTotalMaxActions > 0) {
            threshold = if (triggerMaxActions >= 0) {
                val temporaryThreshold = if (triggerMaxActions < p.surplusActionCount) triggerMaxActions else p.surplusActionCount
                if (temporaryThreshold < currentTriggerActionSize) temporaryThreshold else currentTriggerActionSize
            } else {
                currentTriggerActionSize
            }
        }
        p.surplusActionCount -= threshold
        return threshold
    }

    /**
     *The construction use of the class here is available for a runBucketLevelMonitor or runQueryLevelMonitor with individual restrictions
     for each method
     *If we want a limit on the two sets, we can input in the same object when the two are executing
     *The current default is to limit the two methods individually
     */
    data class TriggersActionThresholdParams(
        var surplusActionCount: Int
    )
}
