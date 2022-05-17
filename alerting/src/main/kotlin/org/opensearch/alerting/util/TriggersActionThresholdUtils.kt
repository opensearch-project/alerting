/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.alerting.util

import kotlin.math.min

object TriggersActionThresholdUtils {


    /**
    Get threshold value
     */
    fun getThreshold(
        p: TriggersActionThresholdParams,
        currentTriggerActionSize: Int,
        triggerMaxActions: Int,
    ): Int {
        return if (p.surplusActionCount <= 0) {
            0
        } else {
            val tmpThreshold = if (triggerMaxActions >= 0) {
                min(min(triggerMaxActions, p.surplusActionCount), currentTriggerActionSize)
            } else {
                min(currentTriggerActionSize, p.surplusActionCount)
            }
            p.surplusActionCount -= tmpThreshold
            tmpThreshold
        }

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
