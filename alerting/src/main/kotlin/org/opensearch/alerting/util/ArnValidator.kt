/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.alerting.util

import org.opensearch.OpenSearchStatusException
import org.opensearch.commons.alerting.model.Target
import org.opensearch.core.rest.RestStatus

/**
 * Validates the ARN in a monitor's [Target] using [ArnHelper].
 * Throws [OpenSearchStatusException] with 400 status if the ARN is invalid.
 */
object ArnValidator {

    fun validateTargetArn(target: Target?) {
        if (target == null || target.type == Target.LOCAL) return
        if (target.arn.isBlank()) {
            throw OpenSearchStatusException(
                "target.arn is required when target type is not LOCAL",
                RestStatus.BAD_REQUEST
            )
        }
        try {
            ArnHelper.parseArn(target.arn)
        } catch (e: IllegalArgumentException) {
            throw OpenSearchStatusException(
                "target.arn is not a valid ARN: ${target.arn}",
                RestStatus.BAD_REQUEST
            )
        }
    }
}
