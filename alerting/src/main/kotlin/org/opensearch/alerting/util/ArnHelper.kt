/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.alerting.util

import software.amazon.awssdk.arns.Arn

/**
 * Consolidated ARN parsing utility using the AWS SDK v2 [Arn] parser.
 */
object ArnHelper {

    /**
     * Parses an ARN string and returns the account ID and resource ID.
     * @throws IllegalArgumentException if the ARN is malformed, missing account ID, or missing resource.
     */
    fun parseArn(arn: String): Pair<String, String> {
        val parsed = try {
            Arn.fromString(arn)
        } catch (e: Exception) {
            throw IllegalArgumentException("Invalid ARN format: $arn", e)
        }
        val accountId = parsed.accountId()
            .orElseThrow { IllegalArgumentException("Missing account ID in ARN: $arn") }
        require(accountId.isNotBlank()) { "Blank account ID in ARN: $arn" }
        val resource = parsed.resourceAsString()
        require(resource.isNotBlank()) { "Missing resource in ARN: $arn" }
        val resourceId = resource.substringAfter("/")
        return accountId to resourceId
    }
}
