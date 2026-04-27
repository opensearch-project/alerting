/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.alerting.service

import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider
import software.amazon.awssdk.services.sts.StsClient
import software.amazon.awssdk.services.sts.auth.StsAssumeRoleCredentialsProvider
import software.amazon.awssdk.services.sts.model.AssumeRoleRequest
import java.util.concurrent.ConcurrentHashMap

/**
 * Caches [AwsCredentialsProvider] instances per account ID, using
 * [StsAssumeRoleCredentialsProvider] for automatic credential refresh on expiry.
 */
class AssumeRoleCredentialsCache(
    private val stsClient: StsClient,
    private val roleArnFormat: String,
    private val sessionPrefix: String = "alerting"
) {
    private val cache = ConcurrentHashMap<String, AwsCredentialsProvider>()

    fun getCredentialsProvider(accountId: String): AwsCredentialsProvider {
        return cache.computeIfAbsent(accountId) {
            StsAssumeRoleCredentialsProvider.builder()
                .stsClient(stsClient)
                .refreshRequest(
                    AssumeRoleRequest.builder()
                        .roleArn(String.format(roleArnFormat, accountId))
                        .roleSessionName("$sessionPrefix-$accountId")
                        .build()
                )
                .build()
        }
    }
}
