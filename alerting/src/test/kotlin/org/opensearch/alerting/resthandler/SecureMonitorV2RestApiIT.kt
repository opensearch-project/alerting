/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.alerting.resthandler

import org.junit.After
import org.junit.Before
import org.junit.BeforeClass
import org.opensearch.alerting.AlertingRestTestCase
import org.opensearch.client.RestClient
import org.opensearch.commons.rest.SecureRestClientBuilder

class SecureMonitorV2RestApiIT : AlertingRestTestCase() {

    companion object {
        @BeforeClass
        @JvmStatic fun setup() {
            // things to execute once and keep around for the class
            org.junit.Assume.assumeTrue(System.getProperty("security", "false")!!.toBoolean())
        }
    }

    val user = "userD"
    var userClient: RestClient? = null

    @Before
    fun create() {
        if (userClient == null) {
            createUser(user, arrayOf())
            userClient = SecureRestClientBuilder(clusterHosts.toTypedArray(), isHttps(), user, password)
                .setSocketTimeout(60000)
                .setConnectionRequestTimeout(180000)
                .build()
        }
    }

    @After
    fun cleanup() {
        userClient?.close()
        deleteUser(user)
    }
}
