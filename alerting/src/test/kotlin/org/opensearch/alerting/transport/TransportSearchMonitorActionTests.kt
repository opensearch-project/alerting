/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.alerting.transport

import org.junit.Before
import org.mockito.ArgumentCaptor
import org.mockito.ArgumentMatchers.any
import org.mockito.Mockito
import org.mockito.Mockito.verify
import org.opensearch.action.support.ActionFilters
import org.opensearch.alerting.AlertingPlugin.Companion.TENANT_ID_HEADER
import org.opensearch.alerting.settings.AlertingSettings
import org.opensearch.cluster.service.ClusterService
import org.opensearch.common.settings.ClusterSettings
import org.opensearch.common.settings.Setting
import org.opensearch.common.settings.Settings
import org.opensearch.common.util.concurrent.ThreadContext
import org.opensearch.core.action.ActionListener
import org.opensearch.core.common.io.stream.NamedWriteableRegistry
import org.opensearch.remote.metadata.client.SdkClient
import org.opensearch.remote.metadata.client.SearchDataObjectRequest
import org.opensearch.remote.metadata.client.SearchDataObjectResponse
import org.opensearch.test.OpenSearchTestCase
import org.opensearch.threadpool.ThreadPool
import org.opensearch.transport.TransportService
import org.opensearch.transport.client.Client
import java.util.concurrent.CompletableFuture
import java.util.concurrent.CompletionStage
import org.mockito.Mockito.`when` as whenever
class TransportSearchMonitorActionTests : OpenSearchTestCase() {

    private lateinit var client: Client
    private lateinit var sdkClient: SdkClient
    private lateinit var clusterService: ClusterService
    private lateinit var threadPool: ThreadPool
    private lateinit var threadContext: ThreadContext

    @Before
    fun setup() {
        client = Mockito.mock(Client::class.java)
        sdkClient = Mockito.mock(SdkClient::class.java)
        clusterService = Mockito.mock(ClusterService::class.java)
        threadPool = Mockito.mock(ThreadPool::class.java)
        threadContext = ThreadContext(Settings.EMPTY)

        whenever(client.threadPool()).thenReturn(threadPool)
        whenever(threadPool.threadContext).thenReturn(threadContext)

        val settingSet = hashSetOf<Setting<*>>()
        settingSet.addAll(ClusterSettings.BUILT_IN_CLUSTER_SETTINGS)
        settingSet.add(AlertingSettings.FILTER_BY_BACKEND_ROLES)
        val clusterSettings = ClusterSettings(Settings.EMPTY, settingSet)
        whenever(clusterService.clusterSettings).thenReturn(clusterSettings)
    }

    fun `test search passes tenantId to SDK`() {
        val expectedTenantId = "test-tenant:test-scope"
        threadContext.putHeader(TENANT_ID_HEADER, expectedTenantId)

        val future: CompletionStage<SearchDataObjectResponse> =
            CompletableFuture.completedFuture(SearchDataObjectResponse(null as org.opensearch.action.search.SearchResponse?))
        whenever(sdkClient.searchDataObjectAsync(any(SearchDataObjectRequest::class.java))).thenReturn(future)

        val action = createAction()

        @Suppress("UNCHECKED_CAST")
        val listener = Mockito.mock(ActionListener::class.java) as ActionListener<org.opensearch.action.search.SearchResponse>
        action.search(
            org.opensearch.action.search.SearchRequest(".opendistro-alerting-config"),
            listener
        )

        val captor = ArgumentCaptor.forClass(SearchDataObjectRequest::class.java)
        verify(sdkClient).searchDataObjectAsync(captor.capture())
        assertEquals(expectedTenantId, captor.value.tenantId())
    }

    fun `test search SDK exception propagated to listener`() {
        threadContext.putHeader(TENANT_ID_HEADER, "test-tenant:test-scope")

        val future: CompletionStage<SearchDataObjectResponse> = CompletableFuture<SearchDataObjectResponse>().also {
            it.completeExceptionally(RuntimeException("SDK search failed"))
        }
        whenever(sdkClient.searchDataObjectAsync(any(SearchDataObjectRequest::class.java))).thenReturn(future)

        val action = createAction()

        @Suppress("UNCHECKED_CAST")
        val listener = Mockito.mock(ActionListener::class.java) as ActionListener<org.opensearch.action.search.SearchResponse>
        action.search(
            org.opensearch.action.search.SearchRequest(".opendistro-alerting-config"),
            listener
        )

        verify(listener).onFailure(any())
    }

    fun `test search null response returns empty search response`() {
        threadContext.putHeader(TENANT_ID_HEADER, "test-tenant:test-scope")

        val future: CompletionStage<SearchDataObjectResponse> =
            CompletableFuture.completedFuture(SearchDataObjectResponse(null as org.opensearch.action.search.SearchResponse?))
        whenever(sdkClient.searchDataObjectAsync(any(SearchDataObjectRequest::class.java))).thenReturn(future)

        val action = createAction()

        @Suppress("UNCHECKED_CAST")
        val listener = Mockito.mock(ActionListener::class.java) as ActionListener<org.opensearch.action.search.SearchResponse>
        action.search(
            org.opensearch.action.search.SearchRequest(".opendistro-alerting-config"),
            listener
        )

        verify(listener).onResponse(any())
    }

    fun `test search IndexNotFoundException returns empty response`() {
        threadContext.putHeader(TENANT_ID_HEADER, "test-tenant:test-scope")

        val future: CompletionStage<SearchDataObjectResponse> = CompletableFuture<SearchDataObjectResponse>().also {
            it.completeExceptionally(org.opensearch.index.IndexNotFoundException(".opendistro-alerting-config"))
        }
        whenever(sdkClient.searchDataObjectAsync(any(SearchDataObjectRequest::class.java))).thenReturn(future)

        val action = createAction()

        @Suppress("UNCHECKED_CAST")
        val listener = Mockito.mock(ActionListener::class.java) as ActionListener<org.opensearch.action.search.SearchResponse>
        action.search(
            org.opensearch.action.search.SearchRequest(".opendistro-alerting-config"),
            listener
        )

        verify(listener).onResponse(any())
    }

    private fun createAction(): TransportSearchMonitorAction {
        return TransportSearchMonitorAction(
            Mockito.mock(TransportService::class.java),
            Settings.EMPTY,
            client,
            clusterService,
            Mockito.mock(ActionFilters::class.java),
            Mockito.mock(NamedWriteableRegistry::class.java),
            sdkClient
        )
    }
}
