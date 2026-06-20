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
import org.opensearch.alerting.action.GetDestinationsResponse
import org.opensearch.alerting.settings.AlertingSettings
import org.opensearch.cluster.service.ClusterService
import org.opensearch.common.settings.ClusterSettings
import org.opensearch.common.settings.Setting
import org.opensearch.common.settings.Settings
import org.opensearch.common.util.concurrent.ThreadContext
import org.opensearch.core.action.ActionListener
import org.opensearch.core.xcontent.NamedXContentRegistry
import org.opensearch.remote.metadata.client.SdkClient
import org.opensearch.remote.metadata.client.SearchDataObjectRequest
import org.opensearch.remote.metadata.client.SearchDataObjectResponse
import org.opensearch.search.builder.SearchSourceBuilder
import org.opensearch.test.OpenSearchTestCase
import org.opensearch.threadpool.ThreadPool
import org.opensearch.transport.TransportService
import org.opensearch.transport.client.Client
import java.util.concurrent.CompletableFuture
import java.util.concurrent.CompletionStage
import org.mockito.Mockito.`when` as whenever
class TransportGetDestinationsActionTests : OpenSearchTestCase() {

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

        val action = TransportGetDestinationsAction(
            Mockito.mock(TransportService::class.java),
            client,
            clusterService,
            Mockito.mock(ActionFilters::class.java),
            Settings.EMPTY,
            Mockito.mock(NamedXContentRegistry::class.java),
            sdkClient
        )

        @Suppress("UNCHECKED_CAST")
        val listener = Mockito.mock(ActionListener::class.java) as ActionListener<GetDestinationsResponse>
        action.search(SearchSourceBuilder(), listener)

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

        val action = TransportGetDestinationsAction(
            Mockito.mock(TransportService::class.java),
            client,
            clusterService,
            Mockito.mock(ActionFilters::class.java),
            Settings.EMPTY,
            Mockito.mock(NamedXContentRegistry::class.java),
            sdkClient
        )

        @Suppress("UNCHECKED_CAST")
        val listener = Mockito.mock(ActionListener::class.java) as ActionListener<GetDestinationsResponse>
        action.search(SearchSourceBuilder(), listener)

        verify(listener).onFailure(any())
    }

    fun `test search null response returns empty destinations`() {
        threadContext.putHeader(TENANT_ID_HEADER, "test-tenant:test-scope")

        val future: CompletionStage<SearchDataObjectResponse> =
            CompletableFuture.completedFuture(SearchDataObjectResponse(null as org.opensearch.action.search.SearchResponse?))
        whenever(sdkClient.searchDataObjectAsync(any(SearchDataObjectRequest::class.java))).thenReturn(future)

        val action = TransportGetDestinationsAction(
            Mockito.mock(TransportService::class.java),
            client,
            clusterService,
            Mockito.mock(ActionFilters::class.java),
            Settings.EMPTY,
            Mockito.mock(NamedXContentRegistry::class.java),
            sdkClient
        )

        @Suppress("UNCHECKED_CAST")
        val listener = Mockito.mock(ActionListener::class.java) as ActionListener<GetDestinationsResponse>
        action.search(SearchSourceBuilder(), listener)

        val captor = ArgumentCaptor.forClass(GetDestinationsResponse::class.java)
        verify(listener).onResponse(captor.capture())
        assertEquals(0, captor.value.totalDestinations)
    }

    fun `test resolve passes tenantId through to search`() {
        val expectedTenantId = "test-tenant:test-scope"

        val future: CompletionStage<SearchDataObjectResponse> =
            CompletableFuture.completedFuture(SearchDataObjectResponse(null as org.opensearch.action.search.SearchResponse?))
        whenever(sdkClient.searchDataObjectAsync(any(SearchDataObjectRequest::class.java))).thenReturn(future)

        val action = TransportGetDestinationsAction(
            Mockito.mock(TransportService::class.java),
            client,
            clusterService,
            Mockito.mock(ActionFilters::class.java),
            Settings.EMPTY,
            Mockito.mock(NamedXContentRegistry::class.java),
            sdkClient
        )

        @Suppress("UNCHECKED_CAST")
        val listener = Mockito.mock(ActionListener::class.java) as ActionListener<GetDestinationsResponse>
        action.resolve(SearchSourceBuilder(), listener, null, expectedTenantId)

        val captor = ArgumentCaptor.forClass(SearchDataObjectRequest::class.java)
        verify(sdkClient).searchDataObjectAsync(captor.capture())
        assertEquals(expectedTenantId, captor.value.tenantId())
    }

    fun `test resolve passes null tenantId when not provided`() {
        val future: CompletionStage<SearchDataObjectResponse> =
            CompletableFuture.completedFuture(SearchDataObjectResponse(null as org.opensearch.action.search.SearchResponse?))
        whenever(sdkClient.searchDataObjectAsync(any(SearchDataObjectRequest::class.java))).thenReturn(future)

        val action = TransportGetDestinationsAction(
            Mockito.mock(TransportService::class.java),
            client,
            clusterService,
            Mockito.mock(ActionFilters::class.java),
            Settings.EMPTY,
            Mockito.mock(NamedXContentRegistry::class.java),
            sdkClient
        )

        @Suppress("UNCHECKED_CAST")
        val listener = Mockito.mock(ActionListener::class.java) as ActionListener<GetDestinationsResponse>
        action.resolve(SearchSourceBuilder(), listener, null)

        val captor = ArgumentCaptor.forClass(SearchDataObjectRequest::class.java)
        verify(sdkClient).searchDataObjectAsync(captor.capture())
        assertNull(captor.value.tenantId())
    }

    fun `test resolve with resource sharing client skips filterby`() {
        val mockRsc = Mockito.mock(org.opensearch.security.spi.resources.client.ResourceSharingClient::class.java)
        org.opensearch.alerting.ResourceSharingClientAccessor.setResourceSharingClient(mockRsc)

        try {
            val future: CompletionStage<SearchDataObjectResponse> =
                CompletableFuture.completedFuture(SearchDataObjectResponse(null as org.opensearch.action.search.SearchResponse?))
            whenever(sdkClient.searchDataObjectAsync(any(SearchDataObjectRequest::class.java))).thenReturn(future)

            val action = TransportGetDestinationsAction(
                Mockito.mock(TransportService::class.java),
                client,
                clusterService,
                Mockito.mock(ActionFilters::class.java),
                Settings.EMPTY,
                Mockito.mock(NamedXContentRegistry::class.java),
                sdkClient
            )

            // User with backend roles that would normally trigger filterBy
            threadContext.putTransient(
                "opendistro_security_user_info",
                "testuser|role1|backend_role1"
            )

            @Suppress("UNCHECKED_CAST")
            val listener = Mockito.mock(ActionListener::class.java) as ActionListener<GetDestinationsResponse>
            action.resolve(SearchSourceBuilder(), listener, null)

            // Should still call search (resource sharing handles access control)
            verify(sdkClient).searchDataObjectAsync(any(SearchDataObjectRequest::class.java))
        } finally {
            org.opensearch.alerting.ResourceSharingClientAccessor.clear()
        }
    }
}
