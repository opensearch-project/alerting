/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.alerting;

import org.apache.hc.core5.http.ContentType;
import org.apache.hc.core5.http.Header;
import org.apache.hc.core5.http.HttpEntity;
import org.apache.hc.core5.http.io.entity.StringEntity;
import org.junit.Assert;
import org.opensearch.alerting.monitor.runners.SampleRemoteDocLevelMonitorRunner;
import org.opensearch.alerting.monitor.runners.SampleRemoteMonitorRunner1;
import org.opensearch.alerting.monitor.runners.SampleRemoteMonitorRunner2;
import org.opensearch.client.Request;
import org.opensearch.client.RequestOptions;
import org.opensearch.client.Response;
import org.opensearch.client.RestClient;
import org.opensearch.client.WarningsHandler;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.xcontent.LoggingDeprecationHandler;
import org.opensearch.common.xcontent.json.JsonXContent;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.test.rest.OpenSearchRestTestCase;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

public class SampleRemoteMonitorIT extends OpenSearchRestTestCase {

    @SuppressWarnings("unchecked")
    public void testSingleSampleMonitor() throws IOException, InterruptedException {
        Response response = makeRequest(client(), "POST", "_plugins/_sample_remote_monitor/monitor", Map.of("run_monitor", "single"), null);
        Assert.assertEquals("Unable to create remote monitor", RestStatus.OK, RestStatus.fromCode(response.getStatusLine().getStatusCode()));

        Map<String, Object> responseJson = JsonXContent.jsonXContent.createParser(
                NamedXContentRegistry.EMPTY,
                LoggingDeprecationHandler.INSTANCE,
                response.getEntity().getContent()
        ).map();
        String monitorId = responseJson.get("_id").toString();

        response = makeRequest(client(), "POST", "/_plugins/_alerting/monitors/" + monitorId + "/_execute", Map.of(), null);
        Assert.assertEquals("Unable to execute remote monitor", RestStatus.OK, RestStatus.fromCode(response.getStatusLine().getStatusCode()));

        AtomicBoolean found = new AtomicBoolean(false);
        OpenSearchRestTestCase.waitUntil(
                () -> {
                    try {
                        Response searchResponse = makeRequest(client(), "POST", SampleRemoteMonitorRunner1.SAMPLE_MONITOR_RUNNER1_INDEX + "/_search", Map.of(),
                                new StringEntity("{\"query\":{\"match_all\":{}}}", ContentType.APPLICATION_JSON));
                        Map<String, Object> searchResponseJson = JsonXContent.jsonXContent.createParser(
                                NamedXContentRegistry.EMPTY,
                                LoggingDeprecationHandler.INSTANCE,
                                searchResponse.getEntity().getContent()
                        ).map();
                        found.set(Integer.parseInt((((Map<String, Object>) ((Map<String, Object>) searchResponseJson.get("hits")).get("total")).get("value")).toString()) == 1);
                        return found.get();
                    } catch (IOException ex) {
                        return false;
                    }
                }, 10, TimeUnit.SECONDS);
        Assert.assertTrue(found.get());
    }

    @SuppressWarnings("unchecked")
    public void testMultipleSampleMonitors() throws IOException, InterruptedException {
        Response response = makeRequest(client(), "POST", "_plugins/_sample_remote_monitor/monitor", Map.of("run_monitor", "multiple"), null);
        Assert.assertEquals("Unable to create remote monitor", RestStatus.OK, RestStatus.fromCode(response.getStatusLine().getStatusCode()));

        Map<String, Object> responseJson = JsonXContent.jsonXContent.createParser(
                NamedXContentRegistry.EMPTY,
                LoggingDeprecationHandler.INSTANCE,
                response.getEntity().getContent()
        ).map();
        String monitorIds = responseJson.get("_id").toString();
        String firstMonitorId = monitorIds.split(" ")[0];
        String secondMonitorId = monitorIds.split(" ")[1];

        response = makeRequest(client(), "POST", "/_plugins/_alerting/monitors/" + firstMonitorId + "/_execute", Map.of(), null);
        Assert.assertEquals("Unable to execute remote monitor", RestStatus.OK, RestStatus.fromCode(response.getStatusLine().getStatusCode()));
        response = makeRequest(client(), "POST", "/_plugins/_alerting/monitors/" + secondMonitorId + "/_execute", Map.of(), null);
        Assert.assertEquals("Unable to execute remote monitor", RestStatus.OK, RestStatus.fromCode(response.getStatusLine().getStatusCode()));

        AtomicBoolean found = new AtomicBoolean(false);
        OpenSearchRestTestCase.waitUntil(
                () -> {
                    try {
                        Response searchResponse = makeRequest(client(), "POST", SampleRemoteMonitorRunner1.SAMPLE_MONITOR_RUNNER1_INDEX + "/_search", Map.of(),
                                new StringEntity("{\"query\":{\"match_all\":{}}}", ContentType.APPLICATION_JSON));
                        Map<String, Object> searchResponseJson = JsonXContent.jsonXContent.createParser(
                                NamedXContentRegistry.EMPTY,
                                LoggingDeprecationHandler.INSTANCE,
                                searchResponse.getEntity().getContent()
                        ).map();
                        found.set(Integer.parseInt((((Map<String, Object>) ((Map<String, Object>) searchResponseJson.get("hits")).get("total")).get("value")).toString()) == 1);
                        return found.get();
                    } catch (IOException ex) {
                        return false;
                    }
                }, 10, TimeUnit.SECONDS);
        Assert.assertTrue(found.get());

        found.set(false);
        OpenSearchRestTestCase.waitUntil(
                () -> {
                    try {
                        Response searchResponse = makeRequest(client(), "POST", SampleRemoteMonitorRunner2.SAMPLE_MONITOR_RUNNER2_INDEX + "/_search", Map.of(),
                                new StringEntity("{\"query\":{\"match_all\":{}}}", ContentType.APPLICATION_JSON));
                        Map<String, Object> searchResponseJson = JsonXContent.jsonXContent.createParser(
                                NamedXContentRegistry.EMPTY,
                                LoggingDeprecationHandler.INSTANCE,
                                searchResponse.getEntity().getContent()
                        ).map();
                        found.set(Integer.parseInt((((Map<String, Object>) ((Map<String, Object>) searchResponseJson.get("hits")).get("total")).get("value")).toString()) == 1);
                        return found.get();
                    } catch (IOException ex) {
                        return false;
                    }
                }, 1, TimeUnit.SECONDS);
        Assert.assertTrue(found.get());
    }

    @SuppressWarnings("unchecked")
    public void testSampleRemoteDocLevelMonitor() throws IOException, InterruptedException {
        createIndex("index", Settings.builder().put("number_of_shards", "7").build());
        Response response = makeRequest(client(), "POST", "_plugins/_sample_remote_monitor/monitor", Map.of("run_monitor", "doc_level"), null);
        Assert.assertEquals("Unable to create remote monitor", RestStatus.OK, RestStatus.fromCode(response.getStatusLine().getStatusCode()));

        Map<String, Object> responseJson = JsonXContent.jsonXContent.createParser(
                NamedXContentRegistry.EMPTY,
                LoggingDeprecationHandler.INSTANCE,
                response.getEntity().getContent()
        ).map();
        String monitorId = responseJson.get("_id").toString();

        response = makeRequest(client(), "POST", "/_plugins/_alerting/monitors/" + monitorId + "/_execute", Map.of(), null);
        Assert.assertEquals("Unable to execute remote monitor", RestStatus.OK, RestStatus.fromCode(response.getStatusLine().getStatusCode()));

        AtomicBoolean found = new AtomicBoolean(false);
        OpenSearchRestTestCase.waitUntil(
                () -> {
                    try {
                        Response searchResponse = makeRequest(client(), "POST", SampleRemoteDocLevelMonitorRunner.SAMPLE_REMOTE_DOC_LEVEL_MONITOR_RUNNER_INDEX + "/_search", Map.of(),
                                new StringEntity("{\"query\":{\"match_all\":{}}}", ContentType.APPLICATION_JSON));
                        Map<String, Object> searchResponseJson = JsonXContent.jsonXContent.createParser(
                                NamedXContentRegistry.EMPTY,
                                LoggingDeprecationHandler.INSTANCE,
                                searchResponse.getEntity().getContent()
                        ).map();
                        found.set(Integer.parseInt((((Map<String, Object>) ((Map<String, Object>) searchResponseJson.get("hits")).get("total")).get("value")).toString()) == 1);
                        return found.get();
                    } catch (IOException ex) {
                        return false;
                    }
                }, 10, TimeUnit.SECONDS);
        Assert.assertTrue(found.get());
    }

    protected Response makeRequest(
            RestClient client,
            String method,
            String endpoint,
            Map<String, String> params,
            HttpEntity entity,
            Header... headers
    ) throws IOException {
        Request request = new Request(method, endpoint);
        RequestOptions.Builder options = RequestOptions.DEFAULT.toBuilder();
        options.setWarningsHandler(WarningsHandler.PERMISSIVE);

        for (Header header : headers) {
            options.addHeader(header.getName(), header.getValue());
        }
        request.setOptions(options.build());
        request.addParameters(params);
        if (entity != null) {
            request.setEntity(entity);
        }
        return client.performRequest(request);
    }
}