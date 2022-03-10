/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.alerting.destination;

import org.opensearch.alerting.destination.client.DestinationHttpClient;
import org.opensearch.alerting.destination.factory.CustomWebhookDestinationFactory;
import org.opensearch.alerting.destination.factory.DestinationFactoryProvider;
import org.opensearch.alerting.destination.message.BaseMessage;
import org.opensearch.alerting.destination.message.CustomWebhookMessage;
import org.opensearch.alerting.destination.message.DestinationType;
import org.opensearch.alerting.destination.response.DestinationResponse;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPatch;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.client.methods.HttpUriRequest;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.message.BasicStatusLine;
import org.easymock.EasyMock;
import org.opensearch.rest.RestStatus;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;


@RunWith(Parameterized.class)
public class CustomWebhookMessageTest {
    @Parameterized.Parameters(name = "Param: {0}={1}")
    public static Object[][] params() {
        return new Object[][]{
                {"POST", HttpPost.class},
                {"PUT", HttpPut.class},
                {"PATCH", HttpPatch.class},
        };
    }

    @Parameterized.Parameter(0)
    public String method;

    @Parameterized.Parameter(1)
    public Class<HttpUriRequest> expectedHttpClass;

    @Test
    public void testCustomWebhookMessage_NullEntityResponse() throws Exception {
        CloseableHttpClient mockHttpClient = EasyMock.createMock(CloseableHttpClient.class);

        // The DestinationHttpClient replaces a null entity with "{}".
        DestinationResponse expectedCustomWebhookResponse = new DestinationResponse.Builder()
                .withResponseContent("{}")
                .withStatusCode(RestStatus.OK.getStatus())
                .build();
        CloseableHttpResponse httpResponse = EasyMock.createMock(CloseableHttpResponse.class);
        EasyMock.expect(mockHttpClient.execute(EasyMock.anyObject(HttpPost.class))).andReturn(httpResponse);
        EasyMock.expect(mockHttpClient.execute(EasyMock.isA(expectedHttpClass))).andReturn(httpResponse);

        BasicStatusLine mockStatusLine = EasyMock.createMock(BasicStatusLine.class);

        EasyMock.expect(httpResponse.getStatusLine()).andReturn(mockStatusLine);
        EasyMock.expect(httpResponse.getEntity()).andReturn(null).anyTimes();
        EasyMock.expect(mockStatusLine.getStatusCode()).andReturn(RestStatus.OK.getStatus());
        EasyMock.replay(mockHttpClient);
        EasyMock.replay(httpResponse);
        EasyMock.replay(mockStatusLine);

        DestinationHttpClient httpClient = new DestinationHttpClient();
        httpClient.setHttpClient(mockHttpClient);
        CustomWebhookDestinationFactory customDestinationFactory = new CustomWebhookDestinationFactory();
        customDestinationFactory.setClient(httpClient);

        DestinationFactoryProvider.setFactory(DestinationType.CUSTOMWEBHOOK, customDestinationFactory);

        Map<String, String> queryParams = new HashMap<String, String>();
        queryParams.put("token", "R2x1UlN4ZHF8MXxxVFJpelJNVDgzdGNwMnVRenJwRFBHUkR0NlhROWhXOVVTZXpiTWx2azVr");

        String message = "{\"Content\":\"Message gughjhjlkh Body emoji test: :) :+1: " +
                "link test: http://sample.com email test: marymajor@example.com " +
                "All member callout: @All All Present member callout: @Present\"}";
        BaseMessage bm = new CustomWebhookMessage.Builder("abc").withHost("hooks.chime.aws").
                withPath("incomingwebhooks/383c0e2b-d028-44f4-8d38-696754bc4574").
                withMessage(message).withMethod(method).
                withQueryParams(queryParams).build();
        DestinationResponse actualCustomResponse = (DestinationResponse) Notification.publish(bm);

        assertEquals(expectedCustomWebhookResponse.getResponseContent(), actualCustomResponse.getResponseContent());
        assertEquals(expectedCustomWebhookResponse.getStatusCode(), actualCustomResponse.getStatusCode());
    }

    @Test
    public void testCustomWebhookMessage_EmptyEntityResponse() throws Exception {
        CloseableHttpClient mockHttpClient = EasyMock.createMock(CloseableHttpClient.class);

        DestinationResponse expectedCustomWebhookResponse = new DestinationResponse.Builder()
                .withResponseContent("")
                .withStatusCode(RestStatus.OK.getStatus())
                .build();
        CloseableHttpResponse httpResponse = EasyMock.createMock(CloseableHttpResponse.class);
        EasyMock.expect(mockHttpClient.execute(EasyMock.isA(expectedHttpClass))).andReturn(httpResponse);

        BasicStatusLine mockStatusLine = EasyMock.createMock(BasicStatusLine.class);

        EasyMock.expect(httpResponse.getStatusLine()).andReturn(mockStatusLine);
        EasyMock.expect(httpResponse.getEntity()).andReturn(new StringEntity("")).anyTimes();
        EasyMock.expect(mockStatusLine.getStatusCode()).andReturn(RestStatus.OK.getStatus());
        EasyMock.replay(mockHttpClient);
        EasyMock.replay(httpResponse);
        EasyMock.replay(mockStatusLine);

        DestinationHttpClient httpClient = new DestinationHttpClient();
        httpClient.setHttpClient(mockHttpClient);
        CustomWebhookDestinationFactory customDestinationFactory = new CustomWebhookDestinationFactory();
        customDestinationFactory.setClient(httpClient);

        DestinationFactoryProvider.setFactory(DestinationType.CUSTOMWEBHOOK, customDestinationFactory);

        Map<String, String> queryParams = new HashMap<String, String>();
        queryParams.put("token", "R2x1UlN4ZHF8MXxxVFJpelJNVDgzdGNwMnVRenJwRFBHUkR0NlhROWhXOVVTZXpiTWx2azVr");

        String message = "{\"Content\":\"Message gughjhjlkh Body emoji test: :) :+1: " +
                "link test: http://sample.com email test: marymajor@example.com " +
                "All member callout: @All All Present member callout: @Present\"}";
        BaseMessage bm = new CustomWebhookMessage.Builder("abc").withHost("hooks.chime.aws").
                withPath("incomingwebhooks/383c0e2b-d028-44f4-8d38-696754bc4574").
                withMessage(message).withMethod(method).
                withQueryParams(queryParams).build();
        DestinationResponse actualCustomResponse = (DestinationResponse) Notification.publish(bm);

        assertEquals(expectedCustomWebhookResponse.getResponseContent(), actualCustomResponse.getResponseContent());
        assertEquals(expectedCustomWebhookResponse.getStatusCode(), actualCustomResponse.getStatusCode());
    }

    @Test
    public void testCustomWebhookMessage_NonemptyEntityResponse() throws Exception {
        String responseContent = "It worked!";

        CloseableHttpClient mockHttpClient = EasyMock.createMock(CloseableHttpClient.class);

        DestinationResponse expectedCustomWebhookResponse = new DestinationResponse.Builder()
                .withResponseContent(responseContent)
                .withStatusCode(RestStatus.OK.getStatus())
                .build();
        CloseableHttpResponse httpResponse = EasyMock.createMock(CloseableHttpResponse.class);
        EasyMock.expect(mockHttpClient.execute(EasyMock.isA(expectedHttpClass))).andReturn(httpResponse);

        BasicStatusLine mockStatusLine = EasyMock.createMock(BasicStatusLine.class);

        EasyMock.expect(httpResponse.getStatusLine()).andReturn(mockStatusLine);
        EasyMock.expect(httpResponse.getEntity()).andReturn(new StringEntity(responseContent)).anyTimes();
        EasyMock.expect(mockStatusLine.getStatusCode()).andReturn(RestStatus.OK.getStatus());
        EasyMock.replay(mockHttpClient);
        EasyMock.replay(httpResponse);
        EasyMock.replay(mockStatusLine);

        DestinationHttpClient httpClient = new DestinationHttpClient();
        httpClient.setHttpClient(mockHttpClient);
        CustomWebhookDestinationFactory customDestinationFactory = new CustomWebhookDestinationFactory();
        customDestinationFactory.setClient(httpClient);

        DestinationFactoryProvider.setFactory(DestinationType.CUSTOMWEBHOOK, customDestinationFactory);

        Map<String, String> queryParams = new HashMap<String, String>();
        queryParams.put("token", "R2x1UlN4ZHF8MXxxVFJpelJNVDgzdGNwMnVRenJwRFBHUkR0NlhROWhXOVVTZXpiTWx2azVr");

        String message = "{\"Content\":\"Message gughjhjlkh Body emoji test: :) :+1: " +
                "link test: http://sample.com email test: marymajor@example.com " +
                "All member callout: @All All Present member callout: @Present\"}";
        BaseMessage bm = new CustomWebhookMessage.Builder("abc").withHost("hooks.chime.aws").
                withPath("incomingwebhooks/383c0e2b-d028-44f4-8d38-696754bc4574").
                withMessage(message).withMethod(method).
                withQueryParams(queryParams).build();
        DestinationResponse actualCustomResponse = (DestinationResponse) Notification.publish(bm);

        assertEquals(expectedCustomWebhookResponse.getResponseContent(), actualCustomResponse.getResponseContent());
        assertEquals(expectedCustomWebhookResponse.getStatusCode(), actualCustomResponse.getStatusCode());
    }

    @Test(expected = IllegalArgumentException.class)
    public void testUrlMissingMessage() {
        try {
            CustomWebhookMessage message = new CustomWebhookMessage.Builder("custom")
                    .withMessage("dummyMessage").build();
        } catch (Exception ex) {
            assertEquals("Either fully qualified URL or host name should be provided", ex.getMessage());
            throw ex;
        }
    }

    @Test(expected = IllegalArgumentException.class)
    public void testContentMissingMessage() {
        try {
            CustomWebhookMessage message = new CustomWebhookMessage.Builder("custom")
                    .withUrl("abc.com").build();
        } catch (Exception ex) {
            assertEquals("Message content is missing", ex.getMessage());
            throw ex;
        }
    }
}
