/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.alerting.destination;

import org.opensearch.alerting.destination.client.DestinationHttpClient;
import org.opensearch.alerting.destination.factory.ChimeDestinationFactory;
import org.opensearch.alerting.destination.factory.DestinationFactoryProvider;
import org.opensearch.alerting.destination.message.BaseMessage;
import org.opensearch.alerting.destination.message.ChimeMessage;
import org.opensearch.alerting.destination.message.DestinationType;
import org.opensearch.alerting.destination.response.DestinationResponse;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.message.BasicStatusLine;
import org.easymock.EasyMock;
import org.opensearch.rest.RestStatus;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class ChimeDestinationTest {

    @Test
    public void testChimeMessage_NullEntityResponse() throws Exception {
        CloseableHttpClient mockHttpClient = EasyMock.createMock(CloseableHttpClient.class);

        // The DestinationHttpClient replaces a null entity with "{}".
        DestinationResponse expectedChimeResponse = new DestinationResponse.Builder()
                .withResponseContent("{}")
                .withStatusCode(RestStatus.OK.getStatus())
                .build();
        CloseableHttpResponse httpResponse = EasyMock.createMock(CloseableHttpResponse.class);
        EasyMock.expect(mockHttpClient.execute(EasyMock.anyObject(HttpPost.class))).andReturn(httpResponse);

        BasicStatusLine mockStatusLine = EasyMock.createMock(BasicStatusLine.class);

        EasyMock.expect(httpResponse.getStatusLine()).andReturn(mockStatusLine);
        EasyMock.expect(httpResponse.getEntity()).andReturn(null).anyTimes();
        EasyMock.expect(mockStatusLine.getStatusCode()).andReturn(RestStatus.OK.getStatus());
        EasyMock.replay(mockHttpClient);
        EasyMock.replay(httpResponse);
        EasyMock.replay(mockStatusLine);

        DestinationHttpClient httpClient = new DestinationHttpClient();
        httpClient.setHttpClient(mockHttpClient);
        ChimeDestinationFactory chimeDestinationFactory = new ChimeDestinationFactory();
        chimeDestinationFactory.setClient(httpClient);

        DestinationFactoryProvider.setFactory(DestinationType.CHIME, chimeDestinationFactory);

        String message = "{\"Content\":\"Message gughjhjlkh Body emoji test: :) :+1: " +
                "link test: http://sample.com email test: marymajor@example.com All member callout: " +
                "@All All Present member callout: @Present\"}";
        BaseMessage bm = new ChimeMessage.Builder("abc").withMessage(message).
                withUrl("https://abc/com").build();
        DestinationResponse actualChimeResponse = (DestinationResponse) Notification.publish(bm);

        assertEquals(expectedChimeResponse.getResponseContent(), actualChimeResponse.getResponseContent());
        assertEquals(expectedChimeResponse.getStatusCode(), actualChimeResponse.getStatusCode());
    }

    @Test
    public void testChimeMessage_EmptyEntityResponse() throws Exception {
        CloseableHttpClient mockHttpClient = EasyMock.createMock(CloseableHttpClient.class);

        DestinationResponse expectedChimeResponse = new DestinationResponse.Builder()
                .withResponseContent("")
                .withStatusCode(RestStatus.OK.getStatus())
                .build();
        CloseableHttpResponse httpResponse = EasyMock.createMock(CloseableHttpResponse.class);
        EasyMock.expect(mockHttpClient.execute(EasyMock.anyObject(HttpPost.class))).andReturn(httpResponse);

        BasicStatusLine mockStatusLine = EasyMock.createMock(BasicStatusLine.class);

        EasyMock.expect(httpResponse.getStatusLine()).andReturn(mockStatusLine);
        EasyMock.expect(httpResponse.getEntity()).andReturn(new StringEntity("")).anyTimes();
        EasyMock.expect(mockStatusLine.getStatusCode()).andReturn(RestStatus.OK.getStatus());
        EasyMock.replay(mockHttpClient);
        EasyMock.replay(httpResponse);
        EasyMock.replay(mockStatusLine);

        DestinationHttpClient httpClient = new DestinationHttpClient();
        httpClient.setHttpClient(mockHttpClient);
        ChimeDestinationFactory chimeDestinationFactory = new ChimeDestinationFactory();
        chimeDestinationFactory.setClient(httpClient);

        DestinationFactoryProvider.setFactory(DestinationType.CHIME, chimeDestinationFactory);

        String message = "{\"Content\":\"Message gughjhjlkh Body emoji test: :) :+1: " +
                "link test: http://sample.com email test: marymajor@example.com All member callout: " +
                "@All All Present member callout: @Present\"}";
        BaseMessage bm = new ChimeMessage.Builder("abc").withMessage(message).
                withUrl("https://abc/com").build();
        DestinationResponse actualChimeResponse = (DestinationResponse) Notification.publish(bm);

        assertEquals(expectedChimeResponse.getResponseContent(), actualChimeResponse.getResponseContent());
        assertEquals(expectedChimeResponse.getStatusCode(), actualChimeResponse.getStatusCode());
    }

    @Test
    public void testChimeMessage_NonemptyEntityResponse() throws Exception {
        String responseContent = "It worked!";

        CloseableHttpClient mockHttpClient = EasyMock.createMock(CloseableHttpClient.class);

        DestinationResponse expectedChimeResponse = new DestinationResponse.Builder().withResponseContent(responseContent)
                .withStatusCode(RestStatus.OK.getStatus()).build();
        CloseableHttpResponse httpResponse = EasyMock.createMock(CloseableHttpResponse.class);
        EasyMock.expect(mockHttpClient.execute(EasyMock.anyObject(HttpPost.class))).andReturn(httpResponse);

        BasicStatusLine mockStatusLine = EasyMock.createMock(BasicStatusLine.class);

        EasyMock.expect(httpResponse.getStatusLine()).andReturn(mockStatusLine);
        EasyMock.expect(httpResponse.getEntity()).andReturn(new StringEntity(responseContent)).anyTimes();
        EasyMock.expect(mockStatusLine.getStatusCode()).andReturn(RestStatus.OK.getStatus());
        EasyMock.replay(mockHttpClient);
        EasyMock.replay(httpResponse);
        EasyMock.replay(mockStatusLine);

        DestinationHttpClient httpClient = new DestinationHttpClient();
        httpClient.setHttpClient(mockHttpClient);
        ChimeDestinationFactory chimeDestinationFactory = new ChimeDestinationFactory();
        chimeDestinationFactory.setClient(httpClient);

        DestinationFactoryProvider.setFactory(DestinationType.CHIME, chimeDestinationFactory);

        String message = "{\"Content\":\"Message gughjhjlkh Body emoji test: :) :+1: " +
                "link test: http://sample.com email test: marymajor@example.com All member callout: " +
                "@All All Present member callout: @Present\"}";
        BaseMessage bm = new ChimeMessage.Builder("abc").withMessage(message).
                withUrl("https://abc/com").build();
        DestinationResponse actualChimeResponse = (DestinationResponse) Notification.publish(bm);

        assertEquals(expectedChimeResponse.getResponseContent(), actualChimeResponse.getResponseContent());
        assertEquals(expectedChimeResponse.getStatusCode(), actualChimeResponse.getStatusCode());
    }

    @Test(expected = IllegalArgumentException.class)
    public void testUrlMissingMessage() {
        try {
            ChimeMessage message = new ChimeMessage.Builder("chime")
                    .withMessage("dummyMessage").build();
        } catch (Exception ex) {
            assertEquals("url is invalid or empty", ex.getMessage());
            throw ex;
        }
    }

    @Test(expected = IllegalArgumentException.class)
    public void testContentMissingMessage() {
        try {
            ChimeMessage message = new ChimeMessage.Builder("chime")
                    .withUrl("abc.com").build();
        } catch (Exception ex) {
            assertEquals("Message content is missing", ex.getMessage());
            throw ex;
        }
    }
}
