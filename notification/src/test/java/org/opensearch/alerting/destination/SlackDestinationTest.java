/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.alerting.destination;

import org.opensearch.alerting.destination.client.DestinationHttpClient;
import org.opensearch.alerting.destination.factory.DestinationFactoryProvider;
import org.opensearch.alerting.destination.factory.SlackDestinationFactory;
import org.opensearch.alerting.destination.message.BaseMessage;
import org.opensearch.alerting.destination.message.DestinationType;
import org.opensearch.alerting.destination.message.SlackMessage;
import org.opensearch.alerting.destination.response.DestinationResponse;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.message.BasicStatusLine;
import org.easymock.EasyMock;
import org.opensearch.rest.RestStatus;
import org.junit.Assert;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class SlackDestinationTest {

    @Test
    public void testSlackMessage_NullEntityResponse() throws Exception {
        CloseableHttpClient mockHttpClient = EasyMock.createMock(CloseableHttpClient.class);

        // The DestinationHttpClient replaces a null entity with "{}".
        DestinationResponse expectedSlackResponse = new DestinationResponse.Builder()
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
        SlackDestinationFactory slackDestinationFactory = new SlackDestinationFactory();
        slackDestinationFactory.setClient(httpClient);

        DestinationFactoryProvider.setFactory(DestinationType.SLACK, slackDestinationFactory);

        String message = "{\"text\":\"Vamshi Message gughjhjlkh Body emoji test: :) :+1: " +
                "link test: http://sample.com email test: marymajor@example.com All member callout: " +
                "@All All Present member callout: @Present\"}";
        BaseMessage bm = new SlackMessage.Builder("abc").withMessage(message).
                withUrl("https://hooks.slack.com/services/xxxx/xxxxxx/xxxxxxxxx").build();

        DestinationResponse actualSlackResponse = (DestinationResponse) Notification.publish(bm);

        assertEquals(expectedSlackResponse.getResponseContent(), actualSlackResponse.getResponseContent());
        assertEquals(expectedSlackResponse.getStatusCode(), actualSlackResponse.getStatusCode());
    }

    @Test
    public void testSlackMessage_EmptyEntityResponse() throws Exception {
        CloseableHttpClient mockHttpClient = EasyMock.createMock(CloseableHttpClient.class);

        DestinationResponse expectedSlackResponse = new DestinationResponse.Builder()
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
        SlackDestinationFactory slackDestinationFactory = new SlackDestinationFactory();
        slackDestinationFactory.setClient(httpClient);

        DestinationFactoryProvider.setFactory(DestinationType.SLACK, slackDestinationFactory);

        String message = "{\"text\":\"Vamshi Message gughjhjlkh Body emoji test: :) :+1: " +
                "link test: http://sample.com email test: marymajor@example.com All member callout: " +
                "@All All Present member callout: @Present\"}";
        BaseMessage bm = new SlackMessage.Builder("abc").withMessage(message).
                withUrl("https://hooks.slack.com/services/xxxx/xxxxxx/xxxxxxxxx").build();

        DestinationResponse actualSlackResponse = (DestinationResponse) Notification.publish(bm);

        assertEquals(expectedSlackResponse.getResponseContent(), actualSlackResponse.getResponseContent());
        assertEquals(expectedSlackResponse.getStatusCode(), actualSlackResponse.getStatusCode());
    }

    @Test
    public void testSlackMessage_NonemptyEntityResponse() throws Exception {
        String responseContent = "It worked!";

        CloseableHttpClient mockHttpClient = EasyMock.createMock(CloseableHttpClient.class);

        DestinationResponse expectedSlackResponse = new DestinationResponse.Builder()
                .withResponseContent(responseContent)
                .withStatusCode(RestStatus.OK.getStatus())
                .build();
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
        SlackDestinationFactory slackDestinationFactory = new SlackDestinationFactory();
        slackDestinationFactory.setClient(httpClient);

        DestinationFactoryProvider.setFactory(DestinationType.SLACK, slackDestinationFactory);

        String message = "{\"text\":\"Vamshi Message gughjhjlkh Body emoji test: :) :+1: " +
                "link test: http://sample.com email test: marymajor@example.com All member callout: " +
                "@All All Present member callout: @Present\"}";
        BaseMessage bm = new SlackMessage.Builder("abc").withMessage(message).
                withUrl("https://hooks.slack.com/services/xxxx/xxxxxx/xxxxxxxxx").build();

        DestinationResponse actualSlackResponse = (DestinationResponse) Notification.publish(bm);

        assertEquals(expectedSlackResponse.getResponseContent(), actualSlackResponse.getResponseContent());
        assertEquals(expectedSlackResponse.getStatusCode(), actualSlackResponse.getStatusCode());
    }

    @Test(expected = IllegalArgumentException.class)
    public void testUrlMissingMessage() {
        try {
            SlackMessage message = new SlackMessage.Builder("slack")
                    .withMessage("dummyMessage").build();
        } catch (Exception ex) {
            Assert.assertEquals("url is invalid or empty", ex.getMessage());
            throw ex;
        }
    }

    @Test(expected = IllegalArgumentException.class)
    public void testContentMissingMessage() {
        try {
            SlackMessage message = new SlackMessage.Builder("slack")
                    .withUrl("abc.com").build();
        } catch (Exception ex) {
            assertEquals("Message content is missing", ex.getMessage());
            throw ex;
        }
    }
}
