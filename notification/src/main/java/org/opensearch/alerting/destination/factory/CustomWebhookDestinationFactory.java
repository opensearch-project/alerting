/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.alerting.destination.factory;

import org.opensearch.alerting.destination.client.DestinationHttpClient;
import org.opensearch.alerting.destination.client.DestinationHttpClientPool;
import org.opensearch.alerting.destination.message.CustomWebhookMessage;
import org.opensearch.alerting.destination.response.DestinationResponse;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.rest.RestStatus;

/**
 * This class handles the client responsible for submitting the messages to custom webhook destination.
 */
public class CustomWebhookDestinationFactory implements DestinationFactory<CustomWebhookMessage, DestinationHttpClient>{

    private static final Logger logger = LogManager.getLogger(CustomWebhookDestinationFactory.class);

    private DestinationHttpClient destinationHttpClient;

    public CustomWebhookDestinationFactory() {
        this.destinationHttpClient = DestinationHttpClientPool.getHttpClient();
    }

    @Override
    public DestinationResponse publish(CustomWebhookMessage message) {
        try {
            String response = getClient(message).execute(message);
            return new DestinationResponse.Builder().withStatusCode(RestStatus.OK.getStatus()).withResponseContent(response).build();
        } catch (Exception ex) {
            logger.error("Exception publishing Message: " + message.toString(), ex);
            throw new IllegalStateException(ex);
        }
    }

    @Override
    public DestinationHttpClient getClient(CustomWebhookMessage message) {
        return destinationHttpClient;
    }

    /*
     *  This function can be used to mock the client for unit test
     */
    public void setClient(DestinationHttpClient client) {
        this.destinationHttpClient = client;
    }

}