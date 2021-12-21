/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.alerting.destination.factory;

import org.opensearch.alerting.destination.client.DestinationEmailClient;
import org.opensearch.alerting.destination.client.DestinationEmailClientPool;
import org.opensearch.alerting.destination.message.EmailMessage;
import org.opensearch.alerting.destination.response.DestinationResponse;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * This class handles the client responsible for submitting the messages to the Email destination.
 */
public class EmailDestinationFactory implements DestinationFactory<EmailMessage, DestinationEmailClient>{

    private DestinationEmailClient destinationEmailClient;

    private static final Logger logger = LogManager.getLogger(EmailDestinationFactory.class);

    public EmailDestinationFactory() {
        this.destinationEmailClient = DestinationEmailClientPool.getEmailClient();
    }

    @Override
    public DestinationResponse publish(EmailMessage message) {
        try {
            String response = getClient(message).execute(message);
            int status = response.equals("Sent") ? 0 : 1;
            return new DestinationResponse.Builder().withStatusCode(status).withResponseContent(response).build();
        } catch (Exception ex) {
            logger.error("Exception publishing Message: " + message.toString(), ex);
            throw new IllegalStateException(ex);
        }
    }

    @Override
    public DestinationEmailClient getClient(EmailMessage message) {
        return destinationEmailClient;
    }

    /*
     *  This function can be used to mock the client for unit test
     */
    public void setClient(DestinationEmailClient client) {
        this.destinationEmailClient = client;
    }

}
