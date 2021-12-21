/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.alerting.destination;

import org.opensearch.alerting.destination.factory.DestinationFactory;
import org.opensearch.alerting.destination.factory.DestinationFactoryProvider;
import org.opensearch.alerting.destination.message.BaseMessage;
import org.opensearch.alerting.destination.response.BaseResponse;

import java.io.IOException;
import java.security.AccessController;
import java.security.PrivilegedAction;
import org.opensearch.alerting.destination.response.BaseResponse;

/**
 * This is a client facing Notification class to publish the messages
 * to the Notification channels like chime, slack, webhooks etc
 */
public class Notification {
    private DestinationFactoryProvider factoryProvider;

    /**
     * Publishes the notification message to the corresponding notification
     * channel
     *
     * @param notificationMessage
     * @return BaseResponse
     */
    public static BaseResponse publish(BaseMessage notificationMessage) throws IOException {
            return AccessController.doPrivileged((PrivilegedAction<BaseResponse>) () -> {
                DestinationFactory destinationFactory = DestinationFactoryProvider.getFactory(notificationMessage.getChannelType());
                return destinationFactory.publish(notificationMessage);
            });
    }
}

