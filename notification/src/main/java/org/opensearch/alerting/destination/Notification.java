/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 * Modifications Copyright OpenSearch Contributors. See
 * GitHub history for details.
 */

/*
 *   Copyright 2019 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 *   Licensed under the Apache License, Version 2.0 (the "License").
 *   You may not use this file except in compliance with the License.
 *   A copy of the License is located at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *   or in the "license" file accompanying this file. This file is distributed
 *   on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 *   express or implied. See the License for the specific language governing
 *   permissions and limitations under the License.
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

