/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.alerting.destination.factory;

import org.opensearch.alerting.destination.message.BaseMessage;
import org.opensearch.alerting.destination.message.DestinationType;
import org.opensearch.alerting.destination.response.BaseResponse;

/**
 * Interface which enables to plug in multiple notification Channel Factories.
 *
 * @param <T> message object of type [{@link DestinationType}]
 * @param <Y> client to publish above message
 */
public interface DestinationFactory<T extends BaseMessage, Y> {
    BaseResponse publish(T message);

    Y getClient(T message);
}
