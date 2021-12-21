/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.alerting.destination.factory;

import org.opensearch.alerting.destination.message.DestinationType;

import java.util.HashMap;
import java.util.Map;

/*
 * This class helps in fetching the right Channel Factory based on the
 * type of the channel.
 * A channel could be  Email, Webhook etc
 */
public class DestinationFactoryProvider {

    private static Map<DestinationType, DestinationFactory> destinationFactoryMap = new HashMap<>();

    static {
        destinationFactoryMap.put(DestinationType.CHIME, new ChimeDestinationFactory());
        destinationFactoryMap.put(DestinationType.SLACK, new SlackDestinationFactory());
        destinationFactoryMap.put(DestinationType.CUSTOMWEBHOOK, new CustomWebhookDestinationFactory());
        destinationFactoryMap.put(DestinationType.EMAIL, new EmailDestinationFactory());
    }

    /**
     * Fetches the right channel factory based on the type of the channel
     *
     * @param destinationType [{@link DestinationType}]
     * @return DestinationFactory factory object for above destination type
     */
    public static DestinationFactory getFactory(DestinationType destinationType) {
        if (!destinationFactoryMap.containsKey(destinationType)) {
            throw new IllegalArgumentException("Invalid channel type");
        }
        return destinationFactoryMap.get(destinationType);
    }

    /*
     *  This function is to mock hooks for the unit test
     */
     public static void setFactory(DestinationType type, DestinationFactory factory) {
        destinationFactoryMap.put(type, factory);
    }
}
