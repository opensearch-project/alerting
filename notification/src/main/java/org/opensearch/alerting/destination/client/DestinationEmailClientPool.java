/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.alerting.destination.client;

/**
 *  This class provides Client to the relevant destinations
 */
public final class DestinationEmailClientPool {

    private static final DestinationEmailClient emailClient = new DestinationEmailClient();

    private DestinationEmailClientPool() { }

    public static DestinationEmailClient getEmailClient() {
        return emailClient;
    }
}
