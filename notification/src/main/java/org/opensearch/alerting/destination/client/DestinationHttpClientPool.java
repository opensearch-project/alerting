/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.alerting.destination.client;

/**
 *  This class provides Client to the relevant destinations
 */
public final class DestinationHttpClientPool {

    private static final DestinationHttpClient httpClient = new DestinationHttpClient();

    private DestinationHttpClientPool() { }

    public static DestinationHttpClient getHttpClient() {
        return httpClient;
    }
}
