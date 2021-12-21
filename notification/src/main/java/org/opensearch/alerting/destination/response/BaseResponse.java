/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.alerting.destination.response;

/**
 * This class holds the generic response attributes
 */
public abstract class BaseResponse {
    protected Integer statusCode;

    BaseResponse(final Integer statusCode) {
        if (statusCode == null) {
            throw new IllegalArgumentException("status code is invalid");
        }
        this.statusCode = statusCode;
    }

    public int getStatusCode() {
        return statusCode;
    }
}
