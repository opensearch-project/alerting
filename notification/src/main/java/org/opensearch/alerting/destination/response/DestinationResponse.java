/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.alerting.destination.response;

/**
 * This class is a place holder for destination response metadata
 */
public class DestinationResponse extends BaseResponse {

    private String responseContent;

    private DestinationResponse(final String responseString, final int statusCode) {
        super(statusCode);
        if (responseString == null) {
            throw new IllegalArgumentException("Response is missing");
        }
        this.responseContent = responseString;
    }

    public static class Builder {
        private String responseContent;
        private Integer statusCode = null;

        public DestinationResponse.Builder withResponseContent(String responseContent) {
            this.responseContent = responseContent;
            return this;
        }

        public DestinationResponse.Builder withStatusCode(Integer statusCode) {
            this.statusCode = statusCode;
            return this;
        }

        public DestinationResponse build() {
            return new DestinationResponse(responseContent, statusCode);
        }
    }

    public String getResponseContent() {
        return this.responseContent;
    }
}
