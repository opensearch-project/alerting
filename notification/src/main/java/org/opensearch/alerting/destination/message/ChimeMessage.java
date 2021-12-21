/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.alerting.destination.message;

import org.opensearch.common.Strings;

/**
 * This class holds the contents of an Chime message
 */
public class ChimeMessage extends BaseMessage {
    private String message;
    private ChimeMessage(final DestinationType destinationType,
                         final String destinationName,
                         final String url,
                         final String message) {

        super(destinationType, destinationName, message, url);

        if (DestinationType.CHIME != destinationType) {
            throw new IllegalArgumentException("Channel Type does not match CHIME");
        }

        if (Strings.isNullOrEmpty(message)) {
            throw new IllegalArgumentException("Message content is missing");
        }

        this.message = message;
    }

    @Override
    public String toString() {
        return "DestinationType: " + destinationType + ", DestinationName:" +  destinationName +
                ", Url: " + url + ", Message: " + message;
    }

    public static class Builder {
        private String message;
        private DestinationType destinationType;
        private String destinationName;
        private String url;

        public Builder(String destinationName) {
            this.destinationName = destinationName;
            this.destinationType = DestinationType.CHIME;
        }

        public ChimeMessage.Builder withMessage(String message) {
            this.message = message;
            return this;
        }

        public ChimeMessage.Builder withUrl(String url) {
            this.url = url;
            return this;
        }

        public ChimeMessage build() {
            ChimeMessage chimeMessage = new ChimeMessage(this.destinationType, this.destinationName, this.url,
                     this.message);
            return chimeMessage;
        }
    }

    public String getUrl() {
        return url;
    }
}
