/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.alerting.destination.message;

import org.apache.http.client.utils.URIBuilder;
import org.opensearch.common.Strings;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.Map;

/**
 * This class holds the generic parameters required for a
 * message.
 */
public abstract class BaseMessage {

    protected DestinationType destinationType;
    protected String destinationName;
    protected String url;
    private String content;

    BaseMessage(final DestinationType destinationType, final String destinationName, final String content) {
        if (destinationType == null) {
            throw new IllegalArgumentException("Channel type must be defined");
        }
        if (!Strings.hasLength(destinationName)) {
            throw new IllegalArgumentException("Channel name must be defined");
        }
        this.destinationType = destinationType;
        this.destinationName = destinationName;
        this.content = content;
    }

    BaseMessage(final DestinationType destinationType, final String destinationName,
                final String content, final String url) {
        this(destinationType, destinationName, content);
        if (url == null) {
            throw new IllegalArgumentException("url is invalid or empty");
        }
        this.url = url;
    }

    public void setUrl(String url) {
        this.url = url;
    }
    public DestinationType getChannelType() {
        return destinationType;
    }

    public String getChannelName() {
        return destinationName;
    }

    public String getMessageContent() {
        return content;
    }

    public String getUrl() {
        return url;
    }

    public URI getUri() {
        return buildUri(getUrl().trim(), null, null, -1, null, null);
    }

    protected URI buildUri(String endpoint, String scheme, String host,
                         int port, String path, Map<String, String> queryParams) {
        try {
            if(Strings.isNullOrEmpty(endpoint)) {
                if (Strings.isNullOrEmpty(scheme)) {
                    scheme = "https";
                }
                URIBuilder uriBuilder = new URIBuilder();
                if(queryParams != null) {
                    for (Map.Entry<String, String> e : queryParams.entrySet())
                        uriBuilder.addParameter(e.getKey(), e.getValue());
                }
                return uriBuilder.setScheme(scheme).setHost(host).setPort(port).setPath(path).build();
            }
            return new URIBuilder(endpoint).build();
        } catch (URISyntaxException exception) {
            throw new IllegalStateException("Error creating URI");
        }
    }

}
