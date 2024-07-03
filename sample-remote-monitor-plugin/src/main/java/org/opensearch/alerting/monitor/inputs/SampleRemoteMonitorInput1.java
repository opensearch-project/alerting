/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.alerting.monitor.inputs;

import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.common.io.stream.Writeable;

import java.io.IOException;
import java.util.Map;

public class SampleRemoteMonitorInput1 implements Writeable {

    private String a;

    private Map<String, Object> b;

    private int c;

    public SampleRemoteMonitorInput1(String a, Map<String, Object> b, int c) {
        this.a = a;
        this.b = b;
        this.c = c;
    }

    public SampleRemoteMonitorInput1(StreamInput sin) throws IOException {
        this(
                sin.readString(),
                sin.readMap(),
                sin.readInt()
        );
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(a);
        out.writeMap(b);
        out.writeInt(c);
    }

    public int getC() {
        return c;
    }

    public Map<String, Object> getB() {
        return b;
    }

    public String getA() {
        return a;
    }
}