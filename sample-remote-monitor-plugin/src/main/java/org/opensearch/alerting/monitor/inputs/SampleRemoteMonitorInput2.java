/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.alerting.monitor.inputs;

import org.opensearch.commons.alerting.model.DocLevelMonitorInput;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.common.io.stream.Writeable;

import java.io.IOException;

public class SampleRemoteMonitorInput2 implements Writeable {

    private String a;

    private DocLevelMonitorInput b;

    public SampleRemoteMonitorInput2(String a, DocLevelMonitorInput b) {
        this.a = a;
        this.b = b;
    }

    public SampleRemoteMonitorInput2(StreamInput sin) throws IOException {
        this(
                sin.readString(),
                DocLevelMonitorInput.readFrom(sin)
        );
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(a);
        b.writeTo(out);
    }

    public String getA() {
        return a;
    }

    public DocLevelMonitorInput getB() {
        return b;
    }
}