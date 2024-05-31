/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.alerting.monitor.trigger.results;

import org.opensearch.commons.alerting.model.ActionRunResult;
import org.opensearch.commons.alerting.model.TriggerRunResult;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.script.ScriptException;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class SampleRemoteMonitorTriggerRunResult extends TriggerRunResult {

    private Map<String, Map<String, ActionRunResult>> actionResultsMap;

    public SampleRemoteMonitorTriggerRunResult(String triggerName,
                                               Exception error,
                                               Map<String, Map<String, ActionRunResult>> actionResultsMap) {
        super(triggerName, error);
        this.actionResultsMap = actionResultsMap;
    }

    public SampleRemoteMonitorTriggerRunResult readFrom(StreamInput sin) throws IOException {
        return new SampleRemoteMonitorTriggerRunResult(
                sin.readString(),
                sin.readException(),
                readActionResults(sin)
        );
    }

    @Override
    public XContentBuilder internalXContent(XContentBuilder xContentBuilder, Params params) {
        try {
            if (this.getError() instanceof ScriptException) {
                this.setError(new Exception(((ScriptException) getError()).toJsonString(), getError()));
            }
            return xContentBuilder.field("action_results", actionResultsMap);
        } catch (IOException ex) {
            return xContentBuilder;
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeInt(actionResultsMap.size());
        for (var actionResultsEntry: actionResultsMap.entrySet()) {
            String alert = actionResultsEntry.getKey();
            Map<String, ActionRunResult> actionResults = actionResultsEntry.getValue();
            out.writeString(alert);
            out.writeInt(actionResults.size());
            for (var actionResult: actionResults.entrySet()) {
                String id = actionResult.getKey();
                ActionRunResult result = actionResult.getValue();
                out.writeString(id);
                result.writeTo(out);
            }
        }
    }

    private Map<String, Map<String, ActionRunResult>> readActionResults(StreamInput sin) throws IOException {
        Map<String, Map<String, ActionRunResult>> actionResultsMapReconstruct = new HashMap<>();
        int size = sin.readInt();
        int idx = 0;
        while (idx < size) {
            String alert = sin.readString();
            int actionResultsSize = sin.readInt();
            Map<String, ActionRunResult> actionRunResultElem = new HashMap<>();
            int i = 0;
            while (i < actionResultsSize) {
                String actionId = sin.readString();
                ActionRunResult actionResult = ActionRunResult.readFrom(sin);
                actionRunResultElem.put(actionId, actionResult);
                ++i;
            }
            actionResultsMapReconstruct.put(alert, actionRunResultElem);
            ++idx;
        }
        return actionResultsMapReconstruct;
    }
}