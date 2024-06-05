/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.alerting;

import org.opensearch.action.support.WriteRequest;
import org.opensearch.alerting.monitor.inputs.SampleRemoteDocLevelMonitorInput;
import org.opensearch.alerting.monitor.inputs.SampleRemoteMonitorInput1;
import org.opensearch.alerting.monitor.inputs.SampleRemoteMonitorInput2;
import org.opensearch.alerting.monitor.triggers.SampleRemoteMonitorTrigger1;
import org.opensearch.client.node.NodeClient;
import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.common.xcontent.json.JsonXContent;
import org.opensearch.commons.alerting.action.AlertingActions;
import org.opensearch.commons.alerting.action.IndexMonitorRequest;
import org.opensearch.commons.alerting.action.IndexMonitorResponse;
import org.opensearch.commons.alerting.model.DataSources;
import org.opensearch.commons.alerting.model.DocLevelMonitorInput;
import org.opensearch.commons.alerting.model.DocLevelQuery;
import org.opensearch.commons.alerting.model.IntervalSchedule;
import org.opensearch.commons.alerting.model.Monitor;
import org.opensearch.commons.alerting.model.action.Action;
import org.opensearch.commons.alerting.model.action.Throttle;
import org.opensearch.commons.alerting.model.remote.monitors.RemoteDocLevelMonitorInput;
import org.opensearch.commons.alerting.model.remote.monitors.RemoteMonitorInput;
import org.opensearch.commons.alerting.model.remote.monitors.RemoteMonitorTrigger;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.index.seqno.SequenceNumbers;
import org.opensearch.rest.BaseRestHandler;
import org.opensearch.rest.BytesRestResponse;
import org.opensearch.rest.RestRequest;
import org.opensearch.rest.RestResponse;
import org.opensearch.script.Script;
import org.opensearch.script.ScriptType;

import java.io.IOException;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static java.util.Collections.emptyList;

public class SampleRemoteMonitorRestHandler extends BaseRestHandler {

    @Override
    public String getName() {
        return "sample-remote-monitor-rest-handler";
    }

    @Override
    public List<Route> routes() {
        return Collections.unmodifiableList(
                Arrays.asList(new Route(RestRequest.Method.POST, "_plugins/_sample_remote_monitor/monitor"))
        );
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest restRequest, NodeClient client) throws IOException {
        String runMonitorParam = restRequest.param("run_monitor");

        SampleRemoteMonitorInput1 input1 = new SampleRemoteMonitorInput1("hello", Map.of("test", 1.0f), 1);
        BytesStreamOutput out = new BytesStreamOutput();
        input1.writeTo(out);
        BytesReference input1Serialized = out.bytes();

        SampleRemoteMonitorTrigger1 trigger1 = new SampleRemoteMonitorTrigger1("hello", Map.of("test", 1.0f), 1);
        BytesStreamOutput outTrigger = new BytesStreamOutput();
        trigger1.writeTo(outTrigger);
        BytesReference trigger1Serialized = outTrigger.bytes();

        Monitor monitor1 = new Monitor(
                Monitor.NO_ID,
                Monitor.NO_VERSION,
                "sample_remote_monitor",
                true,
                new IntervalSchedule(5, ChronoUnit.MINUTES, null),
                Instant.now(),
                Instant.now(),
                SampleRemoteMonitorPlugin.SAMPLE_REMOTE_MONITOR1,
                null,
                0,
                List.of(new RemoteMonitorInput(input1Serialized)),
                List.of(new RemoteMonitorTrigger("id", "name", "1",
                        List.of(new Action("name", "destinationId", new Script(ScriptType.INLINE, Script.DEFAULT_TEMPLATE_LANG, "Hello World", Map.of()),
                                new Script(ScriptType.INLINE, Script.DEFAULT_TEMPLATE_LANG, "Hello World", Map.of()), false, new Throttle(60, ChronoUnit.MINUTES),
                                "id", null)), trigger1Serialized)),
                Map.of(),
                new DataSources(),
                "sample-remote-monitor-plugin"
        );
        IndexMonitorRequest indexMonitorRequest1 = new IndexMonitorRequest(
                Monitor.NO_ID,
                SequenceNumbers.UNASSIGNED_SEQ_NO,
                SequenceNumbers.UNASSIGNED_PRIMARY_TERM,
                WriteRequest.RefreshPolicy.IMMEDIATE,
                RestRequest.Method.POST,
                monitor1,
                null
        );

        if (runMonitorParam.equals("single")) {
            return restChannel -> {
                client.doExecute(
                        AlertingActions.INDEX_MONITOR_ACTION_TYPE,
                        indexMonitorRequest1,
                        new ActionListener<>() {
                            @Override
                            public void onResponse(IndexMonitorResponse indexMonitorResponse) {
                                try {
                                    RestResponse restResponse = new BytesRestResponse(
                                            RestStatus.OK,
                                            indexMonitorResponse.toXContent(JsonXContent.contentBuilder(), ToXContent.EMPTY_PARAMS)
                                    );
                                    restChannel.sendResponse(restResponse);
                                } catch (IOException e) {
                                    restChannel.sendResponse(new BytesRestResponse(RestStatus.INTERNAL_SERVER_ERROR, e.getMessage()));
                                }
                            }

                            @Override
                            public void onFailure(Exception e) {
                                restChannel.sendResponse(new BytesRestResponse(RestStatus.INTERNAL_SERVER_ERROR, e.getMessage()));
                            }
                        }
                );
            };
        } else if (runMonitorParam.equals("multiple")) {
            SampleRemoteMonitorInput2 input2 = new SampleRemoteMonitorInput2("hello",
                    new DocLevelMonitorInput("test", List.of("test"), List.of(new DocLevelQuery("query", "query", List.of(), "test:1", List.of()))));
            BytesStreamOutput out1 = new BytesStreamOutput();
            input2.writeTo(out1);
            BytesReference input1Serialized1 = out1.bytes();

            Monitor monitor2 = new Monitor(
                    Monitor.NO_ID,
                    Monitor.NO_VERSION,
                    "sample_remote_monitor",
                    true,
                    new IntervalSchedule(5, ChronoUnit.MINUTES, null),
                    Instant.now(),
                    Instant.now(),
                    SampleRemoteMonitorPlugin.SAMPLE_REMOTE_MONITOR2,
                    null,
                    0,
                    List.of(new RemoteMonitorInput(input1Serialized1)),
                    List.of(),
                    Map.of(),
                    new DataSources(),
                    "sample-remote-monitor-plugin"
            );
            IndexMonitorRequest indexMonitorRequest2 = new IndexMonitorRequest(
                    Monitor.NO_ID,
                    SequenceNumbers.UNASSIGNED_SEQ_NO,
                    SequenceNumbers.UNASSIGNED_PRIMARY_TERM,
                    WriteRequest.RefreshPolicy.IMMEDIATE,
                    RestRequest.Method.POST,
                    monitor2,
                    null
            );

            return restChannel -> {
                client.doExecute(
                        AlertingActions.INDEX_MONITOR_ACTION_TYPE,
                        indexMonitorRequest1,
                        new ActionListener<>() {
                            @Override
                            public void onResponse(IndexMonitorResponse indexMonitorResponse) {
                                String firstMonitorId = indexMonitorResponse.getId();
                                client.doExecute(
                                        AlertingActions.INDEX_MONITOR_ACTION_TYPE,
                                        indexMonitorRequest2,
                                        new ActionListener<>() {
                                            @Override
                                            public void onResponse(IndexMonitorResponse indexMonitorResponse) {
                                                try {
                                                    indexMonitorResponse.setId(indexMonitorResponse.getId() + " " + firstMonitorId);
                                                    RestResponse restResponse = new BytesRestResponse(
                                                            RestStatus.OK,
                                                            indexMonitorResponse.toXContent(JsonXContent.contentBuilder(), ToXContent.EMPTY_PARAMS)
                                                    );
                                                    restChannel.sendResponse(restResponse);
                                                } catch (IOException e) {
                                                    restChannel.sendResponse(new BytesRestResponse(RestStatus.INTERNAL_SERVER_ERROR, e.getMessage()));
                                                }
                                            }

                                            @Override
                                            public void onFailure(Exception e) {
                                                restChannel.sendResponse(new BytesRestResponse(RestStatus.INTERNAL_SERVER_ERROR, e.getMessage()));
                                            }
                                        }
                                );
                            }

                            @Override
                            public void onFailure(Exception e) {
                                restChannel.sendResponse(new BytesRestResponse(RestStatus.INTERNAL_SERVER_ERROR, e.getMessage()));
                            }
                        }
                );
            };
        } else {
            SampleRemoteDocLevelMonitorInput sampleRemoteDocLevelMonitorInput =
                    new SampleRemoteDocLevelMonitorInput("hello", Map.of("world", 1), 2);
            BytesStreamOutput out2 = new BytesStreamOutput();
            sampleRemoteDocLevelMonitorInput.writeTo(out2);
            BytesReference sampleRemoteDocLevelMonitorInputSerialized = out2.bytes();

            DocLevelMonitorInput docLevelMonitorInput = new DocLevelMonitorInput("description", List.of("index"), emptyList());
            RemoteDocLevelMonitorInput remoteDocLevelMonitorInput = new RemoteDocLevelMonitorInput(sampleRemoteDocLevelMonitorInputSerialized, docLevelMonitorInput);

            Monitor remoteDocLevelMonitor = new Monitor(
                    Monitor.NO_ID,
                    Monitor.NO_VERSION,
                    SampleRemoteMonitorPlugin.SAMPLE_REMOTE_DOC_LEVEL_MONITOR,
                    true,
                    new IntervalSchedule(5, ChronoUnit.MINUTES, null),
                    Instant.now(),
                    Instant.now(),
                    SampleRemoteMonitorPlugin.SAMPLE_REMOTE_DOC_LEVEL_MONITOR,
                    null,
                    0,
                    List.of(remoteDocLevelMonitorInput),
                    List.of(),
                    Map.of(),
                    new DataSources(),
                    "sample-remote-monitor-plugin"
            );
            IndexMonitorRequest indexDocLevelMonitorRequest = new IndexMonitorRequest(
                    Monitor.NO_ID,
                    SequenceNumbers.UNASSIGNED_SEQ_NO,
                    SequenceNumbers.UNASSIGNED_PRIMARY_TERM,
                    WriteRequest.RefreshPolicy.IMMEDIATE,
                    RestRequest.Method.POST,
                    remoteDocLevelMonitor,
                    null
            );
            return restChannel -> {
                client.doExecute(
                        AlertingActions.INDEX_MONITOR_ACTION_TYPE,
                        indexDocLevelMonitorRequest,
                        new ActionListener<>() {
                            @Override
                            public void onResponse(IndexMonitorResponse indexMonitorResponse) {
                                try {
                                    RestResponse restResponse = new BytesRestResponse(
                                            RestStatus.OK,
                                            indexMonitorResponse.toXContent(JsonXContent.contentBuilder(), ToXContent.EMPTY_PARAMS)
                                    );
                                    restChannel.sendResponse(restResponse);
                                } catch (IOException e) {
                                    restChannel.sendResponse(new BytesRestResponse(RestStatus.INTERNAL_SERVER_ERROR, e.getMessage()));
                                }
                            }

                            @Override
                            public void onFailure(Exception e) {
                                restChannel.sendResponse(new BytesRestResponse(RestStatus.INTERNAL_SERVER_ERROR, e.getMessage()));
                            }
                        }
                );
            };
        }
    }
}