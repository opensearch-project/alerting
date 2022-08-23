package org.opensearch.alerting.model;

import org.opensearch.alerting.core.model.IntervalSchedule;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.Map;

public class Model2ModelTranslator {
    /*
     override val id: String = NO_ID,
    override val version: Long = NO_VERSION,
    override val name: String,
    override val enabled: Boolean,
    override val schedule: Schedule,
    override val lastUpdateTime: Instant,
    override val enabledTime: Instant?,
    // TODO: Check how this behaves during rolling upgrade/multi-version cluster
    //  Can read/write and parsing break if it's done from an old -> new version of the plugin?
    val monitorType: MonitorType,
    val user: User?,
    val schemaVersion: Int = NO_SCHEMA_VERSION,
    val inputs: List<Input>,
    val triggers: List<Trigger>,
    val uiMetadata: Map<String, Any>
     */
    public static Monitor fromModel2(final org.opensearch.commons.model2.model.Monitor monitor2) {
        return new Monitor(
                monitor2.id, // id
                monitor2.version, // version
                monitor2.name, // name
                true, // enabled
                new IntervalSchedule(100, ChronoUnit.SECONDS, Instant.now()), // schedule
                Instant.now(), // last update
                Instant.now(), // enabled time
                Monitor.MonitorType.valueOf(monitor2.monitor_type), // monitor type
                null, // user
                (int) monitor2.version, // schema version
                List.of(), // inputs
                List.of(), // triggers
                Map.of()); // ui metadta
    }

    public static org.opensearch.commons.model2.model.Monitor toModel2(final Monitor monitor) {
        return new org.opensearch.commons.model2.model.Monitor(
                monitor.getId(),
                monitor.getMonitorType().getValue(),
                monitor.getVersion(),
                monitor.getName(),
                monitor.getLastUpdateTime().toEpochMilli(),
                "milliseconds",
                List.of()); // inputs
    }
}
