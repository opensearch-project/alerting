/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.alerting.core.util

import org.opensearch.core.xcontent.XContentBuilder
import java.time.Instant

fun XContentBuilder.nonOptionalTimeField(
    name: String,
    instant: Instant,
): XContentBuilder = this.timeField(name, "${name}_in_millis", instant.toEpochMilli())
