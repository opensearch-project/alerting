/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.alerting.action

import org.opensearch.action.ActionRequest
import org.opensearch.action.ActionRequestValidationException
import org.opensearch.core.common.io.stream.StreamInput
import org.opensearch.core.common.io.stream.StreamOutput
import java.io.IOException

/**
 * No payload — the endpoint is a fire-and-forget administrative trigger.
 */
class MigrateToRscRequest : ActionRequest {
    constructor() : super()

    @Throws(IOException::class)
    @Suppress("UNUSED_PARAMETER")
    constructor(sin: StreamInput) : super()

    override fun validate(): ActionRequestValidationException? = null

    override fun writeTo(out: StreamOutput) {
        // no fields
    }
}
