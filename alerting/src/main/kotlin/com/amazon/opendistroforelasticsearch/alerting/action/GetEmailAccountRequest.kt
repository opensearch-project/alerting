/*
 *   Copyright 2020 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 *   Licensed under the Apache License, Version 2.0 (the "License").
 *   You may not use this file except in compliance with the License.
 *   A copy of the License is located at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *   or in the "license" file accompanying this file. This file is distributed
 *   on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 *   express or implied. See the License for the specific language governing
 *   permissions and limitations under the License.
 */

package com.amazon.opendistroforelasticsearch.alerting.action

import org.opensearch.action.ActionRequest
import org.opensearch.action.ActionRequestValidationException
import org.opensearch.common.io.stream.StreamInput
import org.opensearch.common.io.stream.StreamOutput
import org.opensearch.rest.RestRequest
import org.opensearch.search.fetch.subphase.FetchSourceContext
import java.io.IOException

class GetEmailAccountRequest : ActionRequest {
    val emailAccountID: String
    val version: Long
    val method: RestRequest.Method
    val srcContext: FetchSourceContext?

    constructor(
        emailAccountID: String,
        version: Long,
        method: RestRequest.Method,
        srcContext: FetchSourceContext?
    ) : super() {
        this.emailAccountID = emailAccountID
        this.version = version
        this.method = method
        this.srcContext = srcContext
    }

    @Throws(IOException::class)
    constructor(sin: StreamInput) : this(
        sin.readString(), // emailAccountID
        sin.readLong(), // version
        sin.readEnum(RestRequest.Method::class.java), // method
        if (sin.readBoolean()) {
            FetchSourceContext(sin) // srcContext
        } else null
    )

    override fun validate(): ActionRequestValidationException? {
        return null
    }

    @Throws(IOException::class)
    override fun writeTo(out: StreamOutput) {
        out.writeString(emailAccountID)
        out.writeLong(version)
        out.writeEnum(method)
        if (srcContext != null) {
            out.writeBoolean(true)
            srcContext.writeTo(out)
        } else {
            out.writeBoolean(false)
        }
    }
}
