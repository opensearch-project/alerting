/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.alerting.model.destination

import org.opensearch.common.xcontent.ToXContent
import org.opensearch.common.xcontent.XContentBuilder
import org.opensearch.common.xcontent.XContentParser
import org.opensearch.common.xcontent.XContentParserUtils
import java.io.IOException
import java.lang.IllegalStateException
import java.util.regex.Pattern

data class SNS(val topicARN: String, val roleARN: String) : ToXContent {

    init {
        require(SNS_ARN_REGEX.matcher(topicARN).find()) { "Invalid AWS SNS topic ARN: $topicARN" }
        require(IAM_ARN_REGEX.matcher(roleARN).find()) { "Invalid AWS role ARN: $roleARN " }
    }

    override fun toXContent(builder: XContentBuilder, params: ToXContent.Params): XContentBuilder {
        return builder.startObject(SNS_TYPE)
            .field(TOPIC_ARN_FIELD, topicARN)
            .field(ROLE_ARN_FIELD, roleARN)
            .endObject()
    }

    companion object {

        private val SNS_ARN_REGEX = Pattern.compile("^arn:aws(-[^:]+)?:sns:([a-zA-Z0-9-]+):([0-9]{12}):([a-zA-Z0-9-_]+)$")
        private val IAM_ARN_REGEX = Pattern.compile("^arn:aws(-[^:]+)?:iam::([0-9]{12}):([a-zA-Z0-9-/_]+)$")

        const val TOPIC_ARN_FIELD = "topic_arn"
        const val ROLE_ARN_FIELD = "role_arn"
        const val SNS_TYPE = "sns"

        @JvmStatic
        @Throws(IOException::class)
        fun parse(xcp: XContentParser): SNS {
            lateinit var topicARN: String
            lateinit var roleARN: String

            XContentParserUtils.ensureExpectedToken(XContentParser.Token.START_OBJECT, xcp.currentToken(), xcp)
            while (xcp.nextToken() != XContentParser.Token.END_OBJECT) {
                val fieldName = xcp.currentName()
                xcp.nextToken()
                when (fieldName) {
                    TOPIC_ARN_FIELD -> topicARN = xcp.textOrNull()
                    ROLE_ARN_FIELD -> roleARN = xcp.textOrNull()
                    else -> {
                        throw IllegalStateException("Unexpected field: $fieldName, while parsing SNS destination")
                    }
                }
            }
            return SNS(
                requireNotNull(topicARN) { "SNS Action topic_arn is null" },
                requireNotNull(roleARN) { "SNS Action role_arn is null" }
            )
        }
    }
}
