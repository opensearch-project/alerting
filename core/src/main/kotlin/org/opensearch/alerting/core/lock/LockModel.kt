/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.alerting.core.lock

import org.opensearch.common.xcontent.XContentParserUtils
import org.opensearch.core.xcontent.ToXContent
import org.opensearch.core.xcontent.ToXContentObject
import org.opensearch.core.xcontent.XContentBuilder
import org.opensearch.core.xcontent.XContentParser
import org.opensearch.index.seqno.SequenceNumbers
import java.io.IOException
import java.time.Instant

class LockModel(
    val lockId: String,
    val scheduledJobId: String,
    val lockTime: Instant,
    val released: Boolean,
    val seqNo: Long,
    val primaryTerm: Long
) : ToXContentObject {

    constructor(
        copyLock: LockModel,
        seqNo: Long,
        primaryTerm: Long
    ) : this (
        copyLock.lockId,
        copyLock.scheduledJobId,
        copyLock.lockTime,
        copyLock.released,
        seqNo,
        primaryTerm
    )

    constructor(
        copyLock: LockModel,
        released: Boolean
    ) : this (
        copyLock.lockId,
        copyLock.scheduledJobId,
        copyLock.lockTime,
        released,
        copyLock.seqNo,
        copyLock.primaryTerm
    )

    constructor(
        copyLock: LockModel,
        updateLockTime: Instant,
        released: Boolean
    ) : this (
        copyLock.lockId,
        copyLock.scheduledJobId,
        updateLockTime,
        released,
        copyLock.seqNo,
        copyLock.primaryTerm
    )

    constructor(
        scheduledJobId: String,
        lockTime: Instant,
        released: Boolean
    ) : this (
        generateLockId(scheduledJobId),
        scheduledJobId,
        lockTime,
        released,
        SequenceNumbers.UNASSIGNED_SEQ_NO,
        SequenceNumbers.UNASSIGNED_PRIMARY_TERM
    )

    override fun toXContent(builder: XContentBuilder, params: ToXContent.Params): XContentBuilder {
        builder.startObject()
            .field(SCHEDULED_JOB_ID, scheduledJobId)
            .field(LOCK_TIME, lockTime.epochSecond)
            .field(RELEASED, released)
            .endObject()
        return builder
    }

    companion object {
        const val SCHEDULED_JOB_ID = "scheduled_job_id"
        const val LOCK_TIME = "lock_time"
        const val RELEASED = "released"

        fun generateLockId(scheduledJobId: String): String {
            return "$scheduledJobId-lock"
        }

        @JvmStatic
        @JvmOverloads
        @Throws(IOException::class)
        fun parse(xcp: XContentParser, seqNo: Long, primaryTerm: Long): LockModel {
            lateinit var scheduledJobId: String
            lateinit var lockTime: Instant
            var released: Boolean = false

            XContentParserUtils.ensureExpectedToken(XContentParser.Token.START_OBJECT, xcp.currentToken(), xcp)
            while (xcp.nextToken() != XContentParser.Token.END_OBJECT) {
                val fieldName = xcp.currentName()
                xcp.nextToken()

                when (fieldName) {
                    SCHEDULED_JOB_ID -> scheduledJobId = xcp.text()
                    LOCK_TIME -> lockTime = Instant.ofEpochSecond(xcp.longValue())
                    RELEASED -> released = xcp.booleanValue()
                }
            }
            return LockModel(generateLockId(scheduledJobId), scheduledJobId, lockTime, released, seqNo, primaryTerm)
        }
    }
}
