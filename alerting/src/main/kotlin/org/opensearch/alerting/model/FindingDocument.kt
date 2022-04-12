package org.opensearch.alerting.model

import org.apache.logging.log4j.LogManager
import org.opensearch.common.io.stream.StreamInput
import org.opensearch.common.io.stream.StreamOutput
import org.opensearch.common.io.stream.Writeable
import org.opensearch.common.xcontent.ToXContent
import org.opensearch.common.xcontent.XContentBuilder
import org.opensearch.common.xcontent.XContentParser
import org.opensearch.common.xcontent.XContentParserUtils
import java.io.IOException

private val log = LogManager.getLogger(FindingDocument::class.java)

class FindingDocument(
    val index: String,
    val id: String,
    val found: Boolean,
    val document: String
) : Writeable, ToXContent {

    @Throws(IOException::class)
    constructor(sin: StreamInput) : this(
        index = sin.readString(),
        id = sin.readString(),
        found = sin.readBoolean(),
        document = sin.readString()
    )

    override fun toXContent(builder: XContentBuilder, params: ToXContent.Params): XContentBuilder {
        return builder.startObject()
            .field(INDEX_FIELD, index)
            .field(FINDING_DOCUMENT_ID_FIELD, id)
            .field(FOUND_FIELD, found)
            .field(DOCUMENT_FIELD, document)
            .endObject()
    }

    @Throws(IOException::class)
    override fun writeTo(out: StreamOutput) {
        out.writeString(index)
        out.writeString(id)
        out.writeBoolean(found)
        out.writeString(document)
    }

    companion object {
        const val INDEX_FIELD = "index"
        const val FINDING_DOCUMENT_ID_FIELD = "id"
        const val FOUND_FIELD = "found"
        const val DOCUMENT_FIELD = "document"
        const val NO_ID = ""
        const val NO_INDEX = ""

        @JvmStatic @JvmOverloads
        @Throws(IOException::class)
        fun parse(xcp: XContentParser, id: String = NO_ID, index: String = NO_INDEX): FindingDocument {
            var found = false
            var document: String = ""

            XContentParserUtils.ensureExpectedToken(XContentParser.Token.START_OBJECT, xcp.currentToken(), xcp)
            while (xcp.nextToken() != XContentParser.Token.END_OBJECT) {
                val fieldName = xcp.currentName()
                xcp.nextToken()

                when (fieldName) {
                    FOUND_FIELD -> found = xcp.booleanValue()
                    DOCUMENT_FIELD -> document = xcp.text()
                }
            }

            return FindingDocument(
                index = index,
                id = id,
                found = found,
                document = document
            )
        }

        @JvmStatic
        @Throws(IOException::class)
        fun readFrom(sin: StreamInput): FindingDocument {
            return FindingDocument(sin)
        }
    }
}
