/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.alerting.core.model

import org.apache.commons.validator.routines.UrlValidator
import org.apache.http.client.utils.URIBuilder
import org.opensearch.common.CheckedFunction
import org.opensearch.common.ParseField
import org.opensearch.common.io.stream.StreamInput
import org.opensearch.common.io.stream.StreamOutput
import org.opensearch.common.xcontent.NamedXContentRegistry
import org.opensearch.common.xcontent.ToXContent
import org.opensearch.common.xcontent.XContentBuilder
import org.opensearch.common.xcontent.XContentParser
import org.opensearch.common.xcontent.XContentParserUtils
import java.io.IOException
import java.net.URI

val ILLEGAL_PATH_PARAMETER_CHARACTERS = arrayOf(':', '"', '+', '\\', '|', '?', '#', '>', '<', ' ')

/**
 * This is a data class for a URI type of input for Monitors specifically for local clusters.
 */
data class ClusterMetricsInput(
    var path: String,
    var pathParams: String = "",
    var url: String
) : Input {
    val clusterMetricType: ClusterMetricType
    val constructedUri: URI

    // Verify parameters are valid during creation
    init {
        require(validateFields()) {
            "The uri.api_type field, uri.path field, or uri.uri field must be defined."
        }

        // Create an UrlValidator that only accepts "http" and "https" as valid scheme and allows local URLs.
        val urlValidator = UrlValidator(arrayOf("http", "https"), UrlValidator.ALLOW_LOCAL_URLS)

        // Build url field by field if not provided as whole.
        constructedUri = toConstructedUri()

        require(urlValidator.isValid(constructedUri.toString())) {
            "Invalid URI constructed from the path and path_params inputs, or the url input."
        }

        if (url.isNotEmpty() && validateFieldsNotEmpty())
            require(constructedUri == constructUrlFromInputs()) {
                "The provided URL and URI fields form different URLs."
            }

        require(constructedUri.host.lowercase() == SUPPORTED_HOST) {
            "Only host '$SUPPORTED_HOST' is supported."
        }
        require(constructedUri.port == SUPPORTED_PORT) {
            "Only port '$SUPPORTED_PORT' is supported."
        }

        clusterMetricType = findApiType(constructedUri.path)
        this.parseEmptyFields()
    }

    @Throws(IOException::class)
    constructor(sin: StreamInput) : this(
        sin.readString(), // path
        sin.readString(), // path params
        sin.readString() // url
    )

    override fun toXContent(builder: XContentBuilder, params: ToXContent.Params): XContentBuilder {
        return builder.startObject()
            .startObject(URI_FIELD)
            .field(API_TYPE_FIELD, clusterMetricType)
            .field(PATH_FIELD, path)
            .field(PATH_PARAMS_FIELD, pathParams)
            .field(URL_FIELD, url)
            .endObject()
            .endObject()
    }

    override fun name(): String {
        return URI_FIELD
    }

    override fun writeTo(out: StreamOutput) {
        out.writeString(clusterMetricType.toString())
        out.writeString(path)
        out.writeString(pathParams)
        out.writeString(url)
    }

    companion object {
        const val SUPPORTED_SCHEME = "http"
        const val SUPPORTED_HOST = "localhost"
        const val SUPPORTED_PORT = 9200

        const val API_TYPE_FIELD = "api_type"
        const val PATH_FIELD = "path"
        const val PATH_PARAMS_FIELD = "path_params"
        const val URL_FIELD = "url"
        const val URI_FIELD = "uri"

        val XCONTENT_REGISTRY = NamedXContentRegistry.Entry(Input::class.java, ParseField(URI_FIELD), CheckedFunction { parseInner(it) })

        /**
         * This parse function uses [XContentParser] to parse JSON input and store corresponding fields to create a [ClusterMetricsInput] object
         */
        @JvmStatic @Throws(IOException::class)
        fun parseInner(xcp: XContentParser): ClusterMetricsInput {
            var path = ""
            var pathParams = ""
            var url = ""

            XContentParserUtils.ensureExpectedToken(XContentParser.Token.START_OBJECT, xcp.currentToken(), xcp)

            while (xcp.nextToken() != XContentParser.Token.END_OBJECT) {
                val fieldName = xcp.currentName()
                xcp.nextToken()
                when (fieldName) {
                    PATH_FIELD -> path = xcp.text()
                    PATH_PARAMS_FIELD -> pathParams = xcp.text()
                    URL_FIELD -> url = xcp.text()
                }
            }
            return ClusterMetricsInput(path, pathParams, url)
        }
    }

    /**
     * Constructs the [URI] using either the provided [url], or the
     * supported scheme, host, and port and provided [path]+[pathParams].
     * @return The [URI] constructed from [url] if it's defined;
     * otherwise a [URI] constructed from the provided [URI] fields.
     */
    private fun toConstructedUri(): URI {
        return if (url.isEmpty()) {
            constructUrlFromInputs()
        } else {
            URIBuilder(url).build()
        }
    }

    /**
     * Isolates just the path parameters from the [ClusterMetricsInput] URI.
     * @return The path parameters portion of the [ClusterMetricsInput] URI.
     * @throws IllegalArgumentException if the [ClusterMetricType] requires path parameters, but none are supplied;
     * or when path parameters are provided for an [ClusterMetricType] that does not use path parameters.
     */
    fun parsePathParams(): String {
        val path = this.constructedUri.path
        val apiType = this.clusterMetricType

        var pathParams: String
        if (this.pathParams.isNotEmpty()) {
            pathParams = this.pathParams
        } else {
            val prependPath = if (apiType.supportsPathParams) apiType.prependPath else apiType.defaultPath
            pathParams = path.removePrefix(prependPath)
            pathParams = pathParams.removeSuffix(apiType.appendPath)
        }

        if (pathParams.isNotEmpty()) {
            pathParams = pathParams.trim('/')
            ILLEGAL_PATH_PARAMETER_CHARACTERS.forEach { character ->
                if (pathParams.contains(character))
                    throw IllegalArgumentException(
                        "The provided path parameters contain invalid characters or spaces. Please omit: " +
                            "${ILLEGAL_PATH_PARAMETER_CHARACTERS.joinToString(" ")}"
                    )
            }
        }

        if (apiType.requiresPathParams && pathParams.isEmpty())
            throw IllegalArgumentException("The API requires path parameters.")
        if (!apiType.supportsPathParams && pathParams.isNotEmpty())
            throw IllegalArgumentException("The API does not use path parameters.")

        return pathParams
    }

    /**
     * Examines the path of a [ClusterMetricsInput] to determine which API is being called.
     * @param uriPath The path to examine.
     * @return The [ClusterMetricType] associated with the [ClusterMetricsInput] monitor.
     * @throws IllegalArgumentException when the API to call cannot be determined from the URI.
     */
    private fun findApiType(uriPath: String): ClusterMetricType {
        var apiType = ClusterMetricType.BLANK
        ClusterMetricType.values()
            .filter { option -> option != ClusterMetricType.BLANK }
            .forEach { option ->
                if (uriPath.startsWith(option.prependPath) || uriPath.startsWith(option.defaultPath))
                    apiType = option
            }
        if (apiType.isBlank())
            throw IllegalArgumentException("The API could not be determined from the provided URI.")
        return apiType
    }

    /**
     * Constructs a [URI] from the supported scheme, host, and port, and the provided [path], and [pathParams].
     * @return The constructed [URI].
     */
    private fun constructUrlFromInputs(): URI {
        val uriBuilder = URIBuilder()
            .setScheme(SUPPORTED_SCHEME)
            .setHost(SUPPORTED_HOST)
            .setPort(SUPPORTED_PORT)
            .setPath(path + pathParams)
        return uriBuilder.build()
    }

    /**
     * If [url] field is empty, populates it with [constructedUri].
     * If [path] and [pathParams] are empty, populates them with values from [url].
     */
    private fun parseEmptyFields() {
        if (pathParams.isEmpty())
            pathParams = this.parsePathParams()
        if (path.isEmpty())
            path = if (pathParams.isEmpty()) clusterMetricType.defaultPath else clusterMetricType.prependPath
        if (url.isEmpty())
            url = constructedUri.toString()
    }

    /**
     * Helper function to confirm at least [url], or required URI component fields are defined.
     * @return TRUE if at least either [url] or the other components are provided; otherwise FALSE.
     */
    private fun validateFields(): Boolean {
        return url.isNotEmpty() || validateFieldsNotEmpty()
    }

    /**
     * Confirms that required URI component fields are defined.
     * Only validating path for now, as that's the only required field.
     * @return TRUE if all those fields are defined; otherwise FALSE.
     */
    private fun validateFieldsNotEmpty(): Boolean {
        return path.isNotEmpty()
    }

    /**
     * An enum class to quickly reference various supported API.
     */
    enum class ClusterMetricType(
        val defaultPath: String,
        val prependPath: String,
        val appendPath: String,
        val supportsPathParams: Boolean,
        val requiresPathParams: Boolean
    ) {
        BLANK("", "", "", false, false),
        CAT_PENDING_TASKS(
            "/_cat/pending_tasks",
            "/_cat/pending_tasks",
            "",
            false,
            false
        ),
        CAT_RECOVERY(
            "/_cat/recovery",
            "/_cat/recovery",
            "",
            true,
            false
        ),
        CAT_SNAPSHOTS(
            "/_cat/snapshots",
            "/_cat/snapshots",
            "",
            true,
            true
        ),
        CAT_TASKS(
            "/_cat/tasks",
            "/_cat/tasks",
            "",
            false,
            false
        ),
        CLUSTER_HEALTH(
            "/_cluster/health",
            "/_cluster/health",
            "",
            true,
            false
        ),
        CLUSTER_SETTINGS(
            "/_cluster/settings",
            "/_cluster/settings",
            "",
            false,
            false
        ),
        CLUSTER_STATS(
            "/_cluster/stats",
            "/_cluster/stats",
            "",
            true,
            false
        ),
        NODES_STATS(
            "/_nodes/stats",
            "/_nodes",
            "",
            false,
            false
        );

        /**
         * @return TRUE if the [ClusterMetricType] is [BLANK]; otherwise FALSE.
         */
        fun isBlank(): Boolean {
            return this === BLANK
        }
    }
}
