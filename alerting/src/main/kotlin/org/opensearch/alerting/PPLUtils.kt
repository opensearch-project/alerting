/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.alerting

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.node.ArrayNode
import org.json.JSONObject
import org.opensearch.alerting.core.ppl.PPLPluginInterface
import org.opensearch.alerting.opensearchapi.suspendUntil
import org.opensearch.commons.utils.recreateObject
import org.opensearch.core.action.ActionListener
import org.opensearch.core.action.ActionResponse
import org.opensearch.sql.plugin.transport.PPLQueryAction
import org.opensearch.sql.plugin.transport.TransportPPLQueryRequest
import org.opensearch.sql.plugin.transport.TransportPPLQueryResponse
import org.opensearch.transport.client.node.NodeClient

object PPLUtils {

    private val customConditionValidationRegex = """^\s*where\s+.+""".toRegex()
    private val mapper = ObjectMapper()

    const val PPL_RESULTS_SIZE_EXCEEDED_MESSAGE = "The PPL Query results were too large and thus excluded."

    /**
     * Appends a user-defined custom condition to a PPL query.
     *
     * This method is used exclusively for custom condition triggers. It concatenates
     * the custom condition to the base PPL query using the pipe operator (|), allowing the condition
     * to evaluate each query result data row.
     *
     * @param query The base PPL query string (e.g., "source=logs | where status=error")
     * @param customCondition The custom trigger condition to append (e.g., "eval result = avg > 3")
     * @return The combined PPL query with the custom condition appended
     *
     * @example
     * ```
     * val baseQuery = "source=logs | stats max(price) as max_price by region"
     * val condition = "eval result = max_price > 300"
     * val result = appendCustomCondition(baseQuery, condition)
     * // Returns: "source=logs | stats max(price) as max_price by region | eval result = max_price > 300"
     * ```
     *
     * @note This method does not validate the syntax of either the query or custom condition.
     *       It is assumed that upstream workflows have already validated the base query,
     *       and that downstream workflows will validate the constructed query
     */
    fun appendCustomCondition(query: String, customCondition: String): String {
        return "$query | $customCondition"
    }

    /**
     * Appends a limit on the number of documents/data rows to retrieve from a PPL query.
     *
     * This method uses the PPL `head` command to restrict the number of rows returned by
     * the query. This is used to prevent memory issues and improving performance when
     * only a subset of results is needed for alert evaluation.
     *
     * @param query The base PPL query string
     * @param maxDataRows The maximum number of data rows to retrieve
     * @return The PPL query with a head limit appended (e.g., "source=logs | head 1000")
     *
     * @example
     * ```
     * val query = "source=logs | where status=error"
     * val limitedQuery = appendDataRowsLimit(query, 100)
     * // Returns: "source=logs | where status=error | head 100"
     * ```
     */
    fun appendDataRowsLimit(query: String, maxDataRows: Long): String {
        return "$query | head $maxDataRows"
    }

    fun customConditionIsValid(customCondition: String): Boolean {
        return customCondition.matches(customConditionValidationRegex)
    }

    /**
     * Executes a PPL query, for suspend functions and contexts
     *
     * This method calls the PPL Plugin's Execute or Explain API via the transport layer to execute the provided query
     * and parses the response into a structured JSON format suitable for trigger evaluation
     *
     * @param query The PPL query string to execute
     * @param explain true if the query should just be explained, false if the query should be executed
     * @param client The node client used to run the request
     * @return A JsonNode containing the query execution results
     *
     * @throws Exception if the query execution fails or the response cannot be parsed as JSON
     *
     * @note The response format follows the PPL plugin's Execute API response structure with
     *       "schema", "datarows", "total", and "size" fields.
     */
    suspend fun executePplQuery(
        query: String,
        explain: Boolean,
        client: NodeClient
    ): JsonNode {
        val path = if (explain) {
            "/_plugins/_ppl/_explain"
        } else {
            "/_plugins/_ppl"
        }

        // prepare request to SQL/PPL Plugin
        val transportPplQueryRequest = TransportPPLQueryRequest(
            query,
            JSONObject(mapOf("query" to query)),
            path
        )

        val transportPplQueryResponse = PPLPluginInterface.suspendUntil {
            this.executeQuery(
                client,
                transportPplQueryRequest,
                it
            )
        }

        return mapper.readTree(transportPplQueryResponse.result)
    }

    /**
     * Executes a PPL query, for callback style flows with an action listener
     *
     * This method calls the PPL Plugin's Execute or Explain API via the transport layer to execute the provided query
     * and parses the response into a structured JSON format suitable for trigger evaluation
     *
     * @param query The PPL query string to execute
     * @param explain true if the query should just be explained, false if the query should be executed
     * @param client The node client used to run the request
     * @param listener The callback listener for when PPL/SQL plugin sends its response or failure
     *
     *
     * @note The response format follows the PPL plugin's Execute API response structure with
     *       "schema", "datarows", "total", and "size" fields.
     */
    fun executePplQuery(
        query: String,
        explain: Boolean,
        client: NodeClient,
        listener: ActionListener<TransportPPLQueryResponse>
    ) {
        val path = if (explain) {
            "/_plugins/_ppl/_explain"
        } else {
            "/_plugins/_ppl"
        }

        // prepare request to SQL/PPL Plugin
        val request = TransportPPLQueryRequest(
            query,
            JSONObject(mapOf("query" to query)),
            path
        )

        val wrappedListener = object : ActionListener<ActionResponse> {
            override fun onResponse(response: ActionResponse) {
                val recreated = recreateObject(response) { TransportPPLQueryResponse(it) }
                listener.onResponse(recreated)
            }

            override fun onFailure(exception: Exception) {
                listener.onFailure(exception)
            }
        } as ActionListener<TransportPPLQueryResponse>

        client.execute(PPLQueryAction.INSTANCE, request, wrappedListener)
    }

    fun capAndReformatPPLQueryResults(rawQueryResults: JsonNode, maxSize: Long): List<Map<String, Any?>> {
        val cappedQueryResults = capPPLQueryResultsSize(rawQueryResults, maxSize)
        val cappedMap = mapper.convertValue(cappedQueryResults, Map::class.java) as Map<String, Any>
        return constructPPLQueryResultsMap(cappedMap)
    }

    /**
     * Caps the size of PPL query results to prevent memory issues and oversized alert payloads.
     *
     * Checks if the serialized query results exceed a specified size limit. If the results
     * are within the limit, they are returned unchanged. If they exceed the limit, the whole response
     * is replaced with an informational message while preserving the original structure of the response.
     * This ensures alerts can still be created even when query results are too large.
     *
     * @param pplQueryResults The PPL query response JsonNode
     * @param maxSize The maximum allowed size in bytes (estimated by serialized string length)
     * @return The original results if under the limit, or a modified version with datarows replaced by a message
     *
     * @example
     * ```
     * val queryResults = executePplQuery(query, client)
     * val cappedResults = capPPLQueryResultsSize(queryResults, maxSize = 5000L)
     *
     * // If results were too large, datarows will contain:
     * // [["The PPL Query results were too large and thus excluded"]]
     * // But schema, total, and size fields are preserved
     * ```
     *
     * @note Size is estimated using `toString().length`, which approximates byte size but may
     *       not be exact for multi-byte characters
     * @note The PPL query results structure includes:
     *       - `schema`: Array of objects storing data types for each column
     *       - `datarows`: Array of arrays containing the actual query result rows
     *       - `total`: Total number of result rows
     *       - `size`: Same as `total` (redundant field in PPL response)
     */
    fun capPPLQueryResultsSize(pplQueryResults: JsonNode, maxSize: Long): JsonNode {
        val pplQueryResultsSize = pplQueryResults.toString().length
        if (pplQueryResultsSize <= maxSize) {
            return pplQueryResults
        }

        val result = mapper.createObjectNode()
        val schema = mapper.createArrayNode().add(
            mapper.createObjectNode().put("name", "message").put("type", "string")
        )
        val datarows = mapper.createArrayNode().add(
            mapper.createArrayNode().add(PPL_RESULTS_SIZE_EXCEEDED_MESSAGE)
        )
        result.set<ArrayNode>("schema", schema)
        result.set<ArrayNode>("datarows", datarows)
        result.put("total", 1)
        result.put("size", 1)

        return result
    }

    /**
     * Transforms PPL query results from array-based format (that SQL Plugin Execute API response uses)
     * to map-based format for easier template access.
     *
     * PPL query responses contain a `schema` array that defines field names and types, and a `datarows` array
     * that contains the actual data values in positional format. This function combines them into a list of maps
     * where each list element represents a row with field names as keys and corresponding values from the datarows.
     *
     * ### Input Format
     * The input should be a PPL query result with this structure:
     * ```json
     * {
     *   "schema": [
     *     {"name": "abc", "type": "string"},
     *     {"name": "number", "type": "integer"}
     *   ],
     *   "datarows": [
     *     ["xyz", 3],
     *     ["def", 5]
     *   ]
     * }
     * ```
     *
     * ### Output Format
     * The function returns a list where each element is a map representing a data row:
     * ```json
     * [
     *   {"abc": "xyz", "number": 3},
     *   {"abc": "def", "number": 5}
     * ]
     * ```
     *
     * ### Edge Cases
     * - If `schema` is missing or empty, returns an empty list
     * - If `datarows` is missing or empty, returns an empty list
     * - If a schema entry is malformed (not a map or missing "name" field), it is skipped
     * - If a datarow has fewer values than schema fields, missing values are set to `null`
     * - If a datarow has more values than schema fields, extra values are ignored
     * - If a datarow is not a list, that row is skipped
     *
     * @param rawQueryResults The PPL query results map from SQL Plugin Execute API response
     *                        containing "schema" and "datarows" fields.
     * @return A list of maps where each map represents a data row with field names as keys and
     *         corresponding values from datarows. Returns an empty list if schema or datarows
     *         are missing, empty, or malformed.
     *
     * @see org.opensearch.alerting.script.QueryLevelTriggerExecutionContext.asTemplateArg
     * @see org.opensearch.alerting.PPLUtils.executePplQuery
     */
    fun constructPPLQueryResultsMap(rawQueryResults: Map<String, Any>): List<Map<String, Any?>> {
        // Extract schema array
        val schema = rawQueryResults["schema"] as? List<*> ?: return emptyList()

        // Extract field names from schema
        val fieldNames = schema.mapNotNull { schemaEntry ->
            (schemaEntry as? Map<*, *>)?.get("name") as? String
        }

        if (fieldNames.isEmpty()) return emptyList()

        // Extract datarows array
        val datarows = rawQueryResults["datarows"] as? List<*> ?: return emptyList()

        // Transform each row into a map
        return datarows.mapNotNull { row ->
            val rowList = row as? List<*> ?: return@mapNotNull null

            // Create a map from field names to values
            fieldNames.mapIndexed { index, fieldName ->
                fieldName to rowList.getOrNull(index)
            }.toMap()
        }
    }
}
