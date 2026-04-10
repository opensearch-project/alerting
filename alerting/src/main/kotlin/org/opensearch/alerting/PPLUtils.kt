/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.alerting

import org.json.JSONArray
import org.json.JSONObject
import org.opensearch.alerting.core.ppl.PPLPluginInterface
import org.opensearch.alerting.opensearchapi.suspendUntil
import org.opensearch.cluster.node.DiscoveryNode
import org.opensearch.sql.plugin.transport.TransportPPLQueryRequest
import org.opensearch.transport.TransportService

object PPLUtils {

    // TODO: these are in-house PPL query parsers, find a PPL plugin dependency that does this for us
    /* Regular Expressions */
    // captures the name of the result variable in a PPL monitor's custom condition
    // e.g. custom condition: `eval apple = avg_latency > 100`
    // captures: "apple"
    private val evalResultVarRegex = """^(?!.*\|)\s*(?i:eval)\s+([a-zA-Z_][a-zA-Z0-9_]*)\s*=""".toRegex()

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

    /**
     * Executes a PPL query and returns the response as a parsable JSONObject.
     *
     * This method calls the PPL Plugin's Execute API via the transport layer to execute the provided query
     * and parses the response into a structured JSON format suitable for trigger evaluation
     *
     * @param query The PPL query string to execute
     * @param client The NodeClient used to communicate with the PPL plugin
     * @return A JSONObject containing the query execution results
     *
     * @throws Exception if the query execution fails or the response cannot be parsed as JSON
     *
     * @note The response format follows the PPL plugin's Execute API response structure with
     *       "schema", "datarows", "total", and "size" fields.
     */
    suspend fun executePplQuery(
        query: String,
        localNode: DiscoveryNode,
        transportService: TransportService
    ): JSONObject {
        // call PPL plugin to execute query
        val transportPplQueryRequest = TransportPPLQueryRequest(
            query,
            JSONObject(mapOf("query" to query)),
            null // null path falls back to a default path internal to SQL/PPL Plugin
        )

        val transportPplQueryResponse = PPLPluginInterface.suspendUntil {
            this.executeQuery(
                transportService,
                localNode,
                transportPplQueryRequest,
                it
            )
        }

        val queryResponseJson = JSONObject(transportPplQueryResponse.result)

        return queryResponseJson
    }

    /**
     * Searches a custom condition eval statement for the name of the eval result variable.
     *
     * Parses a PPL eval expression to extract the variable name being assigned. The eval
     * statement must follow the format: `eval <variable_name> = <expression>`. This variable
     * name is needed to reference the evaluation result in subsequent trigger condition checks.
     *
     * @param customCondition The PPL custom condition string containing an eval statement (e.g. eval result = avg > 3)
     * @return The name of the eval result variable
     * @throws IllegalArgumentException if no valid eval statement is found or the syntax is invalid
     *
     * @example
     * ```
     * val condition = "eval error_rate = errors / total"
     * val varName = findEvalResultVar(condition)
     * // Returns: "error_rate"
     * ```
     *
     * @note A precheck of the base query + custom condition is assumed to have been done already.
     *       The function thus expects the PPL keyword "eval" followed by whitespace. Without the
     *       whitespace (e.g., "evalresult"), the PPL plugin would have thrown a syntax error
     *       during upstream validations
     * @note Variable names must follow standard identifier rules: start with a letter or underscore,
     *       followed by letters, digits, or underscores (matching `[a-zA-Z_][a-zA-Z0-9_]*`).
     *
     * TODO: Replace this in-house parser with a PPL plugin dependency that provides proper
     *       query parsing functionality.
     */
    fun findEvalResultVar(customCondition: String): String {
        // TODO: these are in-house PPL query parsers, find a PPL plugin dependency that does this for us
        val evalResultVar = evalResultVarRegex.find(customCondition)?.groupValues?.get(1)
            ?: throw IllegalArgumentException("Given custom condition is invalid, could not find eval result variable")
        return evalResultVar
    }

    /**
     * Finds the index of the eval result variable in the PPL query response schema.
     *
     * Searches through the schema array in the PPL query response to locate the column
     * corresponding to the eval result variable. This index is used to extract the
     * eval result values from the datarows in the query response.
     *
     * @param customConditionQueryResponse The JSONObject containing the PPL query response
     *                                     with "schema" and "datarows" fields
     * @param evalResultVarName The name of the eval result variable to locate in the schema
     * @return The zero-based index of the eval result variable in the schema array
     * @throws IllegalStateException if the eval result variable is not found in the schema
     *
     * @note The eval result variable should always be present in the schema if the query
     *       executed successfully. If not found, this indicates an unexpected state.
     * @note The query response schema is assumed to follow PPL plugin Execute API response schema
     */
    fun findEvalResultVarIdxInSchema(customConditionQueryResponse: JSONObject, evalResultVarName: String): Int {
        // find the index eval statement result variable in the PPL query response schema
        val schemaList = customConditionQueryResponse.getJSONArray("schema")
        var evalResultVarIdx = -1
        for (i in 0 until schemaList.length()) {
            val schemaObj = schemaList.getJSONObject(i)
            val columnName = schemaObj.getString("name")

            if (columnName == evalResultVarName) {
                evalResultVarIdx = i
                break
            }
        }

        // eval statement result variable should always be found
        if (evalResultVarIdx == -1) {
            throw IllegalStateException(
                "Expected to find eval statement results variable \"$evalResultVarName\" in results " +
                    "of PPL query with custom condition, but did not."
            )
        }

        return evalResultVarIdx
    }

    fun capAndReformatPPLQueryResults(rawQueryResults: JSONObject, maxSize: Long): List<Map<String, Any?>> {
        val cappedQueryResults = capPPLQueryResultsSize(rawQueryResults, maxSize).toMap()
        val reformattedQueryResults = constructPPLQueryResultsMap(cappedQueryResults)
        return reformattedQueryResults
    }

    /**
     * Caps the size of PPL query results to prevent memory issues and oversized alert payloads.
     *
     * Checks if the serialized query results exceed a specified size limit. If the results
     * are within the limit, they are returned unchanged. If they exceed the limit, the whole response
     * is replaced with an informational message while preserving the original structure of the response.
     * This ensures alerts can still be created even when query results are too large.
     *
     * @param pplQueryResults The PPL query response JSONObject
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
    fun capPPLQueryResultsSize(pplQueryResults: JSONObject, maxSize: Long): JSONObject {
        // estimate byte size with serialized string length
        // if query results size are already under the limit, do nothing
        // and return the query results as is
        val pplQueryResultsSize = pplQueryResults.toString().length
        if (pplQueryResultsSize <= maxSize) {
            return pplQueryResults
        }

        // if the query results exceed the limit, we need to replace the query results
        // with a message that says the results were too large, but still retain the other
        // ppl query response fields like schema, total, and size
        val limitExceedMessageQueryResults = JSONObject()

        val schema = JSONArray().put(JSONObject(mapOf("name" to "message", "type" to "string")))
        val datarows = JSONArray().put(JSONArray(listOf(PPL_RESULTS_SIZE_EXCEEDED_MESSAGE)))

        limitExceedMessageQueryResults.put("schema", schema)
        limitExceedMessageQueryResults.put("datarows", datarows)
        limitExceedMessageQueryResults.put("total", 1)
        limitExceedMessageQueryResults.put("size", 1)

        return limitExceedMessageQueryResults
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
