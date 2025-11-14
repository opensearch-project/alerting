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
    private val evalResultVarRegex = """\beval\s+([a-zA-Z_][a-zA-Z0-9_]*)\s*=""".toRegex()

    // captures the list of indices and index patterns that a given PPL query searches
    // e.g. PPL query: search source = index_1,index_pattern*,index_3 | where responseCode = 500 | head 10
    // captures: index_1,index_pattern*,index_3
    private val indicesListRegex =
        """(?i)source(?:\s*)=(?:\s*)((?:`[^`]+`|[-\w.*'+]+(?:\*)?)(?:\s*,\s*(?:`[^`]+`|[-\w.*'+]+\*?))*)\s*\|*""".toRegex()

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

    /**
     * Extracts the list of indices from a PPL query's source statement.
     *
     * Parses the PPL `source=` clause to identify which indices, index patterns, or index
     * aliases are being queried. This information is primarily used for permission checks.
     * Supports comma-separated lists of indices and wildcard patterns.
     *
     * @param pplQuery The complete PPL query string containing a source statement
     * @return A list of index names, patterns, or aliases (e.g., ["logs-*", "metrics-2024"])
     * @throws IllegalStateException if no valid source statement is found, even after
     *         the query has been validated by the SQL/PPL plugin
     *
     * @example
     * ```
     * val query = "source=logs-* | where level='ERROR'"
     * val indices = getIndicesFromPplQuery(query)
     * // Returns: ["logs-*"]
     *
     * val multiQuery = "source=logs-*, metrics-2024, .kibana | stats count()"
     * val multiIndices = getIndicesFromPplQuery(multiQuery)
     * // Returns: ["logs-*", "metrics-2024", ".kibana"]
     * ```
     *
     * @note Supports concrete indices, wildcard patterns (*), dot-prefixed system indices,
     *       and index aliases
     * @note PPL queries contain exactly one source statement, so only the first match is used
     * @note The regex pattern handles optional whitespace around `=` and commas
     *
     */
    fun getIndicesFromPplQuery(pplQuery: String): List<String> {
        // use find() instead of findAll() because a PPL query only ever has one source statement
        // the only capture group specified in the regex captures the comma separated string of indices/index patterns
        val indices = indicesListRegex.find(pplQuery)?.groupValues?.get(1)?.split(",")?.map { it.trim() }
            ?: throw IllegalStateException(
                "Could not find indices that PPL Monitor query searches even " +
                    "after validating the query through SQL/PPL plugin."
            )

        // remove any backticks that might have been read in
        val unBackTickedIndices = mutableListOf<String>()
        indices.forEach {
            if (it.startsWith("`") && it.endsWith("`")) {
                unBackTickedIndices.add(it.substring(1, it.length - 1))
            } else {
                unBackTickedIndices.add(it)
            }
        }

        return unBackTickedIndices.toList()
    }

    /**
     * Caps the size of PPL query results to prevent memory issues and oversized alert payloads.
     *
     * Checks if the serialized query results exceed a specified size limit. If the results
     * are within the limit, they are returned unchanged. If they exceed the limit, the datarows
     * are replaced with an informational message while preserving the schema and metadata fields.
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

        val schema = JSONArray(pplQueryResults.getJSONArray("schema").toList())
        val datarows = JSONArray().put(JSONArray(listOf("The PPL Query results were too large and thus excluded")))
        val total = pplQueryResults.getInt("total")
        val size = pplQueryResults.getInt("size")

        limitExceedMessageQueryResults.put("schema", schema)
        limitExceedMessageQueryResults.put("datarows", datarows)
        limitExceedMessageQueryResults.put("total", total)
        limitExceedMessageQueryResults.put("size", size)

        return limitExceedMessageQueryResults
    }
}
