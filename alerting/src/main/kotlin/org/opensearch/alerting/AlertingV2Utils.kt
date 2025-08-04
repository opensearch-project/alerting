/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.alerting

import org.json.JSONArray
import org.json.JSONObject
import org.opensearch.alerting.core.ppl.PPLPluginInterface
import org.opensearch.alerting.modelv2.MonitorV2
import org.opensearch.alerting.opensearchapi.suspendUntil
import org.opensearch.commons.alerting.model.Monitor
import org.opensearch.commons.alerting.model.ScheduledJob
import org.opensearch.commons.alerting.model.Workflow
import org.opensearch.sql.plugin.transport.TransportPPLQueryRequest
import org.opensearch.transport.client.node.NodeClient

object AlertingV2Utils {
    // Validates that the given scheduled job is a Monitor
    // returns the exception to pass into actionListener.onFailure if not.
    fun validateMonitorV1(scheduledJob: ScheduledJob): Exception? {
        if (scheduledJob is MonitorV2) {
            return IllegalStateException("The ID given corresponds to a V2 Monitor, but a V1 Monitor was expected")
        } else if (scheduledJob !is Monitor && scheduledJob !is Workflow) {
            return IllegalStateException("The ID given corresponds to a scheduled job of unknown type: ${scheduledJob.javaClass.name}")
        }
        return null
    }

    // Validates that the given scheduled job is a MonitorV2
    // returns the exception to pass into actionListener.onFailure if not.
    fun validateMonitorV2(scheduledJob: ScheduledJob): Exception? {
        if (scheduledJob is Monitor || scheduledJob is Workflow) {
            return IllegalStateException("The ID given corresponds to a V1 Monitor, but a V2 Monitor was expected")
        } else if (scheduledJob !is MonitorV2) {
            return IllegalStateException("The ID given corresponds to a scheduled job of unknown type: ${scheduledJob.javaClass.name}")
        }
        return null
    }

    // appends user-defined custom trigger condition to PPL query, only for custom condition Triggers
    fun appendCustomCondition(query: String, customCondition: String): String {
        return "$query | $customCondition"
    }

    fun appendDataRowsLimit(query: String, maxDataRows: Long): String {
        return "$query | head $maxDataRows"
    }

    // returns PPL query response as parsable JSONObject
    suspend fun executePplQuery(query: String, client: NodeClient): JSONObject {
        // call PPL plugin to execute query
        val transportPplQueryRequest = TransportPPLQueryRequest(
            query,
            JSONObject(mapOf("query" to query)),
            null // null path falls back to a default path internal to SQL/PPL Plugin
        )

        val transportPplQueryResponse = PPLPluginInterface.suspendUntil {
            this.executeQuery(
                client,
                transportPplQueryRequest,
                it
            )
        }

        val queryResponseJson = JSONObject(transportPplQueryResponse.result)

        return queryResponseJson
    }

    // searches a given custom condition eval statement for the name of
    // the eval result variable and returns it
    fun findEvalResultVar(customCondition: String): String {
        // the PPL keyword "eval", followed by a whitespace must be present, otherwise a syntax error from PPL plugin would've
        // been thrown when executing the query (without the whitespace, the query would've had something like "evalresult",
        // which is invalid PPL
        val regex = """\beval\s+([a-zA-Z_][a-zA-Z0-9_]*)\s*=""".toRegex()
        val evalResultVar = regex.find(customCondition)?.groupValues?.get(1)
            ?: throw IllegalArgumentException("Given custom condition is invalid, could not find eval result variable")
        return evalResultVar
    }

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
                "expected to find eval statement results variable \"$evalResultVarName\" in results " +
                    "of PPL query with custom condition, but did not."
            )
        }

        return evalResultVarIdx
    }

    fun getIndicesFromPplQuery(pplQuery: String): List<String> {
        // captures comma-separated concrete indices, index patterns, and index aliases
        val indicesRegex = """(?i)source(?:\s*)=(?:\s*)([-\w.*'+]+(?:\*)?(?:\s*,\s*[-\w.*'+]+\*?)*)\s*\|*""".toRegex()

        // use find() instead of findAll() because a PPL query only ever has one source statement
        // the only capture group specified in the regex captures the comma separated string of indices/index patterns
        val indices = indicesRegex.find(pplQuery)?.groupValues?.get(1)?.split(",")?.map { it.trim() }
            ?: throw IllegalStateException(
                "Could not find indices that PPL Monitor query searches even " +
                    "after validating the query through SQL/PPL plugin"
            )

        return indices
    }

    fun capPplQueryResultsSize(pplQueryResults: JSONObject, maxSize: Long): JSONObject {
        /*
        the query results JSON object schema:
        schema: an array of objects storing the data types of each value of the query result rows, in order
        datarows: an array of arrays storing the query results themselves
        total: total number of results / data rows
        size: same as total, redundant field
         */

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
