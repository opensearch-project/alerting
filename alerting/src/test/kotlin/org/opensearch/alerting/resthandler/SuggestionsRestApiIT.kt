/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.alerting.resthandler

import org.apache.http.entity.ContentType.APPLICATION_JSON
import org.apache.http.entity.StringEntity
import org.apache.logging.log4j.LogManager
import org.opensearch.alerting.ALERTING_BASE_URI
import org.opensearch.alerting.AlertingRestTestCase
import org.opensearch.alerting.SUGGESTIONS_BASE_URI
import org.opensearch.alerting.makeRequest
import org.opensearch.alerting.opensearchapi.string
import org.opensearch.client.ResponseException
import org.opensearch.common.xcontent.ToXContent
import org.opensearch.common.xcontent.XContentFactory
import org.opensearch.rest.RestStatus

private val log = LogManager.getLogger(SuggestionsRestApiIT::class.java)

class SuggestionsRestApiIT : AlertingRestTestCase() {
    fun `test asking for suggestions with any order of request body fields`() {
        val monitor = createRandomMonitor()

        // order: inputType, component, input
        var builder = XContentFactory.jsonBuilder()
        builder.startObject()
        builder.field("inputType", "monitorObj")
        builder.field("component", "monitor")
        builder.startObject("input")
        builder.field("monitorObj")
        builder = monitor.toXContent(builder, ToXContent.EMPTY_PARAMS)
        builder.endObject() // input
        builder.endObject()

        var httpEntity = StringEntity(shuffleXContent(builder).string(), APPLICATION_JSON)

        var response = client().makeRequest("POST", SUGGESTIONS_BASE_URI, emptyMap(), httpEntity)
        assertEquals(RestStatus.OK, response.restStatus())

        // order: inputType, input, component
        builder = XContentFactory.jsonBuilder()
        builder.startObject()
        builder.field("inputType", "monitorObj")
        builder.startObject("input")
        builder.field("monitorObj")
        builder = monitor.toXContent(builder, ToXContent.EMPTY_PARAMS)
        builder.endObject() // input
        builder.field("component", "monitor")
        builder.endObject()

        httpEntity = StringEntity(shuffleXContent(builder).string(), APPLICATION_JSON)

        response = client().makeRequest("POST", SUGGESTIONS_BASE_URI, emptyMap(), httpEntity)
        assertEquals(RestStatus.OK, response.restStatus())

        // order: input, component, inputType
        builder = XContentFactory.jsonBuilder()
        builder.startObject()
        builder.startObject("input")
        builder.field("monitorObj")
        builder = monitor.toXContent(builder, ToXContent.EMPTY_PARAMS)
        builder.endObject() // input
        builder.field("component", "monitor")
        builder.field("inputType", "monitorObj")
        builder.endObject()

        httpEntity = StringEntity(shuffleXContent(builder).string(), APPLICATION_JSON)

        response = client().makeRequest("POST", SUGGESTIONS_BASE_URI, emptyMap(), httpEntity)
        assertEquals(RestStatus.OK, response.restStatus())
    }

    fun `test asking for suggestions with missing request body fields`() {
        // missing input
        var builder = XContentFactory.jsonBuilder()
        builder.startObject()
        builder.field("inputType", "monitorId")
        builder.field("component", "monitor")
        builder.endObject()

        var httpEntity = StringEntity(shuffleXContent(builder).string(), APPLICATION_JSON)

        try {
            client().makeRequest("POST", SUGGESTIONS_BASE_URI, emptyMap(), httpEntity)
        } catch (e: ResponseException) {
            assertEquals("Unexpected status", RestStatus.BAD_REQUEST, e.response.restStatus())
        }

        // missing inputType
        builder = XContentFactory.jsonBuilder()
        builder.startObject()
        builder.startObject("input")
        builder.field("monitorId", "dedaje93m3") // some random monitor id
        builder.endObject() // input
        builder.field("component", "monitor")
        builder.endObject()

        httpEntity = StringEntity(shuffleXContent(builder).string(), APPLICATION_JSON)

        try {
            client().makeRequest("POST", SUGGESTIONS_BASE_URI, emptyMap(), httpEntity)
        } catch (e: ResponseException) {
            assertEquals("Unexpected status", RestStatus.BAD_REQUEST, e.response.restStatus())
        }

        // missing component
        builder = XContentFactory.jsonBuilder()
        builder.startObject()
        builder.startObject("input")
        builder.field("monitorId", "dedaje93m3") // some random monitor id
        builder.endObject() // input
        builder.field("inputType", "monitorId")
        builder.endObject()

        httpEntity = StringEntity(shuffleXContent(builder).string(), APPLICATION_JSON)

        try {
            client().makeRequest("POST", SUGGESTIONS_BASE_URI, emptyMap(), httpEntity)
        } catch (e: ResponseException) {
            assertEquals("Unexpected status", RestStatus.BAD_REQUEST, e.response.restStatus())
        }
    }

    fun `test asking for suggestions with extra request body fields`() {
        val builder = XContentFactory.jsonBuilder()
        builder.startObject()
        builder.field("inputType", "monitorId")
        builder.field("component", "monitor")
        builder.startObject("input")
        builder.field("monitorId", "dedaje93m3") // some random monitor id
        builder.endObject() // input
        builder.field("extraField", "thisFieldShouldntBeHere")
        builder.endObject()

        val httpEntity = StringEntity(shuffleXContent(builder).string(), APPLICATION_JSON)

        try {
            client().makeRequest("POST", SUGGESTIONS_BASE_URI, emptyMap(), httpEntity)
        } catch (e: ResponseException) {
            assertEquals("Unexpected status", RestStatus.BAD_REQUEST, e.response.restStatus())
        }
    }

    fun `test asking for suggestions with invalid inputType`() {
        val builder = XContentFactory.jsonBuilder()
        builder.startObject()
        builder.field("inputType", "foobarbaz") // not a valid inputType
        builder.field("component", "monitor")
        builder.startObject("input")
        builder.field("monitorId", "dedaje93m3")
        builder.endObject() // input
        builder.endObject()

        val httpEntity = StringEntity(shuffleXContent(builder).string(), APPLICATION_JSON)

        try {
            client().makeRequest("POST", SUGGESTIONS_BASE_URI, emptyMap(), httpEntity)
        } catch (e: ResponseException) {
            assertEquals("Unexpected status", RestStatus.BAD_REQUEST, e.response.restStatus())
        }
    }

    @Suppress("UNCHECKED_CAST")
    fun `test asking for suggestions with invalid component or component for which there are no rules`() {
        val monitor = createRandomMonitor()

        var builder = XContentFactory.jsonBuilder()
        builder.startObject()
        builder.field("inputType", "monitorObj")
        builder.field("component", "monitor.notAComponent")
        builder.startObject("input")
        builder.field("monitorObj")
        builder = monitor.toXContent(builder, ToXContent.EMPTY_PARAMS)
        builder.endObject() // input
        builder.endObject()

        val httpEntity = StringEntity(shuffleXContent(builder).string(), APPLICATION_JSON)

        val response = client().makeRequest("POST", SUGGESTIONS_BASE_URI, emptyMap(), httpEntity)
        val responseBody = response.asMap()

        // GetSuggestionsResponse.kt always puts a List<String> in response, so this cast is ok
        val suggestions = responseBody["suggestions"] as List<String>

        assertEquals(1, suggestions.size) // should only contain the "component not found" message
        val msg = suggestions[0]
        assertEquals("no suggestions found for given object and its given component, or the supplied object or component is invalid", msg)
    }

    fun `test asking for suggestions with input object containing no fields`() {
        val monitor = createRandomMonitor()
        val createResponse = client().makeRequest("POST", ALERTING_BASE_URI, emptyMap(), monitor.toHttpEntity())

        val builder = XContentFactory.jsonBuilder()
        builder.startObject()
        builder.field("inputType", "monitorId")
        builder.field("component", "monitor")
        builder.startObject("input")
        // no input contents
        builder.endObject() // input
        builder.endObject()

        val httpEntity = StringEntity(shuffleXContent(builder).string(), APPLICATION_JSON)

        try {
            client().makeRequest("POST", SUGGESTIONS_BASE_URI, emptyMap(), httpEntity)
        } catch (e: ResponseException) {
            assertEquals("Unexpected status", RestStatus.BAD_REQUEST, e.response.restStatus())
        }
    }

    fun `test asking for suggestions with input object containing more than one field`() {
        val monitor = createRandomMonitor()
        val createResponse = client().makeRequest("POST", ALERTING_BASE_URI, emptyMap(), monitor.toHttpEntity())
        val monitorId = createResponse.asMap()["_id"] as String

        val builder = XContentFactory.jsonBuilder()
        builder.startObject()
        builder.field("inputType", "monitorId")
        builder.field("component", "monitor")
        builder.startObject("input")
        builder.field("monitorId", monitorId)
        builder.field("extraField", "thisShouldntBeHere") // extra field
        builder.endObject() // input
        builder.endObject()

        val httpEntity = StringEntity(shuffleXContent(builder).string(), APPLICATION_JSON)

        try {
            client().makeRequest("POST", SUGGESTIONS_BASE_URI, emptyMap(), httpEntity)
        } catch (e: ResponseException) {
            assertEquals("Unexpected status", RestStatus.BAD_REQUEST, e.response.restStatus())
        }
    }

    // INPUT SPECIFIC TESTS

    // MONITOR ID TESTS
    fun `test asking for suggestions with valid monitorId`() {
        val monitor = createRandomMonitor()
        val createResponse = client().makeRequest("POST", ALERTING_BASE_URI, emptyMap(), monitor.toHttpEntity())
        val monitorId = createResponse.asMap()["_id"] as String

        val builder = XContentFactory.jsonBuilder()
        builder.startObject()
        builder.field("inputType", "monitorId")
        builder.field("component", "monitor")
        builder.startObject("input")
        builder.field("monitorId", monitorId)
        builder.endObject() // input
        builder.endObject()

        val httpEntity = StringEntity(shuffleXContent(builder).string(), APPLICATION_JSON)

        val response = client().makeRequest("POST", SUGGESTIONS_BASE_URI, emptyMap(), httpEntity)
        assertEquals(RestStatus.OK, response.restStatus())
    }

    fun `test asking for suggestions with invalid monitorId`() {
        val monitor = createRandomMonitor()
        // index monitor just so Scheduled Job Index gets created first
        client().makeRequest("POST", ALERTING_BASE_URI, emptyMap(), monitor.toHttpEntity())

        val builder = XContentFactory.jsonBuilder()
        builder.startObject()
        builder.field("inputType", "monitorId")
        builder.field("component", "monitor")
        builder.startObject("input")
        builder.field("monitorId", "notAValidID")
        builder.endObject() // input
        builder.endObject()

        val httpEntity = StringEntity(shuffleXContent(builder).string(), APPLICATION_JSON)

        try {
            client().makeRequest("POST", SUGGESTIONS_BASE_URI, emptyMap(), httpEntity)
        } catch (e: ResponseException) {
            assertEquals("Unexpected status", RestStatus.NOT_FOUND, e.response.restStatus())
        }
    }

    fun `test asking for suggestions with mismatched inputType and input for monitorId`() {
        val monitor = createRandomMonitor()
        val createResponse = client().makeRequest("POST", ALERTING_BASE_URI, emptyMap(), monitor.toHttpEntity())
        val monitorId = createResponse.asMap()["_id"] as String

        val builder = XContentFactory.jsonBuilder()
        builder.startObject()
        builder.field("inputType", "monitorId")
        builder.field("component", "monitor")
        builder.startObject("input")
        builder.field("monitorObj", monitorId) // inputType is monitorId but wrong field name is used
        builder.endObject() // input
        builder.endObject()

        val httpEntity = StringEntity(shuffleXContent(builder).string(), APPLICATION_JSON)

        try {
            client().makeRequest("POST", SUGGESTIONS_BASE_URI, emptyMap(), httpEntity)
        } catch (e: ResponseException) {
            assertEquals("Unexpected status", RestStatus.BAD_REQUEST, e.response.restStatus())
        }
    }

    fun `test asking for suggestions with mismatched inputType and input contents for monitorId`() {
        val monitor = createRandomMonitor()

        var builder = XContentFactory.jsonBuilder()
        builder.startObject()
        builder.field("inputType", "monitorId")
        builder.field("component", "monitor")
        builder.startObject("input")
        builder.field("monitorId") // inputType is monitorId and field in "input" has correct name, but contents are something else, not id
        builder = monitor.toXContent(builder, ToXContent.EMPTY_PARAMS)
        builder.endObject() // input
        builder.endObject()

        val httpEntity = StringEntity(shuffleXContent(builder).string(), APPLICATION_JSON)

        try {
            client().makeRequest("POST", SUGGESTIONS_BASE_URI, emptyMap(), httpEntity)
        } catch (e: ResponseException) {
            assertEquals("Unexpected status", RestStatus.BAD_REQUEST, e.response.restStatus())
        }
    }

    // MONITOR OBJ TESTS
    fun `test asking for suggestions with monitorObj`() {
        val monitor = createRandomMonitor()

        var builder = XContentFactory.jsonBuilder()
        builder.startObject()
        builder.field("inputType", "monitorObj")
        builder.field("component", "monitor")
        builder.startObject("input")
        builder.field("monitorObj")
        builder = monitor.toXContent(builder, ToXContent.EMPTY_PARAMS)
        builder.endObject() // input
        builder.endObject()

        val httpEntity = StringEntity(shuffleXContent(builder).string(), APPLICATION_JSON)

        val response = client().makeRequest("POST", SUGGESTIONS_BASE_URI, emptyMap(), httpEntity)
        assertEquals(RestStatus.OK, response.restStatus())
    }

    fun `test asking for suggestions with mismatched inputType and input for monitorObj`() {
        val monitor = createRandomMonitor()

        var builder = XContentFactory.jsonBuilder()
        builder.startObject()
        builder.field("inputType", "monitorObj")
        builder.field("component", "monitor")
        builder.startObject("input")
        builder.field("monitorId") // inputType is monitorObj but wrong field name is used
        builder = monitor.toXContent(builder, ToXContent.EMPTY_PARAMS)
        builder.endObject() // input
        builder.endObject()

        val httpEntity = StringEntity(shuffleXContent(builder).string(), APPLICATION_JSON)

        try {
            client().makeRequest("POST", SUGGESTIONS_BASE_URI, emptyMap(), httpEntity)
        } catch (e: ResponseException) {
            assertEquals("Unexpected status", RestStatus.BAD_REQUEST, e.response.restStatus())
        }
    }

    fun `test asking for suggestions with mismatched inputType and input contents for monitorObj`() {
        val monitor = createRandomMonitor()
        val createResponse = client().makeRequest("POST", ALERTING_BASE_URI, emptyMap(), monitor.toHttpEntity())
        val monitorId = createResponse.asMap()["_id"] as String

        val builder = XContentFactory.jsonBuilder()
        builder.startObject()
        builder.field("inputType", "monitorObj")
        builder.field("component", "monitor")
        builder.startObject("input")
        builder.field("monitorObj", monitorId) // inputType is monitorObj and field in "input" has correct name, but contents are something else, not monitor obj
        builder.endObject() // input
        builder.endObject()

        val httpEntity = StringEntity(shuffleXContent(builder).string(), APPLICATION_JSON)

        try {
            client().makeRequest("POST", SUGGESTIONS_BASE_URI, emptyMap(), httpEntity)
        } catch (e: ResponseException) {
            assertEquals("Unexpected status", RestStatus.BAD_REQUEST, e.response.restStatus())
        }
    }

    // RULE SPECIFIC TESTS
}
