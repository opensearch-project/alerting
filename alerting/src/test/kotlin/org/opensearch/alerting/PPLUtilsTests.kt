/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.alerting

import org.opensearch.alerting.PPLUtils.PPL_RESULTS_SIZE_EXCEEDED_MESSAGE
import org.opensearch.test.OpenSearchTestCase

class PPLUtilsTests : OpenSearchTestCase() {
    fun `test constructPPLQueryResultsMap with simple types`() {
        // Arrange: Simple query result with basic types
        val rawResults = mapOf(
            "schema" to listOf(
                mapOf("name" to "username", "type" to "string"),
                mapOf("name" to "count", "type" to "integer"),
                mapOf("name" to "active", "type" to "boolean")
            ),
            "datarows" to listOf(
                listOf("alice", 42, true),
                listOf("bob", 17, false),
                listOf("charlie", 99, true)
            ),
            "total" to 3,
            "size" to 3
        )

        // Act
        val result = PPLUtils.constructPPLQueryResultsMap(rawResults)

        // Assert
        assertEquals(3, result.size)

        // First row
        assertEquals("alice", result[0]["username"])
        assertEquals(42, result[0]["count"])
        assertEquals(true, result[0]["active"])

        // Second row
        assertEquals("bob", result[1]["username"])
        assertEquals(17, result[1]["count"])
        assertEquals(false, result[1]["active"])

        // Third row
        assertEquals("charlie", result[2]["username"])
        assertEquals(99, result[2]["count"])
        assertEquals(true, result[2]["active"])
    }

    fun `test constructPPLQueryResultsMap with size exceeded message`() {
        // Arrange: Simple query result with basic types
        val rawResults = mapOf(
            "schema" to listOf(
                mapOf("name" to "message", "type" to "string")
            ),
            "datarows" to listOf(
                listOf(PPL_RESULTS_SIZE_EXCEEDED_MESSAGE)
            ),
            "total" to 3,
            "size" to 3
        )

        // Act
        val result = PPLUtils.constructPPLQueryResultsMap(rawResults)

        // Assert
        assertEquals(1, result.size)

        // First row
        assertEquals(PPL_RESULTS_SIZE_EXCEEDED_MESSAGE, result[0]["message"])
    }

    fun `test constructPPLQueryResultsMap with nested objects and nulls`() {
        // Arrange: Complex query result with nested arrays, objects, and nulls
        val rawResults = mapOf(
            "schema" to listOf(
                mapOf("name" to "list", "type" to "bigint"),
                mapOf("name" to "user", "type" to "struct"),
                mapOf("name" to "abc", "type" to "string")
            ),
            "datarows" to listOf(
                // Row 1: All fields populated with complex types
                listOf(
                    listOf(1, 2, 3), // Nested array
                    mapOf("name" to "bob", "age" to 32), // Nested object
                    "abc" // Simple string
                ),
                // Row 2: First field is null, rest populated
                listOf(
                    null, // Null array
                    mapOf("name" to "bob", "age" to 32), // Nested object
                    "abc" // Simple string
                ),
                // Row 3: Multiple null fields
                listOf(
                    null, // Null array
                    null, // Null object
                    "abc" // Simple string
                )
            ),
            "total" to 3,
            "size" to 3
        )

        // Act
        val result = PPLUtils.constructPPLQueryResultsMap(rawResults)

        // Assert
        assertEquals(3, result.size)

        // Row 1: Nested array and nested object
        val row1 = result[0]
        val list1 = row1["list"] as? List<*>
        assertNotNull(list1)
        assertEquals(3, list1?.size)
        assertEquals(1, list1?.get(0))
        assertEquals(2, list1?.get(1))
        assertEquals(3, list1?.get(2))

        val user1 = row1["user"] as? Map<*, *>
        assertNotNull(user1)
        assertEquals("bob", user1?.get("name"))
        assertEquals(32, user1?.get("age"))

        assertEquals("abc", row1["abc"])

        // Row 2: Null list, populated user
        val row2 = result[1]
        assertNull(row2["list"])

        val user2 = row2["user"] as? Map<*, *>
        assertNotNull(user2)
        assertEquals("bob", user2?.get("name"))
        assertEquals(32, user2?.get("age"))

        assertEquals("abc", row2["abc"])

        // Row 3: Multiple nulls
        val row3 = result[2]
        assertNull(row3["list"])
        assertNull(row3["user"])
        assertEquals("abc", row3["abc"])
    }

    fun `test constructPPLQueryResultsMap with empty schema`() {
        // Arrange: Empty schema
        val rawResults = mapOf(
            "schema" to emptyList<Map<String, Any>>(),
            "datarows" to listOf(
                listOf("value1", "value2")
            ),
            "total" to 1,
            "size" to 1
        )

        // Act
        val result = PPLUtils.constructPPLQueryResultsMap(rawResults)

        // Assert
        assertTrue(result.isEmpty())
    }

    fun `test constructPPLQueryResultsMap with empty datarows`() {
        // Arrange: Empty datarows
        val rawResults = mapOf(
            "schema" to listOf(
                mapOf("name" to "field1", "type" to "string"),
                mapOf("name" to "field2", "type" to "integer")
            ),
            "datarows" to emptyList<List<Any>>(),
            "total" to 0,
            "size" to 0
        )

        // Act
        val result = PPLUtils.constructPPLQueryResultsMap(rawResults)

        // Assert
        assertTrue(result.isEmpty())
    }

    fun `test constructPPLQueryResultsMap with missing schema`() {
        // Arrange: Missing schema field
        val rawResults = mapOf(
            "datarows" to listOf(
                listOf("value1", "value2")
            ),
            "total" to 1,
            "size" to 1
        )

        // Act
        val result = PPLUtils.constructPPLQueryResultsMap(rawResults)

        // Assert
        assertTrue(result.isEmpty())
    }

    fun `test constructPPLQueryResultsMap with missing datarows`() {
        // Arrange: Missing datarows field
        val rawResults = mapOf(
            "schema" to listOf(
                mapOf("name" to "field1", "type" to "string")
            ),
            "total" to 0,
            "size" to 0
        )

        // Act
        val result = PPLUtils.constructPPLQueryResultsMap(rawResults)

        // Assert
        assertTrue(result.isEmpty())
    }

    fun `test constructPPLQueryResultsMap with mismatched row lengths`() {
        // Arrange: More schema fields than datarow values
        val rawResults = mapOf(
            "schema" to listOf(
                mapOf("name" to "field1", "type" to "string"),
                mapOf("name" to "field2", "type" to "integer"),
                mapOf("name" to "field3", "type" to "boolean")
            ),
            "datarows" to listOf(
                listOf("value1", 42), // Missing third field
                listOf("value2") // Missing second and third fields
            ),
            "total" to 2,
            "size" to 2
        )

        // Act
        val result = PPLUtils.constructPPLQueryResultsMap(rawResults)

        // Assert
        assertEquals(2, result.size)

        // First row: third field should be null
        assertEquals("value1", result[0]["field1"])
        assertEquals(42, result[0]["field2"])
        assertNull(result[0]["field3"])

        // Second row: second and third fields should be null
        assertEquals("value2", result[1]["field1"])
        assertNull(result[1]["field2"])
        assertNull(result[1]["field3"])
    }

    fun `test constructPPLQueryResultsMap with extra datarow values`() {
        // Arrange: More datarow values than schema fields
        val rawResults = mapOf(
            "schema" to listOf(
                mapOf("name" to "field1", "type" to "string"),
                mapOf("name" to "field2", "type" to "integer")
            ),
            "datarows" to listOf(
                listOf("value1", 42, true, "extra") // Extra values ignored
            ),
            "total" to 1,
            "size" to 1
        )

        // Act
        val result = PPLUtils.constructPPLQueryResultsMap(rawResults)

        // Assert
        assertEquals(1, result.size)
        assertEquals(2, result[0].size)
        assertEquals("value1", result[0]["field1"])
        assertEquals(42, result[0]["field2"])
        // Extra values are ignored
    }

    fun `test constructPPLQueryResultsMap with deeply nested structures`() {
        // Arrange: Deeply nested objects and arrays
        val rawResults = mapOf(
            "schema" to listOf(
                mapOf("name" to "nested_data", "type" to "struct")
            ),
            "datarows" to listOf(
                listOf(
                    mapOf(
                        "level1" to mapOf(
                            "level2" to mapOf(
                                "level3" to listOf(1, 2, 3)
                            )
                        ),
                        "array_of_objects" to listOf(
                            mapOf("id" to 1, "name" to "item1"),
                            mapOf("id" to 2, "name" to "item2")
                        )
                    )
                )
            ),
            "total" to 1,
            "size" to 1
        )

        // Act
        val result = PPLUtils.constructPPLQueryResultsMap(rawResults)

        // Assert
        assertEquals(1, result.size)

        val nestedData = result[0]["nested_data"] as? Map<*, *>
        assertNotNull(nestedData)

        val level1 = nestedData?.get("level1") as? Map<*, *>
        assertNotNull(level1)

        val level2 = level1?.get("level2") as? Map<*, *>
        assertNotNull(level2)

        val level3 = level2?.get("level3") as? List<*>
        assertNotNull(level3)
        assertEquals(3, level3?.size)

        val arrayOfObjects = nestedData?.get("array_of_objects") as? List<*>
        assertNotNull(arrayOfObjects)
        assertEquals(2, arrayOfObjects?.size)

        val firstObject = arrayOfObjects?.get(0) as? Map<*, *>
        assertEquals(1, firstObject?.get("id"))
        assertEquals("item1", firstObject?.get("name"))
    }
}
