package org.opensearch.alerting.resthandler

import org.opensearch.alerting.AlertingRestTestCase
import org.opensearch.alerting.core.model.DocLevelQuery
import org.opensearch.test.junit.annotations.TestLogging

@TestLogging("level:DEBUG", reason = "Debug for tests.")
@Suppress("UNCHECKED_CAST")
class FindingsRestApiIT : AlertingRestTestCase() {

    fun `test find Finding where doc is not retrieved`() {

        createFinding(matchingDocIds = listOf("someId"))
        val response = searchFindings()
        assertEquals(1, response.totalFindings)
        assertEquals(1, response.findings[0].documents.size)
        assertFalse(response.findings[0].documents[0].found)
    }

    fun `test find Finding where doc is retrieved`() {
        val testIndex = createTestIndex()
        val testDoc = """{
            "message" : "This is an error from IAD region",
            "test_field" : "us-west-2"
        }"""
        indexDoc(testIndex, "someId", testDoc)
        val testDoc2 = """{
            "message" : "This is an error2 from IAD region",
            "test_field" : "us-west-3"
        }"""
        indexDoc(testIndex, "someId2", testDoc2)

        val findingWith1 = createFinding(matchingDocIds = listOf("someId"), index = testIndex)
        val findingWith2 = createFinding(matchingDocIds = listOf("someId", "someId2"), index = testIndex)
        val response = searchFindings()
        assertEquals(2, response.totalFindings)
        for (findingWithDoc in response.findings) {
            if (findingWithDoc.finding.id == findingWith1) {
                assertEquals(1, findingWithDoc.documents.size)
                assertTrue(findingWithDoc.documents[0].found)
                assertEquals(testDoc, findingWithDoc.documents[0].document)
            } else if (findingWithDoc.finding.id == findingWith2) {
                assertEquals(2, findingWithDoc.documents.size)
                assertTrue(findingWithDoc.documents[0].found)
                assertTrue(findingWithDoc.documents[1].found)
                assertEquals(testDoc, findingWithDoc.documents[0].document)
                assertEquals(testDoc2, findingWithDoc.documents[1].document)
            } else {
                fail("Found a finding that should not have been retrieved")
            }
        }
    }

    fun `test find Finding for specific finding by id`() {
        val testIndex = createTestIndex()
        val testDoc = """{
            "message" : "This is an error from IAD region",
            "test_field" : "us-west-2"
        }"""
        indexDoc(testIndex, "someId", testDoc)
        val testDoc2 = """{
            "message" : "This is an error2 from IAD region",
            "test_field" : "us-west-3"
        }"""
        indexDoc(testIndex, "someId2", testDoc2)

        createFinding(matchingDocIds = listOf("someId"), index = testIndex)
        val findingId = createFinding(matchingDocIds = listOf("someId", "someId2"), index = testIndex)
        val response = searchFindings(mapOf(Pair("findingId", findingId)))
        assertEquals(1, response.totalFindings)
        assertEquals(findingId, response.findings[0].finding.id)
        assertEquals(2, response.findings[0].documents.size)
        assertTrue(response.findings[0].documents[0].found)
        assertTrue(response.findings[0].documents[1].found)
        assertEquals(testDoc, response.findings[0].documents[0].document)
        assertEquals(testDoc2, response.findings[0].documents[1].document)
    }

    fun `test find Finding by tag`() {
        val testIndex = createTestIndex()
        val testDoc = """{
            "message" : "This is an error from IAD region",
            "test_field" : "us-west-2"
        }"""
        indexDoc(testIndex, "someId", testDoc)
        val testDoc2 = """{
            "message" : "This is an error2 from IAD region",
            "test_field" : "us-west-3"
        }"""
        indexDoc(testIndex, "someId2", testDoc2)

        val docLevelQuery = DocLevelQuery(query = "test_field:\"us-west-2\"", name = "realQuery", tags = listOf("sigma"))
        createFinding(matchingDocIds = listOf("someId"), index = testIndex)
        val findingId = createFinding(
            matchingDocIds = listOf("someId", "someId2"),
            index = testIndex,
            docLevelQueries = listOf(docLevelQuery)
        )
        val response = searchFindings(mapOf(Pair("searchString", "sigma")))
        assertEquals(1, response.totalFindings)
        assertEquals(findingId, response.findings[0].finding.id)
        assertEquals(2, response.findings[0].documents.size)
        assertTrue(response.findings[0].documents[0].found)
        assertTrue(response.findings[0].documents[1].found)
        assertEquals(testDoc, response.findings[0].documents[0].document)
        assertEquals(testDoc2, response.findings[0].documents[1].document)
    }

    fun `test find Finding by name`() {
        val testIndex = createTestIndex()
        val testDoc = """{
            "message" : "This is an error from IAD region",
            "test_field" : "us-west-2"
        }"""
        indexDoc(testIndex, "someId", testDoc)
        val testDoc2 = """{
            "message" : "This is an error2 from IAD region",
            "test_field" : "us-west-3"
        }"""
        indexDoc(testIndex, "someId2", testDoc2)

        val docLevelQuery = DocLevelQuery(query = "test_field:\"us-west-2\"", name = "realQuery", tags = listOf("sigma"))
        createFinding(matchingDocIds = listOf("someId"), index = testIndex)
        val findingId = createFinding(
            matchingDocIds = listOf("someId", "someId2"),
            index = testIndex,
            docLevelQueries = listOf(docLevelQuery)
        )
        val response = searchFindings(mapOf(Pair("searchString", "realQuery")))
        assertEquals(1, response.totalFindings)
        assertEquals(findingId, response.findings[0].finding.id)
        assertEquals(2, response.findings[0].documents.size)
        assertTrue(response.findings[0].documents[0].found)
        assertTrue(response.findings[0].documents[1].found)
        assertEquals(testDoc, response.findings[0].documents[0].document)
        assertEquals(testDoc2, response.findings[0].documents[1].document)
    }
}
