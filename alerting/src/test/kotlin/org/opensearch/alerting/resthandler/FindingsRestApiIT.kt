package org.opensearch.alerting.resthandler

import org.opensearch.alerting.AlertingRestTestCase
import org.opensearch.alerting.elasticapi.string
import org.opensearch.alerting.model.Finding
import org.opensearch.common.xcontent.ToXContent
import org.opensearch.common.xcontent.XContentBuilder
import org.opensearch.common.xcontent.XContentType
import org.opensearch.test.junit.annotations.TestLogging
import java.time.Instant
import java.util.UUID

@TestLogging("level:DEBUG", reason = "Debug for tests.")
@Suppress("UNCHECKED_CAST")
class FindingsRestApiIT : AlertingRestTestCase() {

    fun `test find Finding where doc is not retrieved`() {

        createFinding(matchingDocIds = setOf("someId"))
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

        val findingWith1 = createFinding(matchingDocIds = setOf("someId"), index = testIndex)
        val findingWith2 = createFinding(matchingDocIds = setOf("someId", "someId2"), index = testIndex)
        val response = searchFindings()
        assertEquals(2, response.totalFindings)
        for (findingWithDoc in response.findings) {
            if (findingWithDoc.finding.id == findingWith1) {
                assertEquals(1, findingWithDoc.documents.size)
                assertTrue(findingWithDoc.documents[0].found)
                assertEquals("This is an error from IAD region", findingWithDoc.documents[0].document["message"])
            } else if (findingWithDoc.finding.id == findingWith2) {
                assertEquals(2, findingWithDoc.documents.size)
                assertTrue(findingWithDoc.documents[0].found)
                assertTrue(findingWithDoc.documents[1].found)
                assertEquals("This is an error from IAD region", findingWithDoc.documents[0].document["message"])
                assertEquals("This is an error2 from IAD region", findingWithDoc.documents[1].document["message"])
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

        createFinding(matchingDocIds = setOf("someId"), index = testIndex)
        val findingId = createFinding(matchingDocIds = setOf("someId", "someId2"), index = testIndex)
        val response = searchFindings(mapOf(Pair("findingId", findingId)))
        assertEquals(1, response.totalFindings)
        assertEquals(findingId, response.findings[0].finding.id)
        assertEquals(2, response.findings[0].documents.size)
        assertTrue(response.findings[0].documents[0].found)
        assertTrue(response.findings[0].documents[1].found)
        assertEquals("This is an error from IAD region", response.findings[0].documents[0].document["message"])
        assertEquals("This is an error2 from IAD region", response.findings[0].documents[1].document["message"])
    }

    private fun createFinding(
        monitorId: String = "NO_ID",
        monitorName: String = "NO_NAME",
        index: String = "testIndex",
        docLevelQueryId: String = "NO_ID",
        docLevelQueryTags: List<String> = emptyList(),
        matchingDocIds: Set<String>
    ): String {
        val finding = Finding(
            id = UUID.randomUUID().toString(),
            relatedDocId = matchingDocIds.joinToString(","),
            monitorId = monitorId,
            monitorName = monitorName,
            index = index,
            queryId = docLevelQueryId,
            queryTags = docLevelQueryTags,
            severity = "sev3",
            timestamp = Instant.now(),
            triggerId = null,
            triggerName = null
        )

        val findingStr = finding.toXContent(XContentBuilder.builder(XContentType.JSON.xContent()), ToXContent.EMPTY_PARAMS).string()

        indexDoc(".opensearch-alerting-findings", finding.id, findingStr)
        return finding.id
    }
}
