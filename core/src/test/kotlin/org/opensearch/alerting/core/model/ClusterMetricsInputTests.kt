/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.alerting.core.model

import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith

class ClusterMetricsInputTests {
    private var path = "/_cluster/health"
    private var pathParams = ""
    private var url = ""

    @Test
    fun `test valid ClusterMetricsInput creation using HTTP URI component fields`() {
        // GIVEN
        val testUrl = "http://localhost:9200/_cluster/health"

        // WHEN
        val clusterMetricsInput = ClusterMetricsInput(path, pathParams, url)

        // THEN
        assertEquals(path, clusterMetricsInput.path)
        assertEquals(pathParams, clusterMetricsInput.pathParams)
        assertEquals(testUrl, clusterMetricsInput.url)
    }

    @Test
    fun `test valid ClusterMetricsInput creation using HTTP url field`() {
        // GIVEN
        path = ""
        url = "http://localhost:9200/_cluster/health"

        // WHEN
        val clusterMetricsInput = ClusterMetricsInput(path, pathParams, url)

        // THEN
        assertEquals(url, clusterMetricsInput.url)
    }

    @Test
    fun `test valid ClusterMetricsInput creation using HTTPS url field`() {
        // GIVEN
        path = ""
        url = "https://localhost:9200/_cluster/health"

        // WHEN
        val clusterMetricsInput = ClusterMetricsInput(path, pathParams, url)

        // THEN
        assertEquals(url, clusterMetricsInput.url)
    }

    @Test
    fun `test invalid path`() {
        // GIVEN
        path = "///"

        // WHEN + THEN
        assertFailsWith<IllegalArgumentException>("Invalid URL.") {
            ClusterMetricsInput(path, pathParams, url)
        }
    }

    @Test
    fun `test invalid url`() {
        // GIVEN
        url = "///"

        // WHEN + THEN
        assertFailsWith<IllegalArgumentException>("Invalid URL.") {
            ClusterMetricsInput(path, pathParams, url)
        }
    }

    @Test
    fun `test url field and URI component fields create equal URI`() {
        // GIVEN
        url = "http://localhost:9200/_cluster/health"

        // WHEN
        val clusterMetricsInput = ClusterMetricsInput(path, pathParams, url)

        // THEN
        assertEquals(path, clusterMetricsInput.path)
        assertEquals(pathParams, clusterMetricsInput.pathParams)
        assertEquals(url, clusterMetricsInput.url)
        assertEquals(url, clusterMetricsInput.constructedUri.toString())
    }

    @Test
    fun `test url field and URI component fields with path params create equal URI`() {
        // GIVEN
        path = "/_cluster/health/"
        pathParams = "index1,index2,index3,index4,index5"
        url = "http://localhost:9200/_cluster/health/index1,index2,index3,index4,index5"

        // WHEN
        val clusterMetricsInput = ClusterMetricsInput(path, pathParams, url)

        // THEN
        assertEquals(path, clusterMetricsInput.path)
        assertEquals(pathParams, clusterMetricsInput.pathParams)
        assertEquals(url, clusterMetricsInput.url)
        assertEquals(url, clusterMetricsInput.constructedUri.toString())
    }

    @Test
    fun `test url field and URI component fields create different URI`() {
        // GIVEN
        url = "http://localhost:9200/_cluster/stats"

        // WHEN + THEN
        assertFailsWith<IllegalArgumentException>("The provided URL and URI fields form different URLs.") {
            ClusterMetricsInput(path, pathParams, url)
        }
    }

    @Test
    fun `test url field and URI component fields with path params create different URI`() {
        // GIVEN
        pathParams = "index1,index2,index3,index4,index5"
        url = "http://localhost:9200/_cluster/stats/index1,index2,index3,index4,index5"

        // WHEN + THEN
        assertFailsWith<IllegalArgumentException>("The provided URL and URI fields form different URLs.") {
            ClusterMetricsInput(path, pathParams, url)
        }
    }

    @Test
    fun `test ClusterMetricsInput creation when all inputs are empty`() {
        // GIVEN
        path = ""
        pathParams = ""
        url = ""

        // WHEN + THEN
        assertFailsWith<IllegalArgumentException>("The uri.api_type field, uri.path field, or uri.uri field must be defined.") {
            ClusterMetricsInput(path, pathParams, url)
        }
    }

    @Test
    fun `test ClusterMetricsInput creation when all inputs but path params are empty`() {
        // GIVEN
        path = ""
        pathParams = "index1,index2,index3,index4,index5"
        url = ""

        // WHEN + THEN
        assertFailsWith<IllegalArgumentException>("The uri.api_type field, uri.path field, or uri.uri field must be defined.") {
            ClusterMetricsInput(path, pathParams, url)
        }
    }

    @Test
    fun `test invalid scheme in url field`() {
        // GIVEN
        path = ""
        url = "invalidScheme://localhost:9200/_cluster/health"

        // WHEN + THEN
        assertFailsWith<IllegalArgumentException>("Invalid URL.") {
            ClusterMetricsInput(path, pathParams, url)
        }
    }

    @Test
    fun `test invalid host in url field`() {
        // GIVEN
        path = ""
        url = "http://127.0.0.1:9200/_cluster/health"

        // WHEN + THEN
        assertFailsWith<IllegalArgumentException>("Only host '${ClusterMetricsInput.SUPPORTED_HOST}' is supported.") {
            ClusterMetricsInput(path, pathParams, url)
        }
    }

    @Test
    fun `test invalid port in url field`() {
        // GIVEN
        path = ""
        url = "http://localhost:${ClusterMetricsInput.SUPPORTED_PORT + 1}/_cluster/health"

        // WHEN + THEN
        assertFailsWith<IllegalArgumentException>("Only port '${ClusterMetricsInput.SUPPORTED_PORT}' is supported.") {
            ClusterMetricsInput(path, pathParams, url)
        }
    }

    @Test
    fun `test parsePathParams with no path params`() {
        // GIVEN
        val testUrl = "http://localhost:9200/_cluster/health"
        val clusterMetricsInput = ClusterMetricsInput(path, pathParams, url)

        // WHEN
        val params = clusterMetricsInput.parsePathParams()

        // THEN
        assertEquals(pathParams, params)
        assertEquals(testUrl, clusterMetricsInput.constructedUri.toString())
    }

    @Test
    fun `test parsePathParams with path params as URI field`() {
        // GIVEN
        path = "/_cluster/health/"
        pathParams = "index1,index2,index3,index4,index5"
        val testUrl = "http://localhost:9200/_cluster/health/index1,index2,index3,index4,index5"
        val clusterMetricsInput = ClusterMetricsInput(path, pathParams, url)

        // WHEN
        val params = clusterMetricsInput.parsePathParams()

        // THEN
        assertEquals(pathParams, params)
        assertEquals(testUrl, clusterMetricsInput.constructedUri.toString())
    }

    @Test
    fun `test parsePathParams with path params in url`() {
        // GIVEN
        path = ""
        val testParams = "index1,index2,index3,index4,index5"
        url = "http://localhost:9200/_cluster/health/index1,index2,index3,index4,index5"
        val clusterMetricsInput = ClusterMetricsInput(path, pathParams, url)

        // WHEN
        val params = clusterMetricsInput.parsePathParams()

        // THEN
        assertEquals(testParams, params)
        assertEquals(url, clusterMetricsInput.constructedUri.toString())
    }

    @Test
    fun `test parsePathParams with no path params for ApiType that requires path params`() {
        // GIVEN
        path = "/_cat/snapshots"

        // WHEN + THEN
        assertFailsWith<IllegalArgumentException>("The API requires path parameters.") {
            ClusterMetricsInput(path, pathParams, url)
        }
    }

    @Test
    fun `test parsePathParams with path params for ApiType that doesn't support path params`() {
        // GIVEN
        path = "/_cluster/settings"
        pathParams = "index1,index2,index3,index4,index5"
        val clusterMetricsInput = ClusterMetricsInput(path, pathParams, url)

        // WHEN + THEN
        assertFailsWith<IllegalArgumentException>("The API does not use path parameters.") {
            clusterMetricsInput.parsePathParams()
        }
    }

    @Test
    fun `test parsePathParams with path params containing illegal characters`() {
        var testCount = 0 // Start off with count of 1 to account for ApiType.BLANK
        ILLEGAL_PATH_PARAMETER_CHARACTERS.forEach { character ->
            // GIVEN
            pathParams = "index1,index2,$character,index4,index5"
            val clusterMetricsInput = ClusterMetricsInput(path, pathParams, url)

            // WHEN + THEN
            assertFailsWith<IllegalArgumentException>(
                "The provided path parameters contain invalid characters or spaces. Please omit: " +
                    "${ILLEGAL_PATH_PARAMETER_CHARACTERS.joinToString(" ")}"
            ) {
                clusterMetricsInput.parsePathParams()
            }
            testCount++
        }
        assertEquals(ILLEGAL_PATH_PARAMETER_CHARACTERS.size, testCount)
    }

    @Test
    fun `test ClusterMetricsInput correctly determines ApiType when path is provided as URI component`() {
        var testCount = 1 // Start off with count of 1 to account for ApiType.BLANK
        ClusterMetricsInput.ClusterMetricType.values()
            .filter { enum -> enum != ClusterMetricsInput.ClusterMetricType.BLANK }
            .forEach { testApiType ->
                // GIVEN
                path = testApiType.defaultPath
                pathParams = if (testApiType.supportsPathParams) "index1,index2,index3,index4,index5" else ""

                // WHEN
                val clusterMetricsInput = ClusterMetricsInput(path, pathParams, url)

                // THEN
                assertEquals(testApiType, clusterMetricsInput.clusterMetricType)
                testCount++
            }
        assertEquals(ClusterMetricsInput.ClusterMetricType.values().size, testCount)
    }

    @Test
    fun `test ClusterMetricsInput correctly determines ApiType when path and path params are provided as URI components`() {
        var testCount = 1 // Start off with count of 1 to account for ApiType.BLANK
        ClusterMetricsInput.ClusterMetricType.values()
            .filter { enum -> enum != ClusterMetricsInput.ClusterMetricType.BLANK }
            .forEach { testApiType ->
                // GIVEN
                path = testApiType.defaultPath
                pathParams = "index1,index2,index3,index4,index5"

                // WHEN
                val clusterMetricsInput = ClusterMetricsInput(path, pathParams, url)

                // THEN
                assertEquals(testApiType, clusterMetricsInput.clusterMetricType)
                testCount++
            }
        assertEquals(ClusterMetricsInput.ClusterMetricType.values().size, testCount)
    }

    @Test
    fun `test ClusterMetricsInput correctly determines ApiType when path is provided in URL field`() {
        var testCount = 1 // Start off with count of 1 to account for ApiType.BLANK
        ClusterMetricsInput.ClusterMetricType.values()
            .filter { enum -> enum != ClusterMetricsInput.ClusterMetricType.BLANK }
            .forEach { testApiType ->
                // GIVEN
                path = ""
                pathParams = if (testApiType.supportsPathParams) "index1,index2,index3,index4,index5" else ""
                url = "http://localhost:9200${testApiType.defaultPath}"

                // WHEN
                val clusterMetricsInput = ClusterMetricsInput(path, pathParams, url)

                // THEN
                assertEquals(testApiType, clusterMetricsInput.clusterMetricType)
                testCount++
            }
        assertEquals(ClusterMetricsInput.ClusterMetricType.values().size, testCount)
    }

    @Test
    fun `test ClusterMetricsInput correctly determines ApiType when path and path params are provided in URL field`() {
        var testCount = 1 // Start off with count of 1 to account for ApiType.BLANK
        ClusterMetricsInput.ClusterMetricType.values()
            .filter { enum -> enum != ClusterMetricsInput.ClusterMetricType.BLANK }
            .forEach { testApiType ->
                // GIVEN
                path = ""
                pathParams = if (testApiType.supportsPathParams) "/index1,index2,index3,index4,index5" else ""
                url = "http://localhost:9200${testApiType.defaultPath}$pathParams"

                // WHEN
                val clusterMetricsInput = ClusterMetricsInput(path, pathParams, url)

                // THEN
                assertEquals(testApiType, clusterMetricsInput.clusterMetricType)
                testCount++
            }
        assertEquals(ClusterMetricsInput.ClusterMetricType.values().size, testCount)
    }

    @Test
    fun `test ClusterMetricsInput cannot determine ApiType when invalid path is provided as URI component`() {
        // GIVEN
        path = "/_cat/paws"

        // WHEN + THEN
        assertFailsWith<IllegalArgumentException>("The API could not be determined from the provided URI.") {
            ClusterMetricsInput(path, pathParams, url)
        }
    }

    @Test
    fun `test ClusterMetricsInput cannot determine ApiType when invalid path and path params are provided as URI components`() {
        // GIVEN
        path = "/_cat/paws"
        pathParams = "index1,index2,index3,index4,index5"

        // WHEN + THEN
        assertFailsWith<IllegalArgumentException>("The API could not be determined from the provided URI.") {
            ClusterMetricsInput(path, pathParams, url)
        }
    }

    @Test
    fun `test ClusterMetricsInput cannot determine ApiType when invaid path is provided in URL`() {
        // GIVEN
        path = ""
        url = "http://localhost:9200/_cat/paws"

        // WHEN + THEN
        assertFailsWith<IllegalArgumentException>("The API could not be determined from the provided URI.") {
            ClusterMetricsInput(path, pathParams, url)
        }
    }

    @Test
    fun `test ClusterMetricsInput cannot determine ApiType when invaid path and path params are provided in URL`() {
        // GIVEN
        path = ""
        url = "http://localhost:9200/_cat/paws/index1,index2,index3,index4,index5"

        // WHEN + THEN
        assertFailsWith<IllegalArgumentException>("The API could not be determined from the provided URI.") {
            ClusterMetricsInput(path, pathParams, url)
        }
    }

    @Test
    fun `test parseEmptyFields populates empty path and path_params when url is provided`() {
        // GIVEN
        path = ""
        pathParams = ""
        val testPath = "/_cluster/health"
        val testPathParams = "index1,index2,index3,index4,index5"
        url = "http://localhost:9200$testPath$testPathParams"

        // WHEN
        val clusterMetricsInput = ClusterMetricsInput(path, pathParams, url)

        // THEN
        assertEquals(testPath, clusterMetricsInput.path)
        assertEquals(testPathParams, clusterMetricsInput.pathParams)
        assertEquals(url, clusterMetricsInput.url)
    }

    @Test
    fun `test parseEmptyFields populates empty url field when path and path_params are provided`() {
        // GIVEN
        path = "/_cluster/health/"
        pathParams = "index1,index2,index3,index4,index5"
        val testUrl = "http://localhost:9200$path$pathParams"

        // WHEN
        val clusterMetricsInput = ClusterMetricsInput(path, pathParams, url)

        // THEN
        assertEquals(path, clusterMetricsInput.path)
        assertEquals(pathParams, clusterMetricsInput.pathParams)
        assertEquals(testUrl, clusterMetricsInput.url)
    }
}
