/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.alerting.core.model

import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith

class LocalUriInputTests {
    private var path = "/_cluster/health"
    private var pathParams = ""
    private var url = ""
    private var connectionTimeout = 5
    private var socketTimeout = 5

    @Test
    fun `test valid LocalUriInput creation using HTTP URI component fields`() {
        // GIVEN
        val testUrl = "http://localhost:9200/_cluster/health"

        // WHEN
        val localUriInput = LocalUriInput(path, pathParams, url, connectionTimeout, socketTimeout)

        // THEN
        assertEquals(path, localUriInput.path)
        assertEquals(pathParams, localUriInput.pathParams)
        assertEquals(testUrl, localUriInput.url)
        assertEquals(connectionTimeout, localUriInput.connectionTimeout)
        assertEquals(socketTimeout, localUriInput.socketTimeout)
    }

    @Test
    fun `test valid LocalUriInput creation using HTTP url field`() {
        // GIVEN
        path = ""
        url = "http://localhost:9200/_cluster/health"

        // WHEN
        val localUriInput = LocalUriInput(path, pathParams, url, connectionTimeout, socketTimeout)

        // THEN
        assertEquals(url, localUriInput.url)
    }

    @Test
    fun `test valid LocalUriInput creation using HTTPS url field`() {
        // GIVEN
        path = ""
        url = "https://localhost:9200/_cluster/health"

        // WHEN
        val localUriInput = LocalUriInput(path, pathParams, url, connectionTimeout, socketTimeout)

        // THEN
        assertEquals(url, localUriInput.url)
    }

    @Test
    fun `test invalid path`() {
        // GIVEN
        path = "///"

        // WHEN + THEN
        assertFailsWith<IllegalArgumentException>("Invalid URL.") {
            LocalUriInput(path, pathParams, url, connectionTimeout, socketTimeout)
        }
    }

    @Test
    fun `test invalid connection timeout that's too low`() {
        // GIVEN
        connectionTimeout = LocalUriInput.MIN_CONNECTION_TIMEOUT - 1

        // WHEN + THEN
        assertFailsWith<IllegalArgumentException>(
            "Connection timeout: $connectionTimeout is not in the range of ${LocalUriInput.MIN_CONNECTION_TIMEOUT} - ${LocalUriInput.MIN_CONNECTION_TIMEOUT}."
        ) {
            LocalUriInput(path, pathParams, url, connectionTimeout, socketTimeout)
        }
    }

    @Test
    fun `test invalid connection timeout that's too high`() {
        // GIVEN
        connectionTimeout = LocalUriInput.MAX_CONNECTION_TIMEOUT + 1

        // WHEN + THEN
        assertFailsWith<IllegalArgumentException>(
            "Connection timeout: $connectionTimeout is not in the range of ${LocalUriInput.MIN_CONNECTION_TIMEOUT} - ${LocalUriInput.MIN_CONNECTION_TIMEOUT}."
        ) {
            LocalUriInput(path, pathParams, url, connectionTimeout, socketTimeout)
        }
    }

    @Test
    fun `test invalid socket timeout that's too low`() {
        // GIVEN
        socketTimeout = LocalUriInput.MIN_SOCKET_TIMEOUT - 1

        // WHEN + THEN
        assertFailsWith<IllegalArgumentException>(
            "Socket timeout: $socketTimeout is not in the range of ${LocalUriInput.MIN_SOCKET_TIMEOUT} - ${LocalUriInput.MAX_SOCKET_TIMEOUT}."
        ) {
            LocalUriInput(path, pathParams, url, connectionTimeout, socketTimeout)
        }
    }

    @Test
    fun `test invalid socket timeout that's too high`() {
        // GIVEN
        socketTimeout = LocalUriInput.MAX_SOCKET_TIMEOUT + 1

        // WHEN + THEN
        assertFailsWith<IllegalArgumentException>(
            "Socket timeout: $socketTimeout is not in the range of ${LocalUriInput.MIN_SOCKET_TIMEOUT} - ${LocalUriInput.MAX_SOCKET_TIMEOUT}."
        ) {
            LocalUriInput(path, pathParams, url, connectionTimeout, socketTimeout)
        }
    }

    @Test
    fun `test invalid url`() {
        // GIVEN
        url = "///"

        // WHEN + THEN
        assertFailsWith<IllegalArgumentException>("Invalid URL.") {
            LocalUriInput(path, pathParams, url, connectionTimeout, socketTimeout)
        }
    }

    @Test
    fun `test url field and URI component fields create equal URI`() {
        // GIVEN
        url = "http://localhost:9200/_cluster/health"

        // WHEN
        val localUriInput = LocalUriInput(path, pathParams, url, connectionTimeout, socketTimeout)

        // THEN
        assertEquals(path, localUriInput.path)
        assertEquals(pathParams, localUriInput.pathParams)
        assertEquals(url, localUriInput.url)
        assertEquals(connectionTimeout, localUriInput.connectionTimeout)
        assertEquals(socketTimeout, localUriInput.socketTimeout)
        assertEquals(url, localUriInput.constructedUri.toString())
    }

    @Test
    fun `test url field and URI component fields with path params create equal URI`() {
        // GIVEN
        path = "/_cluster/health/"
        pathParams = "index1,index2,index3,index4,index5"
        url = "http://localhost:9200/_cluster/health/index1,index2,index3,index4,index5"

        // WHEN
        val localUriInput = LocalUriInput(path, pathParams, url, connectionTimeout, socketTimeout)

        // THEN
        assertEquals(path, localUriInput.path)
        assertEquals(pathParams, localUriInput.pathParams)
        assertEquals(url, localUriInput.url)
        assertEquals(connectionTimeout, localUriInput.connectionTimeout)
        assertEquals(socketTimeout, localUriInput.socketTimeout)
        assertEquals(url, localUriInput.constructedUri.toString())
    }

    @Test
    fun `test url field and URI component fields create different URI`() {
        // GIVEN
        url = "http://localhost:9200/_cluster/stats"

        // WHEN + THEN
        assertFailsWith<IllegalArgumentException>("The provided URL and URI fields form different URLs.") {
            LocalUriInput(path, pathParams, url, connectionTimeout, socketTimeout)
        }
    }

    @Test
    fun `test url field and URI component fields with path params create different URI`() {
        // GIVEN
        pathParams = "index1,index2,index3,index4,index5"
        url = "http://localhost:9200/_cluster/stats/index1,index2,index3,index4,index5"

        // WHEN + THEN
        assertFailsWith<IllegalArgumentException>("The provided URL and URI fields form different URLs.") {
            LocalUriInput(path, pathParams, url, connectionTimeout, socketTimeout)
        }
    }

    @Test
    fun `test LocalUriInput creation when all inputs are empty`() {
        // GIVEN
        path = ""
        pathParams = ""
        url = ""

        // WHEN + THEN
        assertFailsWith<IllegalArgumentException>("The uri.api_type field, uri.path field, or uri.uri field must be defined.") {
            LocalUriInput(path, pathParams, url, connectionTimeout, socketTimeout)
        }
    }

    @Test
    fun `test LocalUriInput creation when all inputs but path params are empty`() {
        // GIVEN
        path = ""
        pathParams = "index1,index2,index3,index4,index5"
        url = ""

        // WHEN + THEN
        assertFailsWith<IllegalArgumentException>("The uri.api_type field, uri.path field, or uri.uri field must be defined.") {
            LocalUriInput(path, pathParams, url, connectionTimeout, socketTimeout)
        }
    }

    @Test
    fun `test invalid scheme in url field`() {
        // GIVEN
        path = ""
        url = "invalidScheme://localhost:9200/_cluster/health"

        // WHEN + THEN
        assertFailsWith<IllegalArgumentException>("Invalid URL.") {
            LocalUriInput(path, pathParams, url, connectionTimeout, socketTimeout)
        }
    }

    @Test
    fun `test invalid host in url field`() {
        // GIVEN
        path = ""
        url = "http://127.0.0.1:9200/_cluster/health"

        // WHEN + THEN
        assertFailsWith<IllegalArgumentException>("Only host '${LocalUriInput.SUPPORTED_HOST}' is supported.") {
            LocalUriInput(path, pathParams, url, connectionTimeout, socketTimeout)
        }
    }

    @Test
    fun `test invalid port in url field`() {
        // GIVEN
        path = ""
        url = "http://localhost:${LocalUriInput.SUPPORTED_PORT + 1}/_cluster/health"

        // WHEN + THEN
        assertFailsWith<IllegalArgumentException>("Only port '${LocalUriInput.SUPPORTED_PORT}' is supported.") {
            LocalUriInput(path, pathParams, url, connectionTimeout, socketTimeout)
        }
    }

    @Test
    fun `test parsePathParams with no path params`() {
        // GIVEN
        val testUrl = "http://localhost:9200/_cluster/health"
        val localUriInput = LocalUriInput(path, pathParams, url, connectionTimeout, socketTimeout)

        // WHEN
        val params = localUriInput.parsePathParams()

        // THEN
        assertEquals(pathParams, params)
        assertEquals(testUrl, localUriInput.constructedUri.toString())
    }

    @Test
    fun `test parsePathParams with path params as URI field`() {
        // GIVEN
        path = "/_cluster/health/"
        pathParams = "index1,index2,index3,index4,index5"
        val testUrl = "http://localhost:9200/_cluster/health/index1,index2,index3,index4,index5"
        val localUriInput = LocalUriInput(path, pathParams, url, connectionTimeout, socketTimeout)

        // WHEN
        val params = localUriInput.parsePathParams()

        // THEN
        assertEquals(pathParams, params)
        assertEquals(testUrl, localUriInput.constructedUri.toString())
    }

    @Test
    fun `test parsePathParams with path params in url`() {
        // GIVEN
        path = ""
        val testParams = "index1,index2,index3,index4,index5"
        url = "http://localhost:9200/_cluster/health/index1,index2,index3,index4,index5"
        val localUriInput = LocalUriInput(path, pathParams, url, connectionTimeout, socketTimeout)

        // WHEN
        val params = localUriInput.parsePathParams()

        // THEN
        assertEquals(testParams, params)
        assertEquals(url, localUriInput.constructedUri.toString())
    }

    @Test
    fun `test parsePathParams with no path params for ApiType that requires path params`() {
        // GIVEN
        path = "/_cat/snapshots"

        // WHEN + THEN
        assertFailsWith<IllegalArgumentException>("The API requires path parameters.") {
            LocalUriInput(path, pathParams, url, connectionTimeout, socketTimeout)
        }
    }

    @Test
    fun `test parsePathParams with path params for ApiType that doesn't support path params`() {
        // GIVEN
        path = "/_cluster/settings"
        pathParams = "index1,index2,index3,index4,index5"
        val localUriInput = LocalUriInput(path, pathParams, url, connectionTimeout, socketTimeout)

        // WHEN + THEN
        assertFailsWith<IllegalArgumentException>("The API does not use path parameters.") {
            localUriInput.parsePathParams()
        }
    }

    @Test
    fun `test parsePathParams with path params containing illegal characters`() {
        var testCount = 0 // Start off with count of 1 to account for ApiType.BLANK
        ILLEGAL_PATH_PARAMETER_CHARACTERS.forEach { character ->
            // GIVEN
            pathParams = "index1,index2,$character,index4,index5"
            val localUriInput = LocalUriInput(path, pathParams, url, connectionTimeout, socketTimeout)

            // WHEN + THEN
            assertFailsWith<IllegalArgumentException>(
                "The provided path parameters contain invalid characters or spaces. Please omit: ${ILLEGAL_PATH_PARAMETER_CHARACTERS.joinToString(" ")}"
            ) {
                localUriInput.parsePathParams()
            }
            testCount++
        }
        assertEquals(ILLEGAL_PATH_PARAMETER_CHARACTERS.size, testCount)
    }

    @Test
    fun `test LocalUriInput correctly determines ApiType when path is provided as URI component`() {
        var testCount = 1 // Start off with count of 1 to account for ApiType.BLANK
        LocalUriInput.ApiType.values()
            .filter { enum -> enum != LocalUriInput.ApiType.BLANK }
            .forEach { testApiType ->
                // GIVEN
                path = testApiType.defaultPath
                pathParams = if (testApiType.supportsPathParams) "index1,index2,index3,index4,index5" else ""

                // WHEN
                val localUriInput = LocalUriInput(path, pathParams, url, connectionTimeout, socketTimeout)

                // THEN
                assertEquals(testApiType, localUriInput.apiType)
                testCount++
            }
        assertEquals(LocalUriInput.ApiType.values().size, testCount)
    }

    @Test
    fun `test LocalUriInput correctly determines ApiType when path and path params are provided as URI components`() {
        var testCount = 1 // Start off with count of 1 to account for ApiType.BLANK
        LocalUriInput.ApiType.values()
            .filter { enum -> enum != LocalUriInput.ApiType.BLANK }
            .forEach { testApiType ->
                // GIVEN
                path = testApiType.defaultPath
                pathParams = "index1,index2,index3,index4,index5"

                // WHEN
                val localUriInput = LocalUriInput(path, pathParams, url, connectionTimeout, socketTimeout)

                // THEN
                assertEquals(testApiType, localUriInput.apiType)
                testCount++
            }
        assertEquals(LocalUriInput.ApiType.values().size, testCount)
    }

    @Test
    fun `test LocalUriInput correctly determines ApiType when path is provided in URL field`() {
        var testCount = 1 // Start off with count of 1 to account for ApiType.BLANK
        LocalUriInput.ApiType.values()
            .filter { enum -> enum != LocalUriInput.ApiType.BLANK }
            .forEach { testApiType ->
                // GIVEN
                path = ""
                pathParams = if (testApiType.supportsPathParams) "index1,index2,index3,index4,index5" else ""
                url = "http://localhost:9200${testApiType.defaultPath}"

                // WHEN
                val localUriInput = LocalUriInput(path, pathParams, url, connectionTimeout, socketTimeout)

                // THEN
                assertEquals(testApiType, localUriInput.apiType)
                testCount++
            }
        assertEquals(LocalUriInput.ApiType.values().size, testCount)
    }

    @Test
    fun `test LocalUriInput correctly determines ApiType when path and path params are provided in URL field`() {
        var testCount = 1 // Start off with count of 1 to account for ApiType.BLANK
        LocalUriInput.ApiType.values()
            .filter { enum -> enum != LocalUriInput.ApiType.BLANK }
            .forEach { testApiType ->
                // GIVEN
                path = ""
                pathParams = if (testApiType.supportsPathParams) "/index1,index2,index3,index4,index5" else ""
                url = "http://localhost:9200${testApiType.defaultPath}$pathParams"

                // WHEN
                val localUriInput = LocalUriInput(path, pathParams, url, connectionTimeout, socketTimeout)

                // THEN
                assertEquals(testApiType, localUriInput.apiType)
                testCount++
            }
        assertEquals(LocalUriInput.ApiType.values().size, testCount)
    }

    @Test
    fun `test LocalUriInput cannot determine ApiType when invalid path is provided as URI component`() {
        // GIVEN
        path = "/_cat/paws"

        // WHEN + THEN
        assertFailsWith<IllegalArgumentException>("The API could not be determined from the provided URI.") {
            LocalUriInput(path, pathParams, url, connectionTimeout, socketTimeout)
        }
    }

    @Test
    fun `test LocalUriInput cannot determine ApiType when invalid path and path params are provided as URI components`() {
        // GIVEN
        path = "/_cat/paws"
        pathParams = "index1,index2,index3,index4,index5"

        // WHEN + THEN
        assertFailsWith<IllegalArgumentException>("The API could not be determined from the provided URI.") {
            LocalUriInput(path, pathParams, url, connectionTimeout, socketTimeout)
        }
    }

    @Test
    fun `test LocalUriInput cannot determine ApiType when invaid path is provided in URL`() {
        // GIVEN
        path = ""
        url = "http://localhost:9200/_cat/paws"

        // WHEN + THEN
        assertFailsWith<IllegalArgumentException>("The API could not be determined from the provided URI.") {
            LocalUriInput(path, pathParams, url, connectionTimeout, socketTimeout)
        }
    }

    @Test
    fun `test LocalUriInput cannot determine ApiType when invaid path and path params are provided in URL`() {
        // GIVEN
        path = ""
        url = "http://localhost:9200/_cat/paws/index1,index2,index3,index4,index5"

        // WHEN + THEN
        assertFailsWith<IllegalArgumentException>("The API could not be determined from the provided URI.") {
            LocalUriInput(path, pathParams, url, connectionTimeout, socketTimeout)
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
        val localUriInput = LocalUriInput(path, pathParams, url, connectionTimeout, socketTimeout)

        // THEN
        assertEquals(testPath, localUriInput.path)
        assertEquals(testPathParams, localUriInput.pathParams)
        assertEquals(url, localUriInput.url)
    }

    @Test
    fun `test parseEmptyFields populates empty url field when path and path_params are provided`() {
        // GIVEN
        path = "/_cluster/health/"
        pathParams = "index1,index2,index3,index4,index5"
        val testUrl = "http://localhost:9200$path$pathParams"

        // WHEN
        val localUriInput = LocalUriInput(path, pathParams, url, connectionTimeout, socketTimeout)

        // THEN
        assertEquals(path, localUriInput.path)
        assertEquals(pathParams, localUriInput.pathParams)
        assertEquals(testUrl, localUriInput.url)
    }
}
