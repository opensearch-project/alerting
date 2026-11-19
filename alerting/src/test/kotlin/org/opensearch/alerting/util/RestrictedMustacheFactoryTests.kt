/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.alerting.util

import com.github.mustachejava.MustacheException
import org.opensearch.test.OpenSearchTestCase
import java.io.CharArrayWriter
import java.io.IOException
import java.io.Writer

class RestrictedMustacheFactoryTests : OpenSearchTestCase() {

    fun `test encoding writes JSON string content without enclosing quotes`() {
        val writer = CharArrayWriter()

        RestrictedMustacheFactory().encode("\"quoted\"\\path\n<>&", writer)

        assertArrayEquals("\\\"quoted\\\"\\\\path\\n<>&".toCharArray(), writer.toCharArray())
    }

    fun `test encoding wraps writer failures and preserves the cause`() {
        val failure = IOException("Simulated writer failure")
        val writer = object : Writer() {
            override fun write(cbuf: CharArray, off: Int, len: Int) {
                throw failure
            }

            override fun flush() {}

            override fun close() {}
        }

        val exception = expectThrows(MustacheException::class.java) {
            RestrictedMustacheFactory().encode("value", writer)
        }

        assertEquals("Unable to encode template value", exception.message)
        assertSame(failure, exception.cause)
    }
}
