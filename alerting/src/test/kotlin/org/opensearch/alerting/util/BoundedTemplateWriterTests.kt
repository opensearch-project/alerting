/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.alerting.util

import com.github.mustachejava.MustacheException
import org.opensearch.test.OpenSearchTestCase
import java.io.Writer

class BoundedTemplateWriterTests : OpenSearchTestCase() {

    fun `test overflowing chunk is rejected without consuming remaining capacity`() {
        val writer = BoundedTemplateWriter(5)
        writer.write("ab")

        val exception = expectThrows(MustacheException::class.java) { writer.write("cdef") }

        assertEquals("Rendered template exceeds the maximum output size of 5 UTF-16 code units", exception.message)
        assertEquals("ab", writer.toString())

        writer.write("cde")
        assertEquals("abcde", writer.toString())
    }

    fun `test every write and append overload respects the limit and requested slice`() {
        val operations = listOf<Pair<String, (Writer) -> Unit>>(
            "character" to { it.write('x'.code) },
            "character array" to { it.write(charArrayOf('x')) },
            "character array slice" to { it.write(charArrayOf('a', 'x', 'b'), 1, 1) },
            "string" to { it.write("x") },
            "string slice" to { it.write("axb", 1, 1) },
            "append character" to { it.append('x') },
            "append sequence" to { it.append("x") },
            "append sequence slice" to { it.append("axb", 1, 2) }
        )

        for ((name, operation) in operations) {
            val writer = BoundedTemplateWriter(1)
            operation(writer)
            assertEquals(name, "x", writer.toString())

            expectThrows(MustacheException::class.java) { operation(writer) }
            assertEquals(name, "x", writer.toString())
        }
    }

    fun `test invalid ranges do not consume the output budget`() {
        val writer = BoundedTemplateWriter(3)
        writer.write("a")
        val invalidRanges = listOf(
            -1 to 1,
            0 to -1,
            2 to 2,
            4 to 0,
            Int.MAX_VALUE to 1,
            1 to Int.MAX_VALUE
        )

        for ((offset, length) in invalidRanges) {
            expectThrows(IndexOutOfBoundsException::class.java) { writer.write("xyz", offset, length) }
            expectThrows(IndexOutOfBoundsException::class.java) { writer.write(charArrayOf('x', 'y', 'z'), offset, length) }
            assertEquals("a", writer.toString())
        }

        writer.write("bc")
        assertEquals("abc", writer.toString())
    }

    fun `test supplementary characters count as two UTF-16 code units`() {
        val supplementaryCharacter = "\uD83D\uDE00"
        val exactWriter = BoundedTemplateWriter(2)
        exactWriter.write(supplementaryCharacter)
        assertEquals(supplementaryCharacter, exactWriter.toString())
        expectThrows(MustacheException::class.java) { exactWriter.write("x") }

        val undersizedWriter = BoundedTemplateWriter(1)
        expectThrows(MustacheException::class.java) { undersizedWriter.write(supplementaryCharacter) }
        assertEquals("", undersizedWriter.toString())
    }

    fun `test zero limit permits empty writes and rejects output`() {
        val writer = BoundedTemplateWriter(0)
        writer.write("")
        writer.write(charArrayOf())
        writer.write("abc", 3, 0)
        writer.write(charArrayOf('a'), 1, 0)
        writer.append("")

        assertEquals("", writer.toString())
        expectThrows(MustacheException::class.java) { writer.write('x'.code) }
        expectThrows(IllegalArgumentException::class.java) { BoundedTemplateWriter(-1) }
    }

    fun `test flush and close preserve output and the consumed budget`() {
        val writer = BoundedTemplateWriter(1)
        writer.write("x")
        writer.flush()
        writer.close()

        assertEquals("x", writer.toString())
        expectThrows(MustacheException::class.java) { writer.write("y") }
        assertEquals("x", writer.toString())
    }
}
