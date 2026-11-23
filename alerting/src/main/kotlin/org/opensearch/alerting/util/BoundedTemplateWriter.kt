/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.alerting.util

import com.github.mustachejava.MustacheException
import java.io.Writer
import java.util.Objects

/** Bounds accumulated template output in UTF-16 code units, checking before each write. */
internal class BoundedTemplateWriter(private val maxChars: Int) : Writer() {
    private val output = StringBuilder()

    init {
        require(maxChars >= 0) { "Template output limit must not be negative" }
    }

    override fun write(c: Int) {
        checkCapacity(1)
        output.append(c.toChar())
    }

    override fun write(cbuf: CharArray, off: Int, len: Int) {
        Objects.checkFromIndexSize(off, len, cbuf.size)
        checkCapacity(len)
        output.append(cbuf, off, len)
    }

    override fun write(str: String, off: Int, len: Int) {
        Objects.checkFromIndexSize(off, len, str.length)
        checkCapacity(len)
        output.append(str, off, off + len)
    }

    private fun checkCapacity(length: Int) {
        if (length > maxChars - output.length) {
            throw MustacheException("Rendered template exceeds the maximum output size of $maxChars UTF-16 code units")
        }
    }

    override fun flush() {}

    override fun close() {}

    override fun toString(): String = output.toString()
}
