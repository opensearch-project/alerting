/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.alerting.util

import com.fasterxml.jackson.core.io.JsonStringEncoder
import com.github.mustachejava.DefaultMustacheFactory
import com.github.mustachejava.MustacheException
import java.io.IOException
import java.io.Reader
import java.io.Writer

/** Renders self-contained inline templates with JSON string escaping. */
internal class RestrictedMustacheFactory : DefaultMustacheFactory() {

    // Partials (including dynamic partials and inheritance) must never reach a resource resolver.
    override fun getReader(resourceName: String): Reader {
        throw MustacheException("Partial templates are not supported in Alerting")
    }

    override fun encode(value: String, writer: Writer) {
        try {
            writer.write(JsonStringEncoder.getInstance().quoteAsString(value))
        } catch (e: IOException) {
            throw MustacheException("Unable to encode template value", e)
        }
    }
}
