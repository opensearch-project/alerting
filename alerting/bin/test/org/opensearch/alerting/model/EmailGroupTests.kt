/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.alerting.model

import org.opensearch.alerting.model.destination.email.EmailEntry
import org.opensearch.alerting.model.destination.email.EmailGroup
import org.opensearch.test.OpenSearchTestCase

class EmailGroupTests : OpenSearchTestCase() {

    fun `test email group`() {
        val emailGroup = EmailGroup(
            name = "test",
            emails = listOf(EmailEntry("test@email.com"))
        )
        assertEquals("Email group name was changed", emailGroup.name, "test")
        assertEquals("Email group emails count was changed", emailGroup.emails.size, 1)
        assertEquals("Email group email entry was changed", emailGroup.emails[0].email, "test@email.com")
    }

    fun `test email group get emails as list of string`() {
        val emailGroup = EmailGroup(
            name = "test",
            emails = listOf(
                EmailEntry("test@email.com"),
                EmailEntry("test2@email.com")
            )
        )

        assertEquals(
            "List of email strings does not match email entries",
            listOf("test@email.com", "test2@email.com"), emailGroup.getEmailsAsListOfString()
        )
    }

    fun `test email group with invalid name fails`() {
        try {
            EmailGroup(
                name = "invalid name",
                emails = listOf(EmailEntry("test@email.com"))
            )
            fail("Creating an email group with an invalid name did not fail.")
        } catch (ignored: IllegalArgumentException) {
        }
    }

    fun `test email group with invalid email fails`() {
        try {
            EmailGroup(
                name = "test",
                emails = listOf(EmailEntry("invalid.com"))
            )
            fail("Creating an email group with an invalid email did not fail.")
        } catch (ignored: IllegalArgumentException) {
        }
    }
}
