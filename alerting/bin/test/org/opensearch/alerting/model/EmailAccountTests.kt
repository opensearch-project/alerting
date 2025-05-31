/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.alerting.model

import org.opensearch.alerting.model.destination.email.EmailAccount
import org.opensearch.test.OpenSearchTestCase

class EmailAccountTests : OpenSearchTestCase() {

    fun `test email account`() {
        val emailAccount = EmailAccount(
            name = "test",
            email = "test@email.com",
            host = "smtp.com",
            port = 25,
            method = EmailAccount.MethodType.NONE,
            username = null,
            password = null
        )
        assertEquals("Email account name was changed", emailAccount.name, "test")
        assertEquals("Email account email was changed", emailAccount.email, "test@email.com")
        assertEquals("Email account host was changed", emailAccount.host, "smtp.com")
        assertEquals("Email account port was changed", emailAccount.port, 25)
        assertEquals("Email account method was changed", emailAccount.method, EmailAccount.MethodType.NONE)
    }

    fun `test email account with invalid name`() {
        try {
            EmailAccount(
                name = "invalid-name",
                email = "test@email.com",
                host = "smtp.com",
                port = 25,
                method = EmailAccount.MethodType.NONE,
                username = null,
                password = null
            )
            fail("Creating an email account with an invalid name did not fail.")
        } catch (ignored: IllegalArgumentException) {
        }
    }

    fun `test email account with invalid email`() {
        try {
            EmailAccount(
                name = "test",
                email = "test@.com",
                host = "smtp.com",
                port = 25,
                method = EmailAccount.MethodType.NONE,
                username = null,
                password = null
            )
            fail("Creating an email account with an invalid email did not fail.")
        } catch (ignored: IllegalArgumentException) {
        }
    }
}
