/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 * Modifications Copyright OpenSearch Contributors. See
 * GitHub history for details.
 */

/*
 *   Copyright 2020 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 *   Licensed under the Apache License, Version 2.0 (the "License").
 *   You may not use this file except in compliance with the License.
 *   A copy of the License is located at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *   or in the "license" file accompanying this file. This file is distributed
 *   on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 *   express or implied. See the License for the specific language governing
 *   permissions and limitations under the License.
 */

package org.opensearch.alerting.model.destination

import org.opensearch.alerting.model.AlertingConfigAccessor
import org.opensearch.alerting.model.destination.email.Email
import org.opensearch.alerting.model.destination.email.EmailAccount
import org.opensearch.alerting.model.destination.email.Recipient
import org.opensearch.alerting.settings.DestinationSettings.Companion.SecureDestinationSettings
import org.opensearch.alerting.util.DestinationType
import org.opensearch.client.Client
import org.opensearch.common.settings.SecureString
import org.opensearch.common.xcontent.NamedXContentRegistry

/**
 * This class is responsible for generating [DestinationContext].
 */
class DestinationContextFactory(
    val client: Client,
    val xContentRegistry: NamedXContentRegistry,
    private var destinationSettings: Map<String, SecureDestinationSettings>
) {

    fun updateDestinationSettings(destinationSettings: Map<String, SecureDestinationSettings>) {
        this.destinationSettings = destinationSettings
    }

    suspend fun getDestinationContext(destination: Destination): DestinationContext {
        var destinationContext = DestinationContext()
        // Populate DestinationContext based on Destination type
        if (destination.type == DestinationType.EMAIL) {
            val email = destination.email
            requireNotNull(email) { "Email in Destination: $destination was null" }

            var emailAccount = AlertingConfigAccessor.getEmailAccountInfo(client, xContentRegistry, email.emailAccountID)

            emailAccount = addEmailCredentials(emailAccount)

            // Get the email recipients as a unique list of email strings since
            // recipients can be a combination of EmailGroups and single emails
            val uniqueListOfRecipients = getUniqueListOfEmailRecipients(email)

            destinationContext = destinationContext.copy(emailAccount = emailAccount, recipients = uniqueListOfRecipients)
        }

        return destinationContext
    }

    private fun addEmailCredentials(emailAccount: EmailAccount): EmailAccount {
        // Retrieve and populate the EmailAccount object with credentials if authentication is enabled
        if (emailAccount.method != EmailAccount.MethodType.NONE) {
            val emailUsername: SecureString? = destinationSettings[emailAccount.name]?.emailUsername
            val emailPassword: SecureString? = destinationSettings[emailAccount.name]?.emailPassword

            return emailAccount.copy(username = emailUsername, password = emailPassword)
        }

        return emailAccount
    }

    private suspend fun getUniqueListOfEmailRecipients(email: Email): List<String> {
        val uniqueRecipients: MutableSet<String> = mutableSetOf()
        email.recipients.forEach { recipient ->
            when (recipient.type) {
                // Recipient attributes are checked for being non-null based on type during initialization
                // so non-null assertion calls are made here
                Recipient.RecipientType.EMAIL -> uniqueRecipients.add(recipient.email!!)
                Recipient.RecipientType.EMAIL_GROUP -> {
                    val emailGroup = AlertingConfigAccessor.getEmailGroupInfo(client, xContentRegistry, recipient.emailGroupID!!)
                    emailGroup.getEmailsAsListOfString().map { uniqueRecipients.add(it) }
                }
            }
        }

        return uniqueRecipients.toList()
    }
}
