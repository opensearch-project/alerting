/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.alerting.util.destinationmigration

import org.apache.http.client.utils.URIBuilder
import org.opensearch.alerting.model.destination.Destination
import org.opensearch.alerting.model.destination.email.EmailAccount
import org.opensearch.alerting.model.destination.email.Recipient
import org.opensearch.alerting.util.DestinationType
import org.opensearch.common.Strings
import org.opensearch.commons.notifications.model.Chime
import org.opensearch.commons.notifications.model.ConfigType
import org.opensearch.commons.notifications.model.Email
import org.opensearch.commons.notifications.model.EmailGroup
import org.opensearch.commons.notifications.model.EmailRecipient
import org.opensearch.commons.notifications.model.HttpMethodType
import org.opensearch.commons.notifications.model.MethodType
import org.opensearch.commons.notifications.model.NotificationConfig
import org.opensearch.commons.notifications.model.Slack
import org.opensearch.commons.notifications.model.SmtpAccount
import org.opensearch.commons.notifications.model.Webhook
import java.net.URI
import java.net.URISyntaxException
import java.util.Locale

class DestinationConversionUtils {

    companion object {

        fun convertDestinationToNotificationConfig(destination: Destination): NotificationConfig? {
            when (destination.type) {
                DestinationType.CHIME -> {
                    val alertChime = destination.chime ?: return null
                    val chime = Chime(alertChime.url)
                    val description = "Chime destination created from the Alerting plugin"
                    return NotificationConfig(
                        destination.name,
                        description,
                        ConfigType.CHIME,
                        chime
                    )
                }
                DestinationType.SLACK -> {
                    val alertSlack = destination.slack ?: return null
                    val slack = Slack(alertSlack.url)
                    val description = "Slack destination created from the Alerting plugin"
                    return NotificationConfig(
                        destination.name,
                        description,
                        ConfigType.SLACK,
                        slack
                    )
                }
                // TODO: Add this back after adding SNS to Destination data models
//                DestinationType.SNS -> {
//                    val alertSNS = destination.sns ?: return null
//                    val sns = Sns(alertSNS.topicARN, alertSNS.roleARN)
//                    val description = "SNS destination created from the Alerting plugin"
//                    return NotificationConfig(
//                        destination.name,
//                        description,
//                        ConfigType.SNS,
//                        sns
//                    )
//                }
                DestinationType.CUSTOM_WEBHOOK -> {
                    val alertWebhook = destination.customWebhook ?: return null
                    val uri = buildUri(
                        alertWebhook.url,
                        alertWebhook.scheme,
                        alertWebhook.host,
                        alertWebhook.port,
                        alertWebhook.path,
                        alertWebhook.queryParams
                    ).toString()
                    val methodType = when (alertWebhook.method?.uppercase(Locale.ENGLISH)) {
                        "POST" -> HttpMethodType.POST
                        "PUT" -> HttpMethodType.PUT
                        "PATCH" -> HttpMethodType.PATCH
                        else -> HttpMethodType.POST
                    }
                    val webhook = Webhook(uri, alertWebhook.headerParams, methodType)
                    val description = "Webhook destination created from the Alerting plugin"
                    return NotificationConfig(
                        destination.name,
                        description,
                        ConfigType.WEBHOOK,
                        webhook
                    )
                }
                DestinationType.EMAIL -> {
                    val alertEmail = destination.email ?: return null
                    val recipients = mutableListOf<EmailRecipient>()
                    val emailGroupIds = mutableListOf<String>()
                    alertEmail.recipients.forEach {
                        if (it.type == Recipient.RecipientType.EMAIL_GROUP)
                            it.emailGroupID?.let { emailGroup -> emailGroupIds.add(emailGroup) }
                        else it.email?.let { emailRecipient -> recipients.add(EmailRecipient(emailRecipient)) }
                    }

                    val email = Email(alertEmail.emailAccountID, recipients, emailGroupIds)
                    val description = "Email destination created from the Alerting plugin"
                    return NotificationConfig(
                        destination.name,
                        description,
                        ConfigType.EMAIL,
                        email
                    )
                }
                else -> return null
            }
        }

        fun convertEmailAccountToNotificationConfig(emailAccount: EmailAccount): NotificationConfig {
            val methodType = convertAlertingToNotificationMethodType(emailAccount.method)
            val smtpAccount = SmtpAccount(emailAccount.host, emailAccount.port, methodType, emailAccount.email)
            val description = "Email account created from the Alerting plugin"
            return NotificationConfig(
                emailAccount.name,
                description,
                ConfigType.SMTP_ACCOUNT,
                smtpAccount
            )
        }

        fun convertEmailGroupToNotificationConfig(
            emailGroup: org.opensearch.alerting.model.destination.email.EmailGroup
        ): NotificationConfig {
            val recipients = mutableListOf<EmailRecipient>()
            emailGroup.emails.forEach {
                recipients.add(EmailRecipient(it.email))
            }
            val notificationEmailGroup = EmailGroup(recipients)

            val description = "Email group created from the Alerting plugin"
            return NotificationConfig(
                emailGroup.name,
                description,
                ConfigType.EMAIL_GROUP,
                notificationEmailGroup
            )
        }

        private fun buildUri(
            endpoint: String?,
            scheme: String?,
            host: String?,
            port: Int,
            path: String?,
            queryParams: Map<String, String>
        ): URI? {
            return try {
                if (Strings.isNullOrEmpty(endpoint)) {
                    if (host == null) {
                        throw IllegalStateException("No host was provided when endpoint was null")
                    }
                    var uriScheme = scheme
                    if (Strings.isNullOrEmpty(scheme)) {
                        uriScheme = "https"
                    }
                    val uriBuilder = URIBuilder()
                    if (queryParams.isNotEmpty()) {
                        for ((key, value) in queryParams) uriBuilder.addParameter(key, value)
                    }
                    return uriBuilder.setScheme(uriScheme).setHost(host).setPort(port).setPath(path).build()
                }
                URIBuilder(endpoint).build()
            } catch (e: URISyntaxException) {
                throw IllegalStateException("Error creating URI", e)
            }
        }

        private fun convertAlertingToNotificationMethodType(alertMethodType: EmailAccount.MethodType): MethodType {
            return when (alertMethodType) {
                EmailAccount.MethodType.NONE -> MethodType.NONE
                EmailAccount.MethodType.SSL -> MethodType.SSL
                EmailAccount.MethodType.TLS -> MethodType.START_TLS
            }
        }
    }
}
