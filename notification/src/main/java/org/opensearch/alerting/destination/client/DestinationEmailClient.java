/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.alerting.destination.client;

import org.opensearch.alerting.destination.message.BaseMessage;
import org.opensearch.alerting.destination.message.EmailMessage;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.alerting.destination.message.BaseMessage;
import org.opensearch.alerting.destination.message.EmailMessage;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import javax.mail.Authenticator;
import javax.mail.internet.InternetAddress;
import javax.mail.internet.MimeMessage;
import javax.mail.Message;
import javax.mail.MessagingException;
import javax.mail.PasswordAuthentication;
import javax.mail.Session;
import javax.mail.Transport;

/**
 * This class handles the connections to the given Destination.
 */
public class DestinationEmailClient {

    private static final Logger logger = LogManager.getLogger(DestinationEmailClient.class);
    
    public String execute(BaseMessage message) throws Exception {
        if (message instanceof EmailMessage) {
            EmailMessage emailMessage = (EmailMessage) message;
            Session session = null;

            Properties prop = new Properties();
            prop.put("mail.transport.protocol", "smtp");
            prop.put("mail.smtp.host", emailMessage.getHost());
            prop.put("mail.smtp.port", emailMessage.getPort());

            if (emailMessage.getUsername() != null && !emailMessage.getUsername().equals("".toCharArray())) {
                prop.put("mail.smtp.auth", true);
                try {
                    session = Session.getInstance(prop, new Authenticator() {
	                    protected PasswordAuthentication getPasswordAuthentication() {
	                        return new PasswordAuthentication(
	                                emailMessage.getUsername().toString(),
                                    emailMessage.getPassword().toString());
                        }
		            });
                } catch (IllegalStateException e) {
                    return e.getMessage();
                }
            } else {
                session = Session.getInstance(prop);
            }

            switch(emailMessage.getMethod()) {
            case "ssl":
                prop.put("mail.smtp.ssl.enable", true);
                break;
            case "starttls":
                prop.put("mail.smtp.starttls.enable", true);
                break;
            }

            try {
                Message mailmsg = new MimeMessage(session);
                mailmsg.setFrom(new InternetAddress(emailMessage.getFrom()));
                mailmsg.setRecipients(Message.RecipientType.TO, getRecipientsAsAddresses(emailMessage.getRecipients()));
                mailmsg.setSubject(emailMessage.getSubject());
                mailmsg.setText(emailMessage.getMessageContent());

                SendMessage(mailmsg);
            } catch (MessagingException e) {
                throw new MessagingException(e.getMessage());
            }
        }
        return "Sent";
    }

    /*
     * This method is useful for mocking the client
     */
    public void SendMessage(Message msg) throws Exception {
        Transport.send(msg);
    }

    private InternetAddress[] getRecipientsAsAddresses(List<String> recipients) throws Exception {
        ArrayList<InternetAddress> addresses = new ArrayList<>();
        for (String recipient : recipients) {
            addresses.add(new InternetAddress(recipient));
        }

        return addresses.toArray(new InternetAddress[0]);
    }
}
