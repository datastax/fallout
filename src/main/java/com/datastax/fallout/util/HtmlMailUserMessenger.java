/*
 * Copyright 2020 DataStax, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datastax.fallout.util;

import javax.mail.Message;
import javax.mail.MessagingException;
import javax.mail.Multipart;
import javax.mail.PasswordAuthentication;
import javax.mail.Transport;
import javax.mail.internet.InternetAddress;
import javax.mail.internet.MimeBodyPart;
import javax.mail.internet.MimeMessage;
import javax.mail.internet.MimeMultipart;

import java.util.Date;
import java.util.Properties;

import com.google.common.base.Charsets;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.fallout.service.FalloutConfiguration;

public class HtmlMailUserMessenger implements UserMessenger
{
    private static final Logger log = LoggerFactory.getLogger(HtmlMailUserMessenger.class);
    private final FalloutConfiguration configuration;

    public static UserMessenger create(FalloutConfiguration conf)
    {
        if (conf.getSmtpHost() != null && conf.getSmtpPass() != null && conf.getSmtpPort() != null &&
            conf.getSmtpUser() != null)
        {
            return new HtmlMailUserMessenger(conf);
        }
        else
        {
            log.warn("Fallout config missing mail setup");
            return new NullUserMessenger();
        }
    }

    private HtmlMailUserMessenger(FalloutConfiguration configuration)
    {
        this.configuration = configuration;
    }

    @Override
    public void sendMessage(String emailAddr, String subject, String body) throws MessengerException
    {
        Properties props = new Properties();
        props.put("mail.smtp.host", configuration.getSmtpHost());
        props.put("mail.smtp.port", Integer.toString(configuration.getSmtpPort()));
        props.put("mail.smtp.auth", "true");

        if (configuration.getSmtpPort() != 25)
        {
            props.put("mail.smtp.socketFactory.port", Integer.toString(configuration.getSmtpPort()));
            props.put("mail.smtp.socketFactory.class", "javax.net.ssl.SSLSocketFactory");
        }

        // Note that this logs to console, and there appears to be no way to change that
        if (log.isDebugEnabled())
        {
            props.put("mail.debug", "true");
        }

        SMTPAuthenticator auth = new SMTPAuthenticator(configuration.getSmtpUser(), configuration.getSmtpPass());
        javax.mail.Session session = javax.mail.Session.getInstance(props, auth);
        String emailFrom = "donotreply@fallout-host.org";
        MimeMessage mimeMsg = new MimeMessage(session);

        try
        {
            mimeMsg.setFrom(new InternetAddress(emailFrom));
            mimeMsg.setSubject(subject, Charsets.UTF_8.toString());

            InternetAddress[] recipients = InternetAddress.parse(emailAddr, true);
            mimeMsg.setRecipients(Message.RecipientType.TO, recipients);

            MimeBodyPart part = new MimeBodyPart();
            part.setText(body, Charsets.UTF_8.toString(), "html");

            Multipart mp = new MimeMultipart();
            mp.addBodyPart(part);

            mimeMsg.setSentDate(new Date());
            mimeMsg.setContent(mp);

            Transport.send(mimeMsg);
        }
        catch (MessagingException e)
        {
            throw new MessengerException("Failed to send mail", e);
        }
    }

    private class SMTPAuthenticator extends javax.mail.Authenticator
    {

        final String user;
        final String pass;

        public SMTPAuthenticator(String user, String pass)
        {
            this.user = user;
            this.pass = pass;
        }

        public PasswordAuthentication getPasswordAuthentication()
        {
            return new PasswordAuthentication(user, pass);
        }
    }
}
