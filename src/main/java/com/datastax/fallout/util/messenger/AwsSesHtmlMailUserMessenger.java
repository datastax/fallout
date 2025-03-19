/*
 * Copyright 2022 DataStax, Inc.
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
package com.datastax.fallout.util.messenger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.ses.SesClient;
import software.amazon.awssdk.services.ses.model.Body;
import software.amazon.awssdk.services.ses.model.Content;
import software.amazon.awssdk.services.ses.model.Destination;
import software.amazon.awssdk.services.ses.model.Message;
import software.amazon.awssdk.services.ses.model.SendEmailRequest;
import software.amazon.awssdk.services.ses.model.SesException;

import com.datastax.fallout.service.FalloutConfiguration;

public class AwsSesHtmlMailUserMessenger implements UserMessenger
{
    private static final Logger log = LoggerFactory.getLogger(AwsSesHtmlMailUserMessenger.class);

    private final SesClient sesClient;
    private final FalloutConfiguration conf;

    AwsSesHtmlMailUserMessenger(FalloutConfiguration conf)
    {
        this.conf = conf;
        this.sesClient = SesClient.builder().region(Region.US_WEST_2).build();
    }

    @Override
    public void sendMessage(String email, String subject, String body) throws MessengerException
    {
        Destination destination = Destination.builder().toAddresses(email).build();
        String sender = conf.getSmtpFrom();
        Content content = Content.builder().data(body).build();
        Content sub = Content.builder().data(subject).build();
        Body htmlContentBody = Body.builder().html(content).build();
        Message msg = Message.builder()
            .subject(sub)
            .body(htmlContentBody)
            .build();
        SendEmailRequest emailRequest = SendEmailRequest.builder()
            .destination(destination)
            .message(msg)
            .source(sender)
            .build();

        try
        {
            log.info("Attempting to send an email through Amazon SES using the AWS SDK for Java from sender " + sender);
            sesClient.sendEmail(emailRequest);
            log.info("email was sent from " + sender + " to " + email);
        }
        catch (SesException e)
        {
            throw new MessengerException(
                String.format("SesException while sending email to %s from", email, sender, e));
        }
    }
}
