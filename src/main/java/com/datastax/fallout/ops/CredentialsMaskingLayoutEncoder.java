/*
 * Copyright 2021 DataStax, Inc.
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
package com.datastax.fallout.ops;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

import ch.qos.logback.classic.LoggerContext;
import ch.qos.logback.classic.PatternLayout;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.encoder.LayoutWrappingEncoder;

import com.datastax.fallout.service.core.User;

import static com.datastax.fallout.ops.JobFileLoggers.FALLOUT_PATTERN;
import static com.datastax.fallout.runner.UserCredentialsFactory.UserCredentials;

public class CredentialsMaskingLayoutEncoder extends LayoutWrappingEncoder<ILoggingEvent>
{
    public CredentialsMaskingLayoutEncoder(LoggerContext loggerContext, UserCredentials userCredentials)
    {
        CredentialsMaskingPatternLayout credsMaskingLayout = new CredentialsMaskingPatternLayout(userCredentials);
        credsMaskingLayout.setContext(loggerContext);
        credsMaskingLayout.setPattern(FALLOUT_PATTERN);
        credsMaskingLayout.setOutputPatternAsHeader(false); // default carried over from PatternLayoutEncoder
        credsMaskingLayout.start();

        setLayout(credsMaskingLayout);
    }

    public static class CredentialsMaskingPatternLayout extends PatternLayout
    {
        private final List<String> secrets;

        public CredentialsMaskingPatternLayout(UserCredentials userCredentials)
        {
            this.secrets = getSecretsFromUser(userCredentials.owner);
            userCredentials.runAs.ifPresent(runAs_ -> secrets.addAll(getSecretsFromUser(runAs_)));
        }

        /**
         * Extracts secrets from a User.
         *
         * GoogleServiceAccount has been omitted as the secret is the entire json key file and is used via a temporary file.
         */
        private static ArrayList<String> getSecretsFromUser(User user)
        {
            ArrayList<String> secrets = new ArrayList<>();
            user.getAstraServiceAccounts().forEach(sa -> secrets.add(sa.clientSecret));
            user.getBackupServiceCreds().forEach(bsc -> secrets.add(bsc.s3SecretKey));
            user.getDockerRegistryCredentials().forEach(drc -> secrets.add(drc.password));
            user.getNebulaAppCreds().forEach(nac -> secrets.add(nac.secret));
            secrets.add(user.getEc2SecretKey());
            secrets.add(user.getOpenstackPassword());

            // Not all users have all secrets.
            return secrets.stream()
                .filter(Objects::nonNull)
                .filter(s -> !s.isEmpty())
                .collect(Collectors.toCollection(ArrayList::new));
        }

        @Override
        public String doLayout(ILoggingEvent event)
        {
            String logLine = super.doLayout(event);
            return secrets.stream().reduce(logLine, (line, secret) -> line.replaceAll(secret, "<redacted>"));
        }
    }
}
