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
package com.datastax.fallout.service.core;

import java.nio.ByteBuffer;
import java.security.Principal;
import java.util.HashSet;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.function.Consumer;
import java.util.function.Function;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

import com.datastax.driver.mapping.annotations.Column;
import com.datastax.driver.mapping.annotations.Field;
import com.datastax.driver.mapping.annotations.PartitionKey;
import com.datastax.driver.mapping.annotations.Table;
import com.datastax.driver.mapping.annotations.UDT;
import com.datastax.fallout.exceptions.InvalidConfigurationException;
import com.datastax.fallout.ops.Utils;

@Table(name = "users")
public class User implements Principal
{
    @PartitionKey()
    private String email;

    @Column
    private String name;

    @Column(name = "encpass")
    private ByteBuffer encryptedPassword;

    @Column
    private ByteBuffer salt;

    @Column
    private boolean admin;

    @Column
    private UUID resetToken;

    @Column
    private TestCompletionNotification emailPref = TestCompletionNotification.NONE;

    @Column
    private TestCompletionNotification slackPref = TestCompletionNotification.NONE;

    @Column
    private UUID oauthId;

    @Column
    private String defaultGoogleCloudServiceAccountEmail;

    @Column
    private Set<GoogleCloudServiceAccount> googleCloudServiceAccounts = new HashSet<>();

    public String getEmail()
    {
        return email;
    }

    public void setEmail(String email)
    {
        this.email = email;
    }

    public String getName()
    {
        return name;
    }

    public void setName(String name)
    {
        this.name = name;
    }

    public ByteBuffer getEncryptedPassword()
    {
        return encryptedPassword;
    }

    public void setEncryptedPassword(ByteBuffer encryptedPassword)
    {
        this.encryptedPassword = encryptedPassword;
    }

    public ByteBuffer getSalt()
    {
        return salt;
    }

    public void setSalt(ByteBuffer salt)
    {
        this.salt = salt;
    }

    public boolean isAdmin()
    {
        return admin;
    }

    public TestCompletionNotification getEmailPref()
    {
        return emailPref;
    }

    public void setEmailPref(TestCompletionNotification emailPref)
    {
        this.emailPref = emailPref;
    }

    public TestCompletionNotification getSlackPref()
    {
        return slackPref;
    }

    public void setSlackPref(TestCompletionNotification slackPref)
    {
        this.slackPref = slackPref;
    }

    public UUID getOauthId()
    {
        return oauthId;
    }

    public void setOauthId(UUID oauthId)
    {
        this.oauthId = oauthId;
    }

    public UUID getResetToken()
    {
        return resetToken;
    }

    public void setResetToken(UUID resetToken)
    {
        this.resetToken = resetToken;
    }

    public void setAdmin(boolean admin)
    {
        this.admin = admin;
    }

    public String getDefaultGoogleCloudServiceAccountEmail()
    {
        return defaultGoogleCloudServiceAccountEmail;
    }

    /* @deprecated this should be used by the entity mapper, use
     * {@link #validateAndSetDefaultGoogleCloudServiceAccount} instead
     */
    @Deprecated
    public void setDefaultGoogleCloudServiceAccountEmail(String accountEmail)
    {
        defaultGoogleCloudServiceAccountEmail = accountEmail;
    }

    public void validateAndSetDefaultGoogleCloudServiceAccount(String accountEmail)
    {
        setDefaultGoogleCloudServiceAccountEmail(accountEmail);
        ensureDefaultGoogleCloudServiceAccount();
    }

    private void ensureDefaultGoogleCloudServiceAccount()
    {
        ensureDefaultCreds(defaultGoogleCloudServiceAccountEmail, getGoogleCloudServiceAccounts(),
            sa -> sa.email, this::setDefaultGoogleCloudServiceAccountEmail);
    }

    public Set<GoogleCloudServiceAccount> getGoogleCloudServiceAccounts()
    {
        return googleCloudServiceAccounts;
    }

    public void setGoogleCloudServiceAccounts(Set<GoogleCloudServiceAccount> googleCloudServiceAccounts)
    {
        this.googleCloudServiceAccounts = googleCloudServiceAccounts;
    }

    public synchronized void addGoogleCloudServiceAccount(GoogleCloudServiceAccount googleCloudServiceAccount)
    {
        googleCloudServiceAccounts.add(googleCloudServiceAccount);
        ensureDefaultGoogleCloudServiceAccount();
    }

    public synchronized void dropGoogleCloudServiceAccount(String email)
    {
        googleCloudServiceAccounts.removeIf((sa) -> sa.email.equals(email));
        ensureDefaultGoogleCloudServiceAccount();
    }

    @JsonIgnore
    public GoogleCloudServiceAccount getGoogleCloudServiceAccount(Optional<String> serviceAccountEmail)
    {
        final String serviceAccountEmail_ = serviceAccountEmail
            .orElseGet(() -> Optional
                .ofNullable(defaultGoogleCloudServiceAccountEmail)
                .orElseThrow(() -> new InvalidConfigurationException("No Google Service Account has been set")));

        return getGoogleCloudServiceAccounts().stream()
            .filter(serviceAccount_ -> serviceAccount_.email.equals(serviceAccountEmail_))
            .findFirst()
            .orElseThrow(() -> new InvalidConfigurationException(
                String.format("Found no credentials for %s Google Service Account", serviceAccountEmail_)));
    }

    private <T> void ensureDefaultCreds(String currentDefaultId, Set<T> creds, Function<T, String> getId,
        Consumer<String> setDefault)
    {
        if (currentDefaultId != null)
        {
            if (creds.stream().map(getId).anyMatch(id -> id.equals(currentDefaultId)))
            {
                return;
            }
        }
        Optional<String> newDefault = creds.stream().map(getId).findFirst();
        setDefault.accept(newDefault.orElse(null));
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o)
        {
            return true;
        }
        if (o == null || getClass() != o.getClass())
        {
            return false;
        }
        User user = (User) o;
        return admin == user.admin &&
            Objects.equals(email, user.email) &&
            Objects.equals(name, user.name) &&
            Objects.equals(encryptedPassword, user.encryptedPassword) &&
            Objects.equals(salt, user.salt) &&
            Objects.equals(resetToken, user.resetToken) &&
            emailPref == user.emailPref &&
            slackPref == user.slackPref &&
            Objects.equals(oauthId, user.oauthId) &&
            Objects.equals(defaultGoogleCloudServiceAccountEmail, user.defaultGoogleCloudServiceAccountEmail) &&
            Objects.equals(googleCloudServiceAccounts, user.googleCloudServiceAccounts);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(email, name, encryptedPassword, salt, admin, resetToken, emailPref, slackPref, oauthId,
            defaultGoogleCloudServiceAccountEmail, googleCloudServiceAccounts);
    }

    @Override
    public String toString()
    {
        return "User{" +
            "email='" + email + '\'' +
            ", name='" + name + '\'' +
            ", salt=" + salt +
            ", admin=" + admin +
            ", resetToken=" + resetToken +
            ", emailPref=" + emailPref +
            ", slackPref=" + slackPref +
            ", oauthId=" + oauthId +
            ", defaultGoogleCloudServiceAccountEmail='" + defaultGoogleCloudServiceAccountEmail + '\'' +
            ", googleCloudServiceAccounts=" + googleCloudServiceAccounts +
            '}';
    }

    @UDT(name = "googleCloudServiceAccount")
    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class GoogleCloudServiceAccount
    {
        @Field
        @JsonProperty(value = "client_email", required = true)
        public String email;

        @Field
        @JsonProperty(value = "project_id", required = true)
        public String project;

        @Field
        @JsonProperty(value = "private_key_id", required = true)
        public String privateKeyId;

        @Field
        @JsonIgnore
        public String keyFileJson;

        public static GoogleCloudServiceAccount fromJson(String keyFileJson)
        {
            final GoogleCloudServiceAccount googleCloudServiceAccount =
                Utils.fromJson(keyFileJson, GoogleCloudServiceAccount.class);
            googleCloudServiceAccount.keyFileJson = keyFileJson;
            return googleCloudServiceAccount;
        }

        @Override
        public String toString()
        {
            return "GoogleCloudServiceAccount{" +
                "email='" + email + '\'' +
                ", project='" + project + '\'' +
                ", privateKeyId='" + privateKeyId + '\'' +
                '}';
        }

        @Override
        public boolean equals(Object o)
        {
            if (this == o)
            {
                return true;
            }
            if (o == null || getClass() != o.getClass())
            {
                return false;
            }
            GoogleCloudServiceAccount that = (GoogleCloudServiceAccount) o;
            return Objects.equals(email, that.email);
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(email);
        }
    }
}
