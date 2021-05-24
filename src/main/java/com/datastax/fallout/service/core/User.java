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
package com.datastax.fallout.service.core;

import javax.validation.constraints.NotEmpty;
import javax.validation.constraints.Pattern;

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
import com.datastax.fallout.service.resources.server.AccountResource;
import com.datastax.fallout.service.resources.server.TestResource;
import com.datastax.fallout.util.JsonUtils;

@Table(name = "users")
public class User implements Principal
{
    @NotEmpty
    @Pattern(regexp = AccountResource.EMAIL_PATTERN)
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

    @Pattern(regexp = TestResource.NAME_PATTERN)
    @Column
    private String automatonSharedHandle;

    @Column
    private String publicSshKey;

    @Column
    private String ec2AccessKey;

    @Column
    private String ec2SecretKey;

    @Column
    private UUID resetToken;

    @Column
    private TestCompletionNotification emailPref = TestCompletionNotification.NONE;

    @Column
    private TestCompletionNotification slackPref = TestCompletionNotification.NONE;

    @Column
    private String group;

    @Column
    private UUID oauthId;

    @Column
    private String openstackUsername;

    @Column
    private String openstackPassword;

    // TODO: delete + drop from db eventually
    @Column
    private String openstackTenantName;

    @Column
    private String nebulaProjectName;

    @Column
    private Set<NebulaAppCred> nebulaAppCreds = new HashSet<>();

    @Column
    private String defaultGoogleCloudServiceAccountEmail;

    @Column
    private Set<GoogleCloudServiceAccount> googleCloudServiceAccounts = new HashSet<>();

    @Column
    private String ironicTenantName;

    @Column
    private String defaultAstraServiceAccountName;

    @Column
    private Set<AstraServiceAccount> astraServiceAccounts = new HashSet<>();

    @Column
    private Set<BackupServiceCred> backupServiceCreds = new HashSet<>();

    @Column
    private String defaultBackupServiceCred;

    @Column
    private Set<DockerRegistryCredential> dockerRegistryCredentials = new HashSet<>();

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

    public String getGroup()
    {
        return group;
    }

    public void setGroup(String group)
    {
        this.group = group;
    }

    public boolean isAdmin()
    {
        return admin;
    }

    public void setAdmin(boolean admin)
    {
        this.admin = admin;
    }

    public String getAutomatonSharedHandle()
    {
        return automatonSharedHandle;
    }

    private String defaultAutomatonSharedHandle()
    {
        // Create a valid automatonSharedHandle from an input email by stripping the domain and
        // replacing invalid input characters with valid ones
        final var emailSplit = email.split("@");
        return (emailSplit.length > 0 ? emailSplit[0] : email)
            .replaceAll("[^" + TestResource.ALLOWED_NAME_CHARS + "]", "_");
    }

    public String getOrGenerateAutomatonSharedHandle()
    {
        // NB: we do not lazy-init this field, because if we did and wrote this instance
        // back to the database the effect would be to freeze the effect of the code in the persisted
        // instance; we want the default value to be dynamic, rather than static.
        return Objects.requireNonNullElseGet(automatonSharedHandle, this::defaultAutomatonSharedHandle);
    }

    /** We shouldn't put validation on the raw set method, since the C* driver mapper needs to use
     *  that to deserialize from the DB, and if there's validation on it we can't deserialize invalid
     *  fields to fix them. */
    public void throwOrSetValidAutomatonSharedHandle(String automatonSharedHandle)
    {
        if (automatonSharedHandle != null && !automatonSharedHandle.isEmpty() &&
            !automatonSharedHandle.equals(defaultAutomatonSharedHandle()))
        {
            if (!TestResource.namePattern.matcher(automatonSharedHandle).matches())
            {
                throw new IllegalArgumentException(
                    String.format("Automaton shared handle '%s' should match the regex: %s",
                        automatonSharedHandle, TestResource.NAME_PATTERN));
            }
            this.automatonSharedHandle = automatonSharedHandle;
        }
        else
        {
            this.automatonSharedHandle = null;
        }
    }

    public String getPublicSshKey()
    {
        return publicSshKey;
    }

    public void setPublicSshKey(String publicSshKey)
    {
        this.publicSshKey = publicSshKey;
    }

    public String getEc2AccessKey()
    {
        return ec2AccessKey;
    }

    public void setEc2AccessKey(String ec2AccessKey)
    {
        this.ec2AccessKey = ec2AccessKey;
    }

    public String getEc2SecretKey()
    {
        return ec2SecretKey;
    }

    public void setEc2SecretKey(String ec2SecretKey)
    {
        this.ec2SecretKey = ec2SecretKey;
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

    public String getOpenstackUsername()
    {
        return openstackUsername;
    }

    public void setOpenstackUsername(String openstackUsername)
    {
        this.openstackUsername = openstackUsername;
    }

    public String getOpenstackPassword()
    {
        return openstackPassword;
    }

    public void setOpenstackPassword(String openstackPassword)
    {
        this.openstackPassword = openstackPassword;
    }

    public String getNebulaProjectName()
    {
        return nebulaProjectName;
    }

    /**
     * @see User#validateAndSetNebulaProjectName(String) use this method instead
     * @deprecated this should be used by the entity mapper, use validateAndSetNebulaProjectName instead
     */
    @Deprecated
    public void setNebulaProjectName(String nebulaProjectName)
    {
        this.nebulaProjectName = nebulaProjectName;
    }

    public void validateAndSetNebulaProjectName(String nebulaProjectName)
    {
        setNebulaProjectName(nebulaProjectName);
        ensureNebulaAppCreds();
    }

    public Set<NebulaAppCred> getNebulaAppCreds()
    {
        return nebulaAppCreds;
    }

    public void setNebulaAppCreds(Set<NebulaAppCred> nebulaAppCreds)
    {
        this.nebulaAppCreds = nebulaAppCreds;
    }

    public synchronized void addNebulaAppCred(NebulaAppCred appCred)
    {
        nebulaAppCreds.add(appCred);
        ensureNebulaAppCreds();
    }

    public synchronized void dropNebulaAppCred(String appCredId)
    {
        nebulaAppCreds.removeIf(appCred -> appCred.id.equals(appCredId));
        ensureNebulaAppCreds();
    }

    private void ensureNebulaAppCreds()
    {
        ensureDefaultCreds(nebulaProjectName, nebulaAppCreds, (cred) -> cred.project,
            this::setNebulaProjectName);
    }

    public String getIronicTenantName()
    {
        return ironicTenantName;
    }

    public void setIronicTenantName(String ironicTenantName)
    {
        this.ironicTenantName = ironicTenantName;
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

    public String getDefaultAstraServiceAccountName()
    {
        return defaultAstraServiceAccountName;
    }

    public void setDefaultAstraServiceAccountName(String defaultAstraServiceAccountName)
    {
        this.defaultAstraServiceAccountName = defaultAstraServiceAccountName;
    }

    public Set<AstraServiceAccount> getAstraServiceAccounts()
    {
        return astraServiceAccounts;
    }

    public void setAstraServiceAccounts(Set<AstraServiceAccount> astraServiceAccounts)
    {
        this.astraServiceAccounts = astraServiceAccounts;
    }

    public synchronized void addAstraCred(AstraServiceAccount astraServiceAccount)
    {
        astraServiceAccounts.add(astraServiceAccount);
        ensureDefaultAstraServiceAccount();
    }

    public synchronized void dropAstraCred(String clientName)
    {
        astraServiceAccounts.removeIf(credential -> credential.clientName.equals(clientName));
        ensureDefaultAstraServiceAccount();
    }

    public void validateAndSetDefaultAstraServiceAccount(String clientName)
    {
        setDefaultAstraServiceAccountName(clientName);
        ensureDefaultAstraServiceAccount();
    }

    public Optional<AstraServiceAccount> getAstraServiceAccount(String clientName)
    {
        return astraServiceAccounts.stream().filter(cred -> cred.clientName.equals(clientName)).findFirst();
    }

    private void ensureDefaultAstraServiceAccount()
    {
        ensureDefaultCreds(defaultAstraServiceAccountName, getAstraServiceAccounts(), c -> c.clientName,
            this::setDefaultAstraServiceAccountName);
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

    public Set<BackupServiceCred> getBackupServiceCreds()
    {
        return backupServiceCreds;
    }

    public Optional<BackupServiceCred> getBackupServiceCred(String name)
    {
        return getBackupServiceCreds().stream()
            .filter(cred -> cred.name.equals(name))
            .findFirst();
    }

    public String getDefaultBackupServiceCred()
    {
        return defaultBackupServiceCred;
    }

    public void validateAndSetDefaultBackupServiceCred(String name)
    {
        this.defaultBackupServiceCred = name;
        ensureDefaultBackupServiceCredentials();
    }

    private void ensureDefaultBackupServiceCredentials()
    {
        ensureDefaultCreds(
            defaultBackupServiceCred, getBackupServiceCreds(), c -> c.name, cred -> defaultBackupServiceCred = cred);
    }

    public synchronized void dropBackupServiceCred(String name)
    {
        backupServiceCreds.removeIf(cred -> cred.name.equals(name));
        ensureDefaultBackupServiceCredentials();
    }

    public synchronized void addBackupServiceCred(BackupServiceCred cred)
    {
        backupServiceCreds.add(cred);
        ensureDefaultBackupServiceCredentials();
    }

    public Set<DockerRegistryCredential> getDockerRegistryCredentials()
    {
        return dockerRegistryCredentials;
    }

    public void setDockerRegistryCredentials(
        Set<DockerRegistryCredential> dockerRegistryCredentials)
    {
        this.dockerRegistryCredentials = dockerRegistryCredentials;
    }

    public Optional<DockerRegistryCredential> getDockerRegistryCredential(String dockerRegistry)
    {
        return dockerRegistryCredentials.stream()
            .filter(cred -> cred.dockerRegistry.equals(dockerRegistry))
            .findFirst();
    }

    public synchronized void dropDockerRegistryCredential(String dockerRegistry)
    {
        dockerRegistryCredentials.removeIf(cred -> cred.dockerRegistry.equals(dockerRegistry));
    }

    public synchronized void addDockerRegistryCredential(DockerRegistryCredential cred)
    {
        dockerRegistryCredentials.add(cred);
        ensureDefaultBackupServiceCredentials();
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
            Objects.equals(automatonSharedHandle, user.automatonSharedHandle) &&
            Objects.equals(publicSshKey, user.publicSshKey) &&
            Objects.equals(ec2AccessKey, user.ec2AccessKey) &&
            Objects.equals(ec2SecretKey, user.ec2SecretKey) &&
            Objects.equals(resetToken, user.resetToken) &&
            emailPref == user.emailPref &&
            slackPref == user.slackPref &&
            group == user.group &&
            Objects.equals(oauthId, user.oauthId) &&
            Objects.equals(openstackUsername, user.openstackUsername) &&
            Objects.equals(openstackPassword, user.openstackPassword) &&
            Objects.equals(nebulaProjectName, user.nebulaProjectName) &&
            Objects.equals(nebulaAppCreds, user.nebulaAppCreds) &&
            Objects.equals(ironicTenantName, user.ironicTenantName) &&
            Objects.equals(defaultGoogleCloudServiceAccountEmail, user.defaultGoogleCloudServiceAccountEmail) &&
            Objects.equals(googleCloudServiceAccounts, user.googleCloudServiceAccounts) &&
            Objects.equals(defaultAstraServiceAccountName, user.defaultAstraServiceAccountName) &&
            Objects.equals(astraServiceAccounts, user.astraServiceAccounts) &&
            Objects.equals(backupServiceCreds, user.backupServiceCreds) &&
            Objects.equals(defaultBackupServiceCred, user.defaultBackupServiceCred);
    }

    @Override
    public int hashCode()
    {
        return Objects
            .hash(email, name, encryptedPassword, salt, admin, automatonSharedHandle, publicSshKey, ec2AccessKey,
                ec2SecretKey, resetToken, emailPref, slackPref, group,
                oauthId, openstackUsername, openstackPassword,
                nebulaProjectName, nebulaAppCreds, ironicTenantName, defaultGoogleCloudServiceAccountEmail,
                googleCloudServiceAccounts, defaultAstraServiceAccountName, astraServiceAccounts, backupServiceCreds,
                defaultBackupServiceCred);
    }

    @Override
    public String toString()
    {
        return "User{" +
            "email='" + email + "'" +
            ", emailPref=" + emailPref +
            ", slackPref=" + slackPref +
            ", group=" + group +
            ", admin=" + isAdmin() +
            ", automatonSharedHandle='" + automatonSharedHandle + "'" +
            ", openstackUsername='" + openstackUsername + "'" +
            ", ironicTenantName='" + ironicTenantName + "'" +
            ", nebulaProjectName='" + nebulaProjectName + "'" +
            ", nebulaAppCreds='" + nebulaAppCreds + "'" +
            ", defaultGoogleCloudServiceAccount='" + defaultGoogleCloudServiceAccountEmail + "'" +
            ", googleCloudServiceAccounts='" + googleCloudServiceAccounts + "'" +
            ", defaultAstraServiceAccountId='" + defaultAstraServiceAccountName + "'" +
            ", astraServiceAccounts='" + astraServiceAccounts + "'" +
            ", s3fsCreds='" + backupServiceCreds + "'" +
            ", defaultS3fsCred='" + defaultBackupServiceCred + "'" +
            '}';
    }

    @UDT(name = "nebulaAppCred")
    public static class NebulaAppCred
    {
        @Field(name = "project")
        public String project;

        @Field(name = "id")
        public String id;

        @Field(name = "secret")
        public String secret;

        public NebulaAppCred(String project, String id, String secret)
        {
            this.project = project;
            this.id = id;
            this.secret = secret;
        }

        public NebulaAppCred()
        {
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
            NebulaAppCred that = (NebulaAppCred) o;
            return project.equals(that.project) &&
                id.equals(that.id) &&
                secret.equals(that.secret);
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(project, id, secret);
        }

        @Override
        public String toString()
        {
            return "NebulaAppCred{" +
                "project=" + project +
                ", id=" + id +
                '}';
        }
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
        public String keyFileJson;

        public static GoogleCloudServiceAccount fromJson(String keyFileJson)
        {
            final GoogleCloudServiceAccount googleCloudServiceAccount =
                JsonUtils.fromJson(keyFileJson, GoogleCloudServiceAccount.class);
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

    @UDT(name = "astraServiceAccount")
    public static class AstraServiceAccount
    {
        @Field
        public String clientId;

        @Field
        public String clientName;

        @Field
        public String clientSecret;

        public AstraServiceAccount(String clientId, String clientName, String clientSecret)
        {
            this.clientId = clientId;
            this.clientName = clientName;
            this.clientSecret = clientSecret;
        }

        public AstraServiceAccount()
        {
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
            AstraServiceAccount astraServiceAccount = (AstraServiceAccount) o;
            return Objects.equals(clientId, astraServiceAccount.clientId) &&
                Objects.equals(clientName, astraServiceAccount.clientName) &&
                Objects.equals(clientSecret, astraServiceAccount.clientSecret);
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(clientId, clientName, clientSecret);
        }

        @Override
        public String toString()
        {
            return "AstraServiceAccount{" +
                "clientId='" + clientId + '\'' +
                ", clientName='" + clientName + '\'' +
                ", clientSecret='" + clientSecret + '\'' +
                '}';
        }
    }

    @UDT(name = "BackupServiceCred")
    public static class BackupServiceCred
    {
        @Field
        public String name;

        @Field
        public String s3AccessKey;

        @Field
        public String s3SecretKey;

        public BackupServiceCred(String name, String s3AccessKey, String s3SecretKey)
        {
            this.name = name;
            this.s3AccessKey = s3AccessKey;
            this.s3SecretKey = s3SecretKey;
        }

        public BackupServiceCred()
        {
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
            BackupServiceCred that = (BackupServiceCred) o;
            return Objects.equals(name, that.name) &&
                Objects.equals(s3AccessKey, that.s3AccessKey) &&
                Objects.equals(s3SecretKey, that.s3SecretKey);
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(name, s3AccessKey, s3SecretKey);
        }

        @Override
        public String toString()
        {
            return "BackupServiceCred{" +
                "name='" + name + '\'' +
                ", s3AccessKey='" + s3AccessKey + '\'' +
                ", s3SecretKey='" + s3SecretKey + '\'' +
                '}';
        }
    }

    @UDT(name = "dockerRegistryCredential")
    public static class DockerRegistryCredential
    {
        @Field
        public String dockerRegistry;

        @Field
        public String username;

        @Field
        public String password;

        public DockerRegistryCredential(String dockerServer, String username, String password)
        {
            this.dockerRegistry = dockerServer;
            this.username = username;
            this.password = password;
        }

        public DockerRegistryCredential()
        {
        }
    }
}
