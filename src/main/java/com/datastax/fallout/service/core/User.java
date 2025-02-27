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
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.function.Consumer;
import java.util.function.Function;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Strings;

import com.datastax.driver.mapping.annotations.Column;
import com.datastax.driver.mapping.annotations.Field;
import com.datastax.driver.mapping.annotations.PartitionKey;
import com.datastax.driver.mapping.annotations.Table;
import com.datastax.driver.mapping.annotations.UDT;
import com.datastax.fallout.cassandra.shaded.org.codehaus.jackson.annotate.JsonAutoDetect;
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
    private String credentialStoreKey;

    private CredentialSet credentialsSet = new CredentialSet();

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

    public String getCredentialStoreKey()
    {
        return credentialStoreKey;
    }

    public void setCredentialStoreKey(String key)
    {
        this.credentialStoreKey = key;
    }

    public void setCredentialsSet(CredentialSet credentialSet)
    {
        this.credentialsSet = credentialSet;
    }

    protected CredentialSet getCredentialsSet()
    {
        return credentialsSet;
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

    public boolean hasEc2Credentials()
    {
        return credentialsSet.ec2AccessKey != null && !credentialsSet.ec2AccessKey.isEmpty() &&
            credentialsSet.ec2SecretKey != null && !credentialsSet.ec2SecretKey.isEmpty();
    }

    public String getEc2AccessKey()
    {
        return credentialsSet.ec2AccessKey;
    }

    public void setEc2AccessKey(String ec2AccessKey)
    {
        credentialsSet.ec2AccessKey = ec2AccessKey;
    }

    public String getEc2SecretKey()
    {
        return credentialsSet.ec2SecretKey;
    }

    public void setEc2SecretKey(String ec2SecretKey)
    {
        credentialsSet.ec2SecretKey = ec2SecretKey;
    }

    public String getOpenstackUsername()
    {
        return credentialsSet.openstackUsername;
    }

    public void setOpenstackUsername(String openstackUsername)
    {
        credentialsSet.openstackUsername = openstackUsername;
    }

    public String getOpenstackPassword()
    {
        return credentialsSet.openstackPassword;
    }

    public void setOpenstackPassword(String openstackPassword)
    {
        credentialsSet.openstackPassword = openstackPassword;
    }

    public String getNebulaProjectName()
    {
        return credentialsSet.nebulaProjectName;
    }

    /**
     * @see CredentialSet#validateAndSetNebulaProjectName(String) use this method instead
     * @deprecated this should be used by the entity mapper, use validateAndSetNebulaProjectName instead
     */
    @Deprecated
    public void setNebulaProjectName(String nebulaProjectName)
    {
        credentialsSet.nebulaProjectName = nebulaProjectName;
    }

    public void validateAndSetNebulaProjectName(String nebulaProjectName)
    {
        setNebulaProjectName(nebulaProjectName);
        ensureNebulaAppCreds();
    }

    public Set<NebulaAppCred> getNebulaAppCreds()
    {
        return credentialsSet.nebulaAppCreds;
    }

    public void setNebulaAppCreds(Set<NebulaAppCred> nebulaAppCreds)
    {
        credentialsSet.nebulaAppCreds = nebulaAppCreds;
    }

    public synchronized void addNebulaAppCred(NebulaAppCred appCred)
    {
        if (credentialsSet.nebulaAppCreds.stream().anyMatch(existingCred -> appCred.id.equals(existingCred.id)))
        {
            throw new IllegalArgumentException(
                String.format("A Nebula application credential with ID (%s) already exists", appCred.id));
        }
        credentialsSet.nebulaAppCreds.add(appCred);
        ensureNebulaAppCreds();
    }

    public synchronized void dropNebulaAppCred(String appCredId)
    {
        credentialsSet.nebulaAppCreds.removeIf(appCred -> appCred.id.equals(appCredId));
        ensureNebulaAppCreds();
    }

    private void ensureNebulaAppCreds()
    {
        ensureDefaultCreds(credentialsSet.nebulaProjectName, credentialsSet.nebulaAppCreds, (cred) -> cred.project,
            this::setNebulaProjectName);
    }

    public String getIronicTenantName()
    {
        return credentialsSet.ironicTenantName;
    }

    public void setIronicTenantName(String ironicTenantName)
    {
        credentialsSet.ironicTenantName = ironicTenantName;
    }

    public String getDefaultGoogleCloudServiceAccountEmail()
    {
        return credentialsSet.defaultGoogleCloudServiceAccountEmail;
    }

    /* @deprecated this should be used by the entity mapper, use
     * {@link #validateAndSetDefaultGoogleCloudServiceAccount} instead
     */
    @Deprecated
    public void setDefaultGoogleCloudServiceAccountEmail(String accountEmail)
    {
        credentialsSet.defaultGoogleCloudServiceAccountEmail = accountEmail;
    }

    public void validateAndSetDefaultGoogleCloudServiceAccount(String accountEmail)
    {
        setDefaultGoogleCloudServiceAccountEmail(accountEmail);
        ensureDefaultGoogleCloudServiceAccount();
    }

    private void ensureDefaultGoogleCloudServiceAccount()
    {
        ensureDefaultCreds(credentialsSet.defaultGoogleCloudServiceAccountEmail, getGoogleCloudServiceAccounts(),
            sa -> sa.email, this::setDefaultGoogleCloudServiceAccountEmail);
    }

    public Set<GoogleCloudServiceAccount> getGoogleCloudServiceAccounts()
    {
        return credentialsSet.googleCloudServiceAccounts;
    }

    public void setGoogleCloudServiceAccounts(Set<GoogleCloudServiceAccount> googleCloudServiceAccounts)
    {
        credentialsSet.googleCloudServiceAccounts = googleCloudServiceAccounts;
    }

    public synchronized void addGoogleCloudServiceAccount(GoogleCloudServiceAccount googleCloudServiceAccount)
    {
        credentialsSet.googleCloudServiceAccounts.add(googleCloudServiceAccount);
        ensureDefaultGoogleCloudServiceAccount();
    }

    public synchronized void dropGoogleCloudServiceAccount(String email)
    {
        credentialsSet.googleCloudServiceAccounts.removeIf((sa) -> sa.email.equals(email));
        ensureDefaultGoogleCloudServiceAccount();
    }

    @JsonIgnore
    public GoogleCloudServiceAccount getGoogleCloudServiceAccount(Optional<String> serviceAccountEmail)
    {
        final String serviceAccountEmail_ = serviceAccountEmail
            .orElseGet(() -> Optional
                .ofNullable(credentialsSet.defaultGoogleCloudServiceAccountEmail)
                .orElseThrow(() -> new InvalidConfigurationException("No Google Service Account has been set")));

        return getGoogleCloudServiceAccounts().stream()
            .filter(serviceAccount_ -> serviceAccount_.email.equals(serviceAccountEmail_))
            .findFirst()
            .orElseThrow(() -> new InvalidConfigurationException(
                String.format("Found no credentials for %s Google Service Account", serviceAccountEmail_)));
    }

    public String getDefaultAstraServiceAccountName()
    {
        return credentialsSet.defaultAstraServiceAccountName;
    }

    public void setDefaultAstraServiceAccountName(String defaultAstraServiceAccountName)
    {
        credentialsSet.defaultAstraServiceAccountName = defaultAstraServiceAccountName;
    }

    public Set<AstraServiceAccount> getAstraServiceAccounts()
    {
        return credentialsSet.astraServiceAccounts;
    }

    public void setAstraServiceAccounts(Set<AstraServiceAccount> astraServiceAccounts)
    {
        credentialsSet.astraServiceAccounts = astraServiceAccounts;
    }

    public synchronized void addAstraCred(AstraServiceAccount astraServiceAccount)
    {
        astraServiceAccount.validate();
        credentialsSet.astraServiceAccounts.add(astraServiceAccount);
        ensureDefaultAstraServiceAccount();
    }

    public synchronized void dropAstraCred(String clientName)
    {
        credentialsSet.astraServiceAccounts.removeIf(credential -> credential.clientName.equals(clientName));
        ensureDefaultAstraServiceAccount();
    }

    public void validateAndSetDefaultAstraServiceAccount(String clientName)
    {
        setDefaultAstraServiceAccountName(clientName);
        ensureDefaultAstraServiceAccount();
    }

    public Optional<AstraServiceAccount> getAstraServiceAccount(String clientName)
    {
        Optional<AstraServiceAccount> account = credentialsSet.astraServiceAccounts.stream()
            .filter(cred -> cred.clientName.equals(clientName)).findFirst();
        account.ifPresent(AstraServiceAccount::validate);
        return account;
    }

    private void ensureDefaultAstraServiceAccount()
    {
        ensureDefaultCreds(credentialsSet.defaultAstraServiceAccountName, getAstraServiceAccounts(), c -> c.clientName,
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
        return credentialsSet.backupServiceCreds;
    }

    public void setBackupServiceCreds(Set<BackupServiceCred> backupServiceCreds)
    {
        credentialsSet.backupServiceCreds = backupServiceCreds;
    }

    public Optional<BackupServiceCred> getBackupServiceCred(String name)
    {
        if (credentialsSet.backupServiceCreds != null)
        {
            return credentialsSet.backupServiceCreds.stream()
                .filter(cred -> cred.name.equals(name))
                .findFirst();
        }
        return Optional.empty();
    }

    public String getDefaultBackupServiceCred()
    {
        return credentialsSet.defaultBackupServiceCred;
    }

    public void validateAndSetDefaultBackupServiceCred(String name)
    {
        credentialsSet.defaultBackupServiceCred = name;
        ensureDefaultBackupServiceCredentials();
    }

    private void ensureDefaultBackupServiceCredentials()
    {
        ensureDefaultCreds(
            credentialsSet.defaultBackupServiceCred, getBackupServiceCreds(), c -> c.name,
            cred -> credentialsSet.defaultBackupServiceCred = cred);
    }

    public synchronized void dropBackupServiceCred(String name)
    {
        credentialsSet.backupServiceCreds.removeIf(cred -> cred.name.equals(name));
        ensureDefaultBackupServiceCredentials();
    }

    public synchronized void addBackupServiceCred(BackupServiceCred cred)
    {
        credentialsSet.backupServiceCreds.add(cred);
        ensureDefaultBackupServiceCredentials();
    }

    public Set<DockerRegistryCredential> getDockerRegistryCredentials()
    {
        return credentialsSet.dockerRegistryCredentials;
    }

    public void setDockerRegistryCredentials(
        Set<DockerRegistryCredential> dockerRegistryCredentials)
    {
        credentialsSet.dockerRegistryCredentials = dockerRegistryCredentials;
    }

    public Optional<DockerRegistryCredential> getDockerRegistryCredential(String dockerRegistry)
    {
        return credentialsSet.dockerRegistryCredentials.stream()
            .filter(cred -> cred.dockerRegistry.equals(dockerRegistry))
            .findFirst();
    }

    public synchronized void dropDockerRegistryCredential(String dockerRegistry)
    {
        credentialsSet.dockerRegistryCredentials.removeIf(cred -> cred.dockerRegistry.equals(dockerRegistry));
    }

    public synchronized void addDockerRegistryCredential(DockerRegistryCredential cred)
    {
        credentialsSet.dockerRegistryCredentials.add(cred);
        ensureDefaultBackupServiceCredentials();
    }

    public Map<String, String> getGenericSecrets()
    {
        return credentialsSet.genericSecrets;
    }

    public void setGenericSecrets(Map<String, String> genericSecrets)
    {
        credentialsSet.genericSecrets = genericSecrets;
    }

    public synchronized void addGenericSecret(String secretName, String secret)
    {
        credentialsSet.genericSecrets.put(secretName, secret);
    }

    public synchronized void dropGenericSecret(String name)
    {
        credentialsSet.genericSecrets.remove(name);
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
            Objects.equals(resetToken, user.resetToken) &&
            emailPref == user.emailPref &&
            slackPref == user.slackPref &&
            group == user.group &&
            Objects.equals(oauthId, user.oauthId);
    }

    @Override
    public int hashCode()
    {
        return Objects
            .hash(email, name, encryptedPassword, salt, admin, automatonSharedHandle, publicSshKey, resetToken,
                emailPref, slackPref, group, oauthId);
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

        @Field(name = "s3_access")
        public String s3AccessKey;

        @Field(name = "s3_secret")
        public String s3SecretKey;

        public NebulaAppCred(String project, String id, String secret, String s3AccessKey, String s3SecretKey)
        {
            this.project = project;
            this.id = id;
            this.secret = secret;
            this.s3AccessKey = s3AccessKey;
            this.s3SecretKey = s3SecretKey;
        }

        public NebulaAppCred()
        {
            // needed for serialization
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

        public GoogleCloudServiceAccount(String email, String project, String privateKeyId, String keyFileJson)
        {
            this.email = email;
            this.project = project;
            this.privateKeyId = privateKeyId;
            this.keyFileJson = keyFileJson;
        }

        public GoogleCloudServiceAccount()
        {
            // needed for serialization
        }

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
        /**
         Just the name the user decided to give this account.
         It is not tied to clientId or clientSecret below in any way.
         We keep this suboptiomal field name for legacy reasons after the astra IAM changes.
         */
        @Field
        public String clientName;

        @Field
        public String env;

        @Field
        public String token;

        @Field
        public String clientId;

        @Field
        public String clientSecret;

        public AstraServiceAccount(String clientName, String env, String token)
        {
            this(clientName, env, token, "", "");
        }

        public AstraServiceAccount(String clientName, String env, String token, String clientId, String clientSecret)
        {
            this.clientName = clientName;
            this.env = env;
            this.token = token;
            this.clientId = clientId;
            this.clientSecret = clientSecret;
            validate();
        }

        public AstraServiceAccount()
        {
            // needed for serialization
        }

        public final void validate()
        {
            if (Strings.isNullOrEmpty(token))
            {
                throw new InvalidConfigurationException("Astra Account is missing token: " + token);
            }
            if (!Set.of("dev", "test", "prod").contains(env))
            {
                throw new InvalidConfigurationException("Astra Account has invalid env: " + env);
            }
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
            return Objects.equals(clientName, astraServiceAccount.clientName) &&
                Objects.equals(env, astraServiceAccount.env) &&
                Objects.equals(token, astraServiceAccount.token) &&
                Objects.equals(clientId, astraServiceAccount.clientId) &&
                Objects.equals(clientSecret, astraServiceAccount.clientSecret);
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(clientName, env, token, clientId, clientSecret);
        }

        @Override
        public String toString()
        {
            return "AstraServiceAccount{" +
                ", clientName=" + clientName +
                ", env=" + env +
                ", token=" + token +
                ", clientId=" + clientId +
                ", clientSecret=" + clientSecret +
                "}";
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
            // needed for serialization
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
            // needed for serialization
        }
    }

    @JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.ANY)
    protected static class CredentialSet
    {
        @JsonProperty
        private String ec2AccessKey;

        @JsonProperty
        private String ec2SecretKey;

        @JsonProperty
        private String openstackUsername;

        @JsonProperty
        private String openstackPassword;

        // TODO: delete + drop from db eventually
        @JsonProperty
        private String openstackTenantName;

        @JsonProperty
        private String nebulaProjectName;

        @JsonProperty
        private Set<NebulaAppCred> nebulaAppCreds = new HashSet<>();

        @JsonProperty
        private String defaultGoogleCloudServiceAccountEmail;

        @JsonProperty
        private Set<GoogleCloudServiceAccount> googleCloudServiceAccounts = new HashSet<>();

        @JsonProperty
        private String ironicTenantName;

        @JsonProperty
        private String defaultAstraServiceAccountName;

        @JsonProperty
        private Set<AstraServiceAccount> astraServiceAccounts = new HashSet<>();

        @JsonProperty
        private Set<BackupServiceCred> backupServiceCreds = new HashSet<>();

        @JsonProperty
        private String defaultBackupServiceCred;

        @JsonProperty
        private Set<DockerRegistryCredential> dockerRegistryCredentials = new HashSet<>();

        @JsonProperty
        private Map<String, String> genericSecrets = new HashMap<>();
    }
}
