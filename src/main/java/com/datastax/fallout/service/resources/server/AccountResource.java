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
package com.datastax.fallout.service.resources.server;

import javax.validation.constraints.NotEmpty;
import javax.ws.rs.Consumes;
import javax.ws.rs.CookieParam;
import javax.ws.rs.DELETE;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.FormParam;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.NewCookie;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriBuilder;

import java.net.URI;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import com.codahale.metrics.annotation.Timed;
import com.google.common.base.Preconditions;
import io.dropwizard.auth.Auth;
import org.apache.commons.lang3.tuple.Pair;

import com.datastax.fallout.service.FalloutConfiguration;
import com.datastax.fallout.service.FalloutService;
import com.datastax.fallout.service.auth.SecurityUtil;
import com.datastax.fallout.service.core.Session;
import com.datastax.fallout.service.core.TestCompletionNotification;
import com.datastax.fallout.service.core.User;
import com.datastax.fallout.service.db.UserDAO;
import com.datastax.fallout.service.db.UserGroupMapper;
import com.datastax.fallout.service.views.FalloutView;
import com.datastax.fallout.service.views.MainView;
import com.datastax.fallout.util.ScopedLogger;
import com.datastax.fallout.util.UserMessenger;

@Path("/account")
@Produces(MediaType.APPLICATION_JSON)
public class AccountResource
{
    private static final ScopedLogger logger = ScopedLogger.getLogger(AccountResource.class);

    public static final String EMAIL_PATTERN = "[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\\.[a-zA-Z]{2,}";

    private final UserDAO userDAO;
    private final FalloutConfiguration configuration;
    private final UserMessenger mailer;
    private final MainView mainView;
    private final SecurityUtil securityUtil;
    private final UserGroupMapper userGroupMapper;

    public AccountResource(UserDAO userDAO, FalloutConfiguration configuration, UserMessenger mailer,
        MainView mainView, SecurityUtil securityUtil, UserGroupMapper userGroupMapper)
    {
        this.userDAO = userDAO;
        this.configuration = configuration;
        this.mailer = mailer;
        this.mainView = mainView;
        this.securityUtil = securityUtil;
        this.userGroupMapper = userGroupMapper;
    }

    @GET
    @Path("/users/api")
    @Timed
    @Produces(MediaType.APPLICATION_JSON)
    public Response getAllUsers(@Auth User user)
    {
        List<Map<String, String>> users = userDAO.getAllUsers();
        return Response.accepted(users).build();
    }

    @GET
    @Path("/logout")
    @Timed
    @Produces(MediaType.APPLICATION_JSON)
    public Response doLogout(@CookieParam(FalloutService.COOKIE_NAME) String sessionId)
    {
        try
        {
            userDAO.dropSession(sessionId);
        }
        catch (Exception e)
        {
            logger.error("UserDAO.dropSession failed", e);
            throw new WebApplicationException(e.getMessage());
        }

        URI uri = UriBuilder.fromUri("/a/pages/login.html").build();

        return Response.seeOther(uri)
            .cookie(new NewCookie(FalloutService.COOKIE_NAME, null, "/", null, null, 0 /* maxAge */, false))
            .build();
    }

    @POST
    @Path("/register")
    @Timed
    @Produces(MediaType.APPLICATION_JSON)
    public Response doRegistration(@FormParam("name") @NotEmpty String name, @FormParam("email") @NotEmpty String email,
        @FormParam("password") @NotEmpty String password, @FormParam("group") String group)
    {
        validateEmail(email);

        /** Special logic for fallout in production **/
        if (configuration.isDatastaxOnly())
        {
            if (!email.toLowerCase().endsWith("@datastax.com"))
            {
                throw new WebApplicationException("Only DataStax employees can register, Sorry!",
                    Response.Status.BAD_REQUEST);
            }

            if (configuration.getIsSharedEndpoint())
            {
                //Force users to recover their password
                if (configuration.isDatastaxOnly() && configuration.getIsSharedEndpoint())
                {
                    password = UUID.randomUUID().toString();
                }
            }
        }

        User existingUser = userDAO.getUser(email);
        if (existingUser != null && existingUser.getSalt() != null)
        {
            throw new WebApplicationException("Email already registered", Response.Status.BAD_REQUEST);
        }

        Session session;

        try
        {
            var user = userDAO.createUserIfNotExists(name, email, password, userGroupMapper.validGroupOrOther(group));
            session = userDAO.addSession(user);
        }
        catch (Exception e)
        {
            logger.error("UserDAO registration failed", e);
            throw new WebApplicationException(e.getMessage());
        }

        int expires = 60 * 60 * 24 * 14; // 2 weeks

        //Login too
        return Response.ok()
            .cookie(new NewCookie(FalloutService.COOKIE_NAME, session.getTokenId().toString(),
                "/", null, null, expires, false))
            .build();
    }

    @POST
    @Path("/login")
    @Timed
    @Produces(MediaType.APPLICATION_JSON)
    public Response doLogin(@FormParam("email") @NotEmpty String email,
        @FormParam("password") @NotEmpty String password,
        @FormParam("remember") @DefaultValue("false") String rememberStr)
    {
        validateEmail(email);
        User existingUser = userDAO.getUser(email);

        boolean badCreds = existingUser == null || existingUser.getSalt() == null;

        try
        {
            badCreds = badCreds ||
                !securityUtil.authenticate(password, existingUser.getEncryptedPassword(), existingUser.getSalt());
        }
        catch (Exception e)
        {
            logger.error("Error creating user", e);
            throw new WebApplicationException(e.getMessage());
        }

        if (badCreds)
        {
            throw new WebApplicationException("Bad Email/Password", Response.Status.BAD_REQUEST);
        }

        Session session = userDAO.addSession(existingUser);

        int expires = rememberStr.equals("false") ? -1 : 60 * 60 * 24 * 14; // 2 weeks

        return Response.ok()
            .cookie(new NewCookie(FalloutService.COOKIE_NAME, session.getTokenId().toString(), "/", null, null, expires,
                false))
            .build();
    }

    @POST
    @Path("/reset")
    @Timed
    @Produces(MediaType.APPLICATION_JSON)
    public Response doReset(@FormParam("email") @NotEmpty String email, @FormParam("token") @NotEmpty String token,
        @FormParam("pass") @NotEmpty String password)
    {
        validateEmail(email);
        User existingUser = userDAO.getUser(email);

        if (existingUser == null || !token.equalsIgnoreCase(existingUser.getResetToken().toString()))
        {
            throw new WebApplicationException("Invalid reset meta", 422);
        }

        Preconditions.checkNotNull(existingUser.getSalt());

        existingUser.setEncryptedPassword(securityUtil.getEncryptedPassword(password, existingUser.getSalt()));
        existingUser.setResetToken(null);

        userDAO.updateUserCredentials(existingUser);

        return Response.ok().build();
    }

    @POST
    @Path("/lost")
    @Timed
    @Produces(MediaType.APPLICATION_JSON)
    public Response doLost(@FormParam("email") @NotEmpty String email)
    {
        validateEmail(email);
        User existingUser = userDAO.getUser(email);

        if (existingUser != null)
        {
            userDAO.addResetToken(existingUser);

            String resetUrl = String.format("%s/a/pages/reset.html?token=%s&email=%s", configuration.getExternalUrl(),
                existingUser.getResetToken(), email);
            try
            {
                String emailBody = "<html><body>" +
                    "Hi, <br/> We heard you are having trouble logging into Fallout." +
                    "<br/><br/> You can reset your fallout password with the following link: " +
                    "<a href=\"" + resetUrl + "\">" + resetUrl + "</a>" +
                    "</body></html>";
                mailer.sendMessage(email, "Fallout password reset", emailBody);
            }
            catch (UserMessenger.MessengerException e)
            {
                logger.warn("Failed to send password email", e);
                throw new WebApplicationException("Error sending email, let someone know");
            }
        }

        return Response.ok().build();
    }

    @GET
    @Timed
    @Produces(MediaType.TEXT_HTML)
    public Object getProfile(@Auth User user)
    {
        return new AccountView(configuration, user);
    }

    @POST
    @Path("/profile/nebula_app_creds")
    @Timed
    @Produces(MediaType.APPLICATION_JSON)
    public Response addNebulaAppCred(@Auth User user, @FormParam("projectName") @NotEmpty String projectName,
        @FormParam("appCredsId") @NotEmpty String appCredsId,
        @FormParam("appCredsSecret") @NotEmpty String appCredsSecret)
    {
        try
        {
            User.NebulaAppCred appCred = new User.NebulaAppCred(projectName, appCredsId, appCredsSecret);
            user.addNebulaAppCred(appCred);
            userDAO.updateUserCredentials(user);

            return Response.ok().build();
        }
        catch (IllegalArgumentException e)
        {
            return Response.status(Response.Status.BAD_REQUEST).entity(e.getMessage()).build();
        }
    }

    @POST
    @Path("/profile/nebula_app_creds_default")
    @Timed
    @Consumes(MediaType.APPLICATION_JSON)
    public Response setDefaultNebulaAppCred(@Auth User user, Map<String, String> json)
    {
        String projectName = getNonNullNonEmpty(json, "projectName");

        user.validateAndSetNebulaProjectName(projectName);
        userDAO.updateUserCredentials(user);

        return Response
            .ok()
            .entity("{\"default_nebula_project_name\":\"" + user.getNebulaProjectName() + "\"}")
            .build();
    }

    @DELETE
    @Path("/profile/nebula_app_creds")
    @Timed
    @Consumes(MediaType.APPLICATION_JSON)
    public Response dropNebulaAppCred(@Auth User user, Map<String, String> json)
    {
        String appCredsId = getNonNullNonEmpty(json, "appCredsId");

        user.dropNebulaAppCred(appCredsId);
        userDAO.updateUserCredentials(user);

        return Response.ok().build();
    }

    @POST
    @Path("/profile/google_cloud_service_account")
    @Timed
    @Produces(MediaType.APPLICATION_JSON)
    public Response addGoogleCloudServiceAccount(@Auth User user,
        @FormParam("googleCloudSAKeyFile") @NotEmpty String keyFileJson)
    {
        User.GoogleCloudServiceAccount serviceAccount = User.GoogleCloudServiceAccount.fromJson(keyFileJson);
        user.addGoogleCloudServiceAccount(serviceAccount);
        userDAO.updateUserCredentials(user);

        return Response.ok().build();
    }

    private static class GoogleCloudServiceAccountEmail
    {
        public String email;
    }

    @POST
    @Path("/profile/google_cloud_service_account_default")
    @Timed
    @Consumes(MediaType.APPLICATION_JSON)
    public Response setDefaultGoogleCloudServiceAccount(@Auth User user, GoogleCloudServiceAccountEmail accountEmail)
    {
        user.validateAndSetDefaultGoogleCloudServiceAccount(accountEmail.email);
        userDAO.updateUserCredentials(user);

        return Response
            .ok()
            .entity("{\"default_google_cloud_service_account_email\":\"" +
                user.getDefaultGoogleCloudServiceAccountEmail() + "\"}")
            .build();
    }

    @DELETE
    @Path("/profile/google_cloud_service_account")
    @Timed
    @Consumes(MediaType.APPLICATION_JSON)
    public Response dropGoogleCloudServiceAccount(@Auth User user, GoogleCloudServiceAccountEmail accountEmail)
    {
        user.dropGoogleCloudServiceAccount(accountEmail.email);
        userDAO.updateUserCredentials(user);

        return Response.ok().build();
    }

    @POST
    @Path("/profile/astra_service_account")
    @Timed
    @Produces(MediaType.APPLICATION_JSON)
    public Response addAstraServiceAccount(@Auth User user, @FormParam("astraClientId") @NotEmpty String clientId,
        @FormParam("astraClientName") @NotEmpty String clientName,
        @FormParam("astraClientSecret") @NotEmpty String clientSecret)
    {
        User.AstraServiceAccount astraServiceAccount = new User.AstraServiceAccount(clientId, clientName, clientSecret);
        user.addAstraCred(astraServiceAccount);
        userDAO.updateUserCredentials(user);

        return Response.ok().build();
    }

    @DELETE
    @Path("/profile/astra_service_account")
    @Timed
    @Consumes(MediaType.APPLICATION_JSON)
    public Response dropAstraServiceAccount(@Auth User user, User.AstraServiceAccount cred)
    {
        user.dropAstraCred(cred.clientName);
        userDAO.updateUserCredentials(user);

        return Response.ok().build();
    }

    private static class AstraServiceAccountName
    {
        public String clientName;
    }

    @POST
    @Path("/profile/astra_service_account_default")
    @Timed
    @Consumes(MediaType.APPLICATION_JSON)
    public Response setDefaultAstraServiceAccount(@Auth User user, AstraServiceAccountName serviceAccountName)
    {
        user.validateAndSetDefaultAstraServiceAccount(serviceAccountName.clientName);
        userDAO.updateUserCredentials(user);

        return Response
            .ok()
            .entity("{\"default_astra_service_account\":\"" +
                user.getDefaultAstraServiceAccountName() + "\"}")
            .build();
    }

    @POST
    @Path("/profile/backup_service_creds")
    @Timed
    @Produces(MediaType.APPLICATION_JSON)
    public Response addBackupServiceCred(@Auth User user,
        @FormParam("backupServiceName") @NotEmpty String backupServiceName,
        @FormParam("s3AccessKey") @NotEmpty String s3AccessKey,
        @FormParam("s3SecretKey") @NotEmpty String s3SecretKey)
    {
        User.BackupServiceCred cred = new User.BackupServiceCred(backupServiceName, s3AccessKey, s3SecretKey);
        user.addBackupServiceCred(cred);
        userDAO.updateUserCredentials(user);

        return Response.ok().build();
    }

    @DELETE
    @Path("/profile/backup_service_creds")
    @Timed
    @Consumes(MediaType.APPLICATION_JSON)
    public Response dropBackupServiceCred(@Auth User user, Map<String, String> json)
    {
        String credName = getNonNullNonEmpty(json, "backupServiceName");

        user.dropBackupServiceCred(credName);
        userDAO.updateUserCredentials(user);

        return Response.ok().build();
    }

    @POST
    @Path("/profile/backup_service_creds_default")
    @Timed
    @Consumes(MediaType.APPLICATION_JSON)
    public Response setDefaultBackupServiceCred(@Auth User user, Map<String, String> json)
    {
        String name = getNonNullNonEmpty(json, "backupServiceName");

        user.validateAndSetDefaultBackupServiceCred(name);
        userDAO.updateUserCredentials(user);

        return Response
            .ok()
            .entity("{\"backup_service_creds_default\":\"" + user.getDefaultBackupServiceCred() + "\"}")
            .build();
    }

    @POST
    @Path("/profile/docker_registry_creds")
    @Timed
    @Produces(MediaType.APPLICATION_JSON)
    public Response addDockerRegistryCred(@Auth User user,
        @FormParam("dockerRegistry") @NotEmpty String dockerRegistry,
        @FormParam("dockerRegistryUsername") @NotEmpty String username,
        @FormParam("dockerRegistryPassword") @NotEmpty String password)
    {
        User.DockerRegistryCredential cred = new User.DockerRegistryCredential(dockerRegistry, username, password);
        user.addDockerRegistryCredential(cred);
        userDAO.updateUserCredentials(user);

        return Response.ok().build();
    }

    @POST
    @Path("/profile/generic_secret")
    @Timed
    @Produces(MediaType.APPLICATION_JSON)
    public Response addGenericSecret(@Auth User user,
        @FormParam("secretName") @NotEmpty String credentialName,
        @FormParam("secret") @NotEmpty String secret)
    {
        user.addGenericSecret(credentialName, secret);
        userDAO.updateUserCredentials(user);

        return Response.ok().build();
    }

    @DELETE
    @Path("/profile/docker_registry_creds")
    @Timed
    @Consumes(MediaType.APPLICATION_JSON)
    public Response dropDockerRegistryCred(@Auth User user, Map<String, String> json)
    {
        String dockerRegistry = getNonNullNonEmpty(json, "dockerRegistry");

        user.dropDockerRegistryCredential(dockerRegistry);
        userDAO.updateUserCredentials(user);

        return Response.ok().build();
    }

    @DELETE
    @Path("/profile/generic_secret")
    @Timed
    @Consumes(MediaType.APPLICATION_JSON)
    public Response dropGenericSecret(@Auth User user, Map<String, String> json)
    {
        String genericSecret = getNonNullNonEmpty(json, "secretName");

        user.dropGenericSecret(genericSecret);
        userDAO.updateUserCredentials(user);

        return Response.ok().build();
    }

    private String getNonNullNonEmpty(Map<String, String> json, String key)
    {
        String val = json.get(key);
        if (val == null || val.isEmpty())
        {
            throw new WebApplicationException(String.format("Missing %s", key), 422);
        }
        return val;
    }

    private <T> void setIfNonNull(Consumer<T> setter, T param)
    {
        if (param != null)
        {
            setter.accept(param);
        }
    }

    private <T> void setIfNonNull(Consumer<T> setter, Function<String, T> converter, String param)
    {
        if (param != null)
        {
            setter.accept(converter.apply(param));
        }
    }

    @POST
    @Path("/profile")
    @Timed
    @Produces(MediaType.APPLICATION_JSON)
    public Response doProfile(@Auth User user,
        @FormParam("automatonSharedHandle") String automatonSharedHandle,
        @FormParam("group") String group,
        @FormParam("emailPref") String emailPref,
        @FormParam("slackPref") String slackPref,
        @FormParam("publicSshKey") String sshKey,
        @FormParam("ec2AccessKey") String ec2AccessKey,
        @FormParam("ec2SecretKey") String ec2SecretKey,
        @FormParam("openstackUsername") String openstackUsername,
        @FormParam("openstackPassword") String openstackPassword,
        @FormParam("ironicTenantName") String ironicTenantName,
        @FormParam("nebulaProjectName") String nebulaProjectName)
    {
        try
        {
            // FAL-716: This API method serves as the endpoint to several forms, so not all of the
            // FormParams will be set.  To make sure we don't overwrite anything that wasn't
            // specified, we only set things in user for which a non-null FormParam was supplied.

            setIfNonNull(user::setGroup, userGroupMapper::validGroupOrOther, group);
            setIfNonNull(user::throwOrSetValidAutomatonSharedHandle, automatonSharedHandle);
            setIfNonNull(user::setPublicSshKey, sshKey);
            setIfNonNull(user::setEc2AccessKey, ec2AccessKey);
            setIfNonNull(user::setEc2SecretKey, ec2SecretKey);
            setIfNonNull(user::setOpenstackUsername, openstackUsername);
            setIfNonNull(user::setOpenstackPassword, openstackPassword);
            setIfNonNull(user::setIronicTenantName, ironicTenantName);
            setIfNonNull(user::validateAndSetNebulaProjectName, nebulaProjectName);
            setIfNonNull(user::setEmailPref, TestCompletionNotification::valueOf, emailPref);
            setIfNonNull(user::setSlackPref, TestCompletionNotification::valueOf, slackPref);

            userDAO.updateUserCredentials(user);
        }
        catch (Exception e)
        {
            logger.error("UserDAO updateUserCredentials failed", e);
            throw new WebApplicationException(e.getMessage());
        }

        return Response.ok().build();
    }

    @GET
    @Path("/reset-oauth")
    @Timed
    @Produces(MediaType.APPLICATION_JSON)
    public Response resetOauth(@Auth User user)
    {
        userDAO.updateUserOauthId(user);
        return Response.seeOther(URI.create("/account")).build();
    }

    public class AccountView extends FalloutView
    {
        protected final Collection<UserGroupMapper.UserGroup> allUserGroups = userGroupMapper.getGroups();
        final List<Pair<TestCompletionNotification, Boolean>> allEmailNotify;
        final List<Pair<TestCompletionNotification, Boolean>> allSlackNotify;
        final Set<String> genericSecrets;

        protected final boolean showOpenstackSection;

        public AccountView(FalloutConfiguration configuration, User user)
        {
            super(List.of(user.getEmail(), "Profile"), "user_account.mustache", user, mainView);
            this.showOpenstackSection =
                !configuration.getUseTeamOpenstackCredentials() || userGroupMapper.isCIUser(user);
            allEmailNotify = Arrays.stream(TestCompletionNotification.values())
                .map(notify -> Pair.of(notify, user.getEmailPref() == notify))
                .collect(Collectors.toList());
            allSlackNotify = Arrays.stream(TestCompletionNotification.values())
                .map(notify -> Pair.of(notify, user.getSlackPref() == notify))
                .collect(Collectors.toList());
            genericSecrets = user.getGenericSecrets().keySet();
        }
    }

    private void validateEmail(String email)
    {
        if (!Pattern.compile(EMAIL_PATTERN).matcher(email).matches())
        {
            throw new WebApplicationException("Invalid email address", 422);
        }
    }
}
