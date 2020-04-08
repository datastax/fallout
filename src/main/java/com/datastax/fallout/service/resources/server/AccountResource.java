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
package com.datastax.fallout.service.resources.server;

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
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import com.codahale.metrics.annotation.Timed;
import com.google.common.base.Preconditions;
import io.dropwizard.auth.Auth;
import org.apache.commons.lang3.tuple.Pair;
import org.hibernate.validator.constraints.NotEmpty;

import com.datastax.driver.mapping.Mapper;
import com.datastax.fallout.service.FalloutConfiguration;
import com.datastax.fallout.service.FalloutService;
import com.datastax.fallout.service.auth.SecurityUtil;
import com.datastax.fallout.service.core.Session;
import com.datastax.fallout.service.core.TestCompletionNotification;
import com.datastax.fallout.service.core.User;
import com.datastax.fallout.service.db.UserDAO;
import com.datastax.fallout.service.views.FalloutView;
import com.datastax.fallout.service.views.MainView;
import com.datastax.fallout.util.ScopedLogger;
import com.datastax.fallout.util.UserMessenger;

@Path("/account")
@Produces(MediaType.APPLICATION_JSON)
public class AccountResource
{
    private static final ScopedLogger logger = ScopedLogger.getLogger(AccountResource.class);

    private static final String EMAIL_PATTERN_STR =
        "^[_A-Za-z0-9-]+(\\.[_A-Za-z0-9-]+)*@[A-Za-z0-9]+(\\.[A-Za-z0-9]+)*(\\.[A-Za-z]{2,})$";
    private static final Pattern EMAIL_PATTERN = Pattern.compile(EMAIL_PATTERN_STR);

    private final UserDAO userDAO;
    private final FalloutConfiguration configuration;
    private final UserMessenger mailer;
    private final MainView mainView;
    private final SecurityUtil securityUtil;

    public AccountResource(UserDAO userDAO, FalloutConfiguration configuration, UserMessenger mailer,
        MainView mainView, SecurityUtil securityUtil)
    {
        this.userDAO = userDAO;
        this.configuration = configuration;
        this.mailer = mailer;
        this.mainView = mainView;
        this.securityUtil = securityUtil;
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
        @FormParam("password") @NotEmpty String password)
    {
        validateEmail(email);

        User existingUser = userDAO.getUser(email);
        if (existingUser != null && existingUser.getSalt() != null)
        {
            throw new WebApplicationException("Email already registered", Response.Status.BAD_REQUEST);
        }

        Session session;

        try
        {
            var user = userDAO.createUserIfNotExists(name, email, password);
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
    @Path("/profile")
    @Timed
    @Produces(MediaType.APPLICATION_JSON)
    public Response doProfile(@Auth User user,
        @FormParam("emailPref") String emailPref,
        @FormParam("slackPref") String slackPref)
    {
        try
        {
            if (emailPref != null)
            {
                user.setEmailPref(TestCompletionNotification.valueOf(emailPref));
            }
            if (slackPref != null)
            {
                user.setSlackPref(TestCompletionNotification.valueOf(slackPref));
            }

            userDAO.updateUserCredentials(user, Mapper.Option.saveNullFields(false));
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
        final List<Pair<TestCompletionNotification, Boolean>> allEmailNotify;
        final List<Pair<TestCompletionNotification, Boolean>> allSlackNotify;

        public AccountView(FalloutConfiguration configuration, User user)
        {
            super("user_account.mustache", user, mainView);
            allEmailNotify = Arrays.stream(TestCompletionNotification.values())
                .map(notify -> Pair.of(notify, user.getEmailPref() == notify))
                .collect(Collectors.toList());
            allSlackNotify = Arrays.stream(TestCompletionNotification.values())
                .map(notify -> Pair.of(notify, user.getSlackPref() == notify))
                .collect(Collectors.toList());
        }
    }

    private void validateEmail(String email)
    {
        if (!EMAIL_PATTERN.matcher(email).matches())
        {
            throw new WebApplicationException("Invalid email address", 422);
        }
    }
}
