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
package com.datastax.fallout.service.views;

import javax.ws.rs.core.UriBuilder;

import java.net.URI;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.datastax.fallout.harness.ActiveTestRun;
import com.datastax.fallout.service.core.ReadOnlyTestRun;
import com.datastax.fallout.service.core.TestRun;
import com.datastax.fallout.service.core.User;
import com.datastax.fallout.service.db.UserGroupMapper;
import com.datastax.fallout.service.resources.server.TestResource;
import com.datastax.fallout.util.DateUtils;

/** Used in view classes that use partials/testrun/table.mustache */
public class LinkedTestRuns
{
    public static class LinkedTestRun
    {
        final String ownerLink;
        final String testLink;
        final String testRunLink;
        final String testRunUri;
        final ReadOnlyTestRun testRun;
        final boolean canCancel;
        final boolean canDelete;
        final boolean canModify;
        final Set<Map.Entry<String, Object>> templateParamValues;
        final Set<Map.Entry<String, String>> grafanaDashboardLinks;

        LinkedTestRun(UserGroupMapper userGroupMapper, Optional<User> currentUser,
            Set<String> templateParamColumns, ReadOnlyTestRun testRun, String externalUrl)
        {
            ownerLink = TestResource.linkForShowTests(testRun);
            testLink = TestResource.linkForShowTestRuns(testRun);
            testRunLink = TestResource.linkForShowTestRunArtifacts(testRun);
            testRunUri = UriBuilder.fromUri(externalUrl)
                .uri(TestResource.uriForShowTestRunArtifacts(testRun))
                .toString();
            this.testRun = testRun;
            canCancel = TestResource.canCancel(userGroupMapper, currentUser, testRun);
            canDelete = TestResource.canDelete(userGroupMapper, currentUser, testRun);
            canModify = currentUser.map(user_ -> testRun.canBeModifiedBy(userGroupMapper, user_)).orElse(false);

            final Map<String, Object> templateParams = testRun.getTemplateParamsMap();
            final Map<String, Object> allTemplateParams = new LinkedHashMap<>();

            templateParamColumns.forEach(param -> {
                Object value = templateParams.get(param);
                if (value == null)
                {
                    value = "";
                }
                allTemplateParams.put(param, value);
            });

            templateParamValues = allTemplateParams.entrySet();

            if (testRun.getState().equals(TestRun.State.RUNNING))
            {
                grafanaDashboardLinks = testRun.getLinks().entrySet();
            }
            else
            {
                grafanaDashboardLinks = new HashSet<>();
            }
        }

        public String getStateAlertType()
        {
            // https://blackrockdigital.github.io/startbootstrap-sb-admin-2/pages/notifications.html
            switch (testRun.getState())
            {
                case PASSED:
                    return "alert alert-success";
                case FAILED:
                    return failedDuringWorkload() ?
                        "alert alert-danger" :
                        "alert alert-warning";
                case ABORTED:
                    return "alert alert-info";
                default:
                    return "";
            }
        }

        public boolean failed()
        {
            return testRun.getState() == TestRun.State.FAILED;
        }

        public boolean failedDuringWorkload()
        {
            return testRun.getFailedDuring() == null ||
                testRun.getFailedDuring() == TestRun.State.RUNNING ||
                testRun.getFailedDuring() == TestRun.State.CHECKING_ARTIFACTS;
        }

        /** Return {@link TestRun#failedDuring} only if we're not waiting.
         *
         *  <p>Rationale: if we're waiting, then (state, failedDuring) can only be (CREATED,
         *  null) or (WAITING_FOR_RESOURCES, CHECKING_RESOURCES).  Showing that we failed during
         *  CHECKING_RESOURCES isn't useful information to the user, as it implies a real failure;
         *  however it isn't one because we've requeued and are WAITING_FOR_RESOURCES, so we hide it. */
        public TestRun.State notWaitingAndFailedDuring()
        {
            return !testRun.getState().waiting() ? testRun.getFailedDuring() : null;
        }

        public String getStartedAtUtc()
        {
            return DateUtils.formatUTCDate(testRun.getStartedAt());
        }

        public String getCreatedAtUtc()
        {
            return DateUtils.formatUTCDate(testRun.getCreatedAt());
        }

        /** Unconditionally format {@link TestRun#getFinishedAt} so that we can see the last time the
         *  testrun was tried. */
        public String getLastTriedAtUtc()
        {
            return DateUtils.formatUTCDate(testRun.getFinishedAt());
        }

        /** Show {@link TestRun#getFinishedAt} only if a {@link TestRun} is actually finished
         *
         *  <p>Rationale: finishedAt will be populated when {@link ActiveTestRun} is done
         *  with it, which includes requeuing due to lack of resources; this is useful
         *  information, but potentially confusing when presented to an end user */
        public String getFinishedAtUtc()
        {
            return testRun.getState().finished() ?
                DateUtils.formatUTCDate(testRun.getFinishedAt()) :
                "";
        }

        public String getTestRunUri()
        {
            return testRunUri;
        }
    }

    /** Enum listing the optional parts of a testrun that can be hidden in table.mustache */
    public enum TableDisplayOption
    {
        OWNER,
        TEST_NAME,
        TEST_RUN,
        FINISHED_AT,
        DURATION,
        RESULTS,
        TEMPLATE_PARAMS,
        ARTIFACTS_LINK,
        MUTATION_ACTIONS,
        RESTORE_ACTIONS,
        RESOURCE_REQUIREMENTS,
        SIZE_ON_DISK,
        DELETE_MANY
    }

    public final List<LinkedTestRun> testRuns;

    /** Lists which optional items should be shown.  If something is in the map, it should be shown; Map is used
     *  instead of Set because mustache.java doesn't handle Sets */
    final Map<String, Boolean> show = new HashMap<>();

    /** Set this to alter table.mustache header, based on if {@link #testRuns} are queued testruns */
    final boolean areQueued;
    final boolean canDeleteAny;

    final Set<String> templateParamColumns = new LinkedHashSet<>();
    String tableClass = "table-striped";

    public LinkedTestRuns(UserGroupMapper userGroupMapper, Optional<User> currentUser,
        Collection<? extends ReadOnlyTestRun> testRuns)
    {
        this(userGroupMapper, currentUser, false, testRuns);
    }

    public LinkedTestRuns(UserGroupMapper userGroupMapper, Optional<User> currentUser,
        boolean areQueued, Collection<? extends ReadOnlyTestRun> testRuns)
    {
        this(userGroupMapper, currentUser, areQueued, testRuns, "");
    }

    public LinkedTestRuns(UserGroupMapper userGroupMapper, Optional<User> currentUser,
        Collection<? extends ReadOnlyTestRun> testRuns, String externalUrl)
    {
        this(userGroupMapper, currentUser, false, testRuns, externalUrl);
    }

    private LinkedTestRuns(UserGroupMapper userGroupMapper, Optional<User> currentUser,
        boolean areQueued, Collection<? extends ReadOnlyTestRun> testRuns, String externalUrl)
    {
        testRuns.stream()
            .flatMap(testRun -> testRun.getTemplateParamsMap().keySet().stream())
            .forEach(templateParamColumns::add);

        this.testRuns = testRuns.stream()
            .map(tr -> new LinkedTestRun(userGroupMapper, currentUser, templateParamColumns, tr, externalUrl))
            .collect(Collectors.toList());

        this.areQueued = areQueued;
        this.canDeleteAny = testRuns.stream().anyMatch(tr -> TestResource.canDelete(userGroupMapper, currentUser, tr));

        Stream.of(TableDisplayOption.values()).forEach(v -> show.put(v.toString(), true));
        hide();
    }

    private boolean shows(TableDisplayOption option)
    {
        return show.containsKey(option.toString());
    }

    public LinkedTestRuns hide(TableDisplayOption... options)
    {
        Stream.of(options).map(TableDisplayOption::toString).forEach(show::remove);

        boolean hasStateColoring = testRuns.stream().anyMatch(x -> !x.getStateAlertType().isEmpty());
        if (hasStateColoring)
        {
            tableClass = "";
        }
        else
        {
            tableClass = "table-striped";
        }
        return this;
    }
}
