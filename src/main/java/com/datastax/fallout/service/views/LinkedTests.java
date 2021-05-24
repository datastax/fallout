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

import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.BiPredicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.datastax.fallout.service.core.Test;
import com.datastax.fallout.service.core.User;
import com.datastax.fallout.service.resources.server.TestResource;
import com.datastax.fallout.util.DateUtils;

public class LinkedTests
{
    public static class LinkedTest
    {
        final String ownerLink;
        final String testLink;
        final Test test;
        final boolean canDelete;

        LinkedTest(Test test, boolean canDelete)
        {
            ownerLink = TestResource.linkForShowTests(test);
            testLink = TestResource.linkForShowTestRuns(test);
            this.test = test;
            this.canDelete = canDelete;
        }

        public String getCreatedAtUtc()
        {
            return DateUtils.formatUTCDate(test.getCreatedAt());
        }

        public String getLastRunAtUtc()
        {
            return DateUtils.formatUTCDate(test.getLastRunAt());
        }
    }

    /** Enum listing the optional parts of a test that can be hidden in table.mustache */
    public enum TableDisplayOption
    {
        OWNER,
        RESTORE_ACTIONS
    }

    final List<LinkedTest> tests;

    /** Lists which optional items should be shown */
    final Map<String, Boolean> show = new HashMap<>();

    public LinkedTests(Optional<User> user, Collection<? extends Test> tests,
        BiPredicate<Optional<User>, Test> canDelete)
    {
        this.tests = tests
            .stream()
            .map(test -> new LinkedTest(test, canDelete.test(user, test)))
            .collect(Collectors.toList());

        Stream.of(LinkedTests.TableDisplayOption.values()).forEach(v -> show.put(v.toString(), true));
    }

    public LinkedTests hide(LinkedTests.TableDisplayOption... options)
    {
        Stream.of(options).map(LinkedTests.TableDisplayOption::toString).forEach(show::remove);
        return this;
    }
}
