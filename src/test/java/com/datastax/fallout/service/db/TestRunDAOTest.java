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
package com.datastax.fallout.service.db;

import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import com.fasterxml.jackson.core.type.TypeReference;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import com.datastax.driver.core.ColumnMetadata;
import com.datastax.driver.core.Session;
import com.datastax.fallout.ops.ResourceRequirement;
import com.datastax.fallout.service.core.DeletedTestRun;
import com.datastax.fallout.service.core.Fakes;
import com.datastax.fallout.service.core.FinishedTestRun;
import com.datastax.fallout.service.core.TestRun;
import com.datastax.fallout.util.JsonUtils;

import static com.datastax.fallout.assertj.Assertions.assertThat;
import static com.datastax.fallout.ops.ResourceRequirementHelpers.req;
import static com.datastax.fallout.service.db.CassandraDriverManagerHelpers.createDriverManager;

@Tag("requires-db")
public class TestRunDAOTest
{
    private static final String keyspace = "test_run_dao";
    private static Session session;
    private static CassandraDriverManager driverManager;
    private static TestRunDAO testRunDAO;

    @BeforeAll
    public static void startCassandra() throws Exception
    {
        driverManager = createDriverManager(keyspace);

        testRunDAO = new TestRunDAO(driverManager);

        driverManager.start();
        testRunDAO.start();

        session = driverManager.getSession();
    }

    @AfterAll
    public static void stopCassandra() throws Exception
    {
        testRunDAO.stop();
        driverManager.stop();
    }

    protected final String owner = "owner";
    protected final String testName = "testName";
    protected final UUID testRunId = UUID.fromString("69A38F36-8A91-4ABB-A2C8-B669189FEFD5");
    private Fakes.UUIDFactory uuidFactory = new Fakes.UUIDFactory();

    private TestRun createTestRun()
    {
        TestRun testRun = new TestRun();
        testRun.setOwner(owner);
        testRun.setTestName(testName);
        testRun.setTestRunId(uuidFactory.create());

        return testRun;
    }

    private TestRun createSavedTestRun(Consumer<TestRun> testRunModifiers)
    {
        TestRun testRun = createTestRun();
        testRunModifiers.accept(testRun);

        testRunDAO.update(testRun);

        return testRun;
    }

    protected TestRun fetchSavedTestRun()
    {
        return fetchSavedTestRun(testRun -> {});
    }

    protected TestRun fetchSavedTestRun(Consumer<TestRun> testRunModifiers)
    {
        return fetchTestRun(createSavedTestRun(testRunModifiers));
    }

    protected TestRun fetchTestRun(TestRun testRun)
    {
        return testRunDAO.get(owner, testName, testRun.getTestRunId());
    }

    private static class DateFactory
    {
        private Date current;

        public DateFactory(Date start)
        {
            current = start;
        }

        public Date next()
        {
            Date result = current;
            current = Date.from(current.toInstant().plus(1, ChronoUnit.SECONDS));
            return result;
        }

        public Date nextDay()
        {
            Date result = current;
            current = Date.from(current.toInstant().plus(1, ChronoUnit.DAYS));
            return result;
        }
    }

    protected DateFactory dateFactory;

    @BeforeEach
    public void setUp()
    {
        List.of("test_runs", "finished_test_runs", "deleted_test_runs")
            .forEach(table -> session.execute(String.format("truncate %s.%s;", keyspace, table)));
        dateFactory = new DateFactory(Date.from(Instant.now().minus(20, ChronoUnit.DAYS)));
        testRunDAO.maybeAddFinishedTestRunEndStop(dateFactory.next());
    }

    public static class Tests extends TestRunDAOTest
    {

        @Test
        public void unset_templateParams_are_persisted_as_null()
        {
            TestRun testRun = fetchSavedTestRun();
            assertThat(testRun).hasTemplateParams(null);
        }

        @Test
        public void null_templateParams_are_read_as_emptyMap()
        {
            TestRun testRun = fetchSavedTestRun();
            assertThat(testRun).hasTemplateParamsMap(Map.of());
        }

        @Test
        public void empty_templateParams_are_persisted_as_null()
        {
            TestRun testRun = fetchSavedTestRun(testRun_ -> testRun_.setTemplateParamsMap(Map.of()));
            assertThat(testRun).hasTemplateParams(null);
        }

        @Test
        public void null_templateParams_are_serialized_to_json_as_empty_yaml()
        {
            TestRun testRun = fetchSavedTestRun();
            assertThat(JsonUtils.fromJson(JsonUtils.toJson(testRun), new TypeReference<Map<String, Object>>() {}))
                .hasEntrySatisfying("templateParams", value -> assertThat(value).isEqualTo("{}"));
        }

        @Test
        public void only_finished_test_runs_are_persisted_when_finished()
        {
            final List<FinishedTestRun> expectedFinishedTestRuns = Arrays.stream(TestRun.State.values())
                .map(state -> fetchSavedTestRun(testRun_ -> {
                    testRun_.setState(state);
                    testRun_.setFinishedAt(dateFactory.next());
                }))
                .filter(testRun -> testRun.getState().finished())
                .map(FinishedTestRun::fromTestRun)
                .collect(Collectors.toList());

            Collections.reverse(expectedFinishedTestRuns);

            assertThat(expectedFinishedTestRuns).isNotEmpty();

            assertThat(testRunDAO.getRecentFinishedTestRuns())
                .isEqualTo(expectedFinishedTestRuns);
        }

        @Test
        public void finished_test_runs_are_limited()
        {
            final List<FinishedTestRun> expectedFinishedTestRuns = IntStream
                .range(0, 2 * TestRunDAO.FINISHED_TEST_RUN_LIMIT)
                .mapToObj(i -> fetchSavedTestRun(testRun_ -> {
                    testRun_.setState(TestRun.State.PASSED);
                    testRun_.setFinishedAt(i % 5 == 0 ? dateFactory.nextDay() : dateFactory.next());
                }))
                .map(FinishedTestRun::fromTestRun)
                .collect(Collectors.toList());

            assertThat(expectedFinishedTestRuns).hasSize(2 * TestRunDAO.FINISHED_TEST_RUN_LIMIT);

            Collections.reverse(expectedFinishedTestRuns);

            assertThat(testRunDAO.getRecentFinishedTestRuns())
                .isEqualTo(expectedFinishedTestRuns.subList(0, TestRunDAO.FINISHED_TEST_RUN_LIMIT));
        }

        public void assertFinishedTestRunsCorrectlyFilterOnRange(int latestAgeInHours, int earliestAgeInHours)
        {
            final var oldestAgeInHours = 72;

            final var startInstant = Instant.now().truncatedTo(ChronoUnit.DAYS)
                .minus(20, ChronoUnit.DAYS);

            final var expectedFinishedTestRuns = IntStream
                .range(0, oldestAgeInHours)
                .mapToObj(hours -> fetchSavedTestRun(testRun_ -> {
                    testRun_.setState(TestRun.State.PASSED);
                    testRun_.setFinishedAt(Date.from(startInstant.minus(hours, ChronoUnit.HOURS)));
                }))
                .map(FinishedTestRun::fromTestRun)
                .collect(Collectors.toList());

            final var latestInstant = startInstant.minus(latestAgeInHours, ChronoUnit.HOURS);
            final var earliestInstant = startInstant.minus(earliestAgeInHours, ChronoUnit.HOURS);

            assertThat(List.of(latestInstant, earliestInstant))
                .allSatisfy(instant -> assertThat(instant).isNotEqualTo(instant.truncatedTo(ChronoUnit.DAYS)));

            assertThat(
                testRunDAO.getAllFinishedTestRunsThatFinishedBetweenInclusive(earliestInstant, latestInstant)
                    .collect(Collectors.toList()))
                        .isEqualTo(expectedFinishedTestRuns.subList(latestAgeInHours, earliestAgeInHours + 1));
        }

        @Test
        public void finished_test_runs_can_be_filtered_on_non_day_ranges_across_days()
        {
            assertFinishedTestRunsCorrectlyFilterOnRange(12, 60);
        }

        @Test
        public void finished_test_runs_can_be_filtered_on_non_day_rangs_on_a_single_day()
        {
            assertFinishedTestRunsCorrectlyFilterOnRange(12, 14);
        }

        @Test
        public void finished_test_runs_are_only_stored_if_they_are_recent()
        {
            final TestRun recentTestRun = fetchSavedTestRun(testRun -> {
                testRun.setState(TestRun.State.PASSED);
                testRun.setFinishedAt(Date.from(Instant.now()));
            });

            final TestRun ancientTestRun = fetchSavedTestRun(testRun -> {
                testRun.setState(TestRun.State.PASSED);
                testRun.setFinishedAt(Date.from(Instant.now().minus(Duration.ofDays(60))));
            });

            assertThat(testRunDAO.getRecentFinishedTestRuns())
                .containsOnly(FinishedTestRun.fromTestRun(recentTestRun));
        }

        @Test
        public void test_run_moved_to_deleted_test_runs()
        {
            TestRun testRun = fetchSavedTestRun();
            assertThat(testRunDAO.getAllDeleted(owner, testName)).doesNotContain(DeletedTestRun.fromTestRun(testRun));
            testRunDAO.delete(testRun);

            assertThat(testRunDAO.getAllDeleted(owner, testName)).contains(DeletedTestRun.fromTestRun(testRun));
            assertThat(testRunDAO.getAll(owner, testName)).doesNotContain(testRun);
        }

        @Test
        public void all_test_runs_moved_to_deleted_test_runs()
        {
            TestRun testRun = fetchSavedTestRun();
            TestRun testRun2 = fetchSavedTestRun();
            assertThat(testRunDAO.getAllDeleted(owner, testName))
                .doesNotContain(DeletedTestRun.fromTestRun(testRun), DeletedTestRun.fromTestRun(testRun2));

            testRunDAO.deleteAll(testRun.getOwner(), testRun.getTestName());

            assertThat(testRunDAO.getAllDeleted(owner, testName))
                .containsExactlyInAnyOrder(DeletedTestRun.fromTestRun(testRun), DeletedTestRun.fromTestRun(testRun2));
            assertThat(testRunDAO.getAll(owner, testName)).doesNotContain(testRun, testRun2);
        }

        @Test
        public void test_run_and_deleted_test_run_tables_schema_equal()
        {
            List<ColumnMetadata> cm = session.getCluster().getMetadata().getKeyspace(keyspace).getTable("test_runs")
                .getColumns();
            List<ColumnMetadata> cm2 = session.getCluster().getMetadata().getKeyspace(keyspace)
                .getTable("deleted_test_runs").getColumns();

            for (ColumnMetadata c : cm)
            {
                assertThat(c).isIn(cm2);
            }

        }

        @Test
        public void resource_requirements_are_preserved()
        {
            final ResourceRequirement resourceRequirement =
                req("provider", "tenant", "instance", 5);

            assertThat(
                fetchSavedTestRun(testRun -> testRun.setResourceRequirements(Set.of(resourceRequirement))))
                    .hasResourceRequirements(resourceRequirement);

            assertThat(fetchSavedTestRun(testRun -> testRun.setResourceRequirements(Set.of())))
                .hasNoResourceRequirements();
        }
    }

    public static class ArtifactUpdate extends TestRunDAOTest
    {

        @ParameterizedTest(name = "{0}")
        @EnumSource(TestRun.State.class)
        public void artifact_update_does_not_affect_finished_testruns(TestRun.State state)
        {
            final var originalArtifacts = Map.of("fish", 100L);
            final var updatedArtifacts = Map.of("cheese", 200L);

            TestRun testRun = fetchSavedTestRun(t -> {
                t.updateArtifacts(originalArtifacts);
                t.setState(state);
            });

            testRun.updateArtifacts(updatedArtifacts);

            testRunDAO.updateArtifactsIfNeeded(testRun);

            final TestRun savedTestRun = fetchTestRun(testRun);

            assertThat(savedTestRun.getArtifacts()).isEqualTo(state.finished() ?
                originalArtifacts :
                updatedArtifacts);
        }

        @ParameterizedTest(name = "{0}")
        @EnumSource(TestRun.State.class)
        public void artifact_update_always_updates_empty_artifacts(TestRun.State state)
        {
            final var updatedArtifacts = Map.of("cheese", 200L);

            TestRun testRun = fetchSavedTestRun(t -> {
                t.setState(state);
            });

            testRun.updateArtifacts(updatedArtifacts);

            testRunDAO.updateArtifactsIfNeeded(testRun);

            final TestRun savedTestRun = fetchTestRun(testRun);

            assertThat(savedTestRun.getArtifacts()).isEqualTo(updatedArtifacts);
        }
    }
}
