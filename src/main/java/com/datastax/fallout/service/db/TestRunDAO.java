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

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import com.google.common.annotations.VisibleForTesting;
import io.dropwizard.lifecycle.Managed;

import com.datastax.driver.core.BatchStatement;
import com.datastax.driver.core.CodecRegistry;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.TypeCodec;
import com.datastax.driver.extras.codecs.enums.EnumNameCodec;
import com.datastax.driver.extras.codecs.jdk8.OptionalCodec;
import com.datastax.driver.mapping.Mapper;
import com.datastax.driver.mapping.MappingManager;
import com.datastax.driver.mapping.Result;
import com.datastax.driver.mapping.annotations.Accessor;
import com.datastax.driver.mapping.annotations.Query;
import com.datastax.fallout.service.core.DeletedTestRun;
import com.datastax.fallout.service.core.FinishedTestRun;
import com.datastax.fallout.service.core.ReadOnlyTestRun;
import com.datastax.fallout.service.core.TestCompletionNotification;
import com.datastax.fallout.service.core.TestRun;
import com.datastax.fallout.service.core.TestRunIdentifier;
import com.datastax.fallout.util.Duration;
import com.datastax.fallout.util.ScopedLogger;

public class TestRunDAO implements Managed
{
    private static final ScopedLogger logger = ScopedLogger.getLogger(TestRunDAO.class);
    public static final int FINISHED_TEST_RUN_LIMIT = 10;
    private static int FINISHED_TEST_RUN_TTL = (int) Duration.days(30).toSeconds();

    @Accessor
    interface TestRunAccessor
    {
        @Query("SELECT * FROM test_runs")
        Result<TestRun> getAll();

        @Query("SELECT * FROM test_runs WHERE owner = :owner AND testName = :testName")
        Result<TestRun> getAll(String owner, String testName);

        @Query("SELECT * FROM test_runs WHERE testRunId = :testRunId")
        Result<TestRun> getByTestRunId(UUID testRunId);

        @Query("SELECT * FROM test_runs WHERE state = :state")
        Result<TestRun> getWithState(TestRun.State state);

        @Query("DELETE FROM test_runs WHERE owner = :owner AND testName = :testName")
        TestRun delete(String owner, String testName);

        /** Use LWT to ensure we're not updating a {@link TestRun} that has finished (if it's
         *  finished it should have a completely up-to-date set of artifacts) */
        @Query("UPDATE test_runs " +
            "SET artifacts = :artifacts, artifactsLastUpdated = :artifactsLastUpdated " +
            "WHERE owner = :owner AND testName = :testName AND testRunId = :testRunId " +
            "IF state in :unfinishedStates")
        ResultSet updateArtifactsIfNotFinished(Map<String, Long> artifacts, Date artifactsLastUpdated,
            String owner, String testName, UUID testRunId,
            List<TestRun.State> unfinishedStates);

        /** Use LWT to ensure we only update finished {@link TestRun}s if they have no artifacts */
        @Query("UPDATE test_runs " +
            "SET artifacts = :artifacts, artifactsLastUpdated = :artifactsLastUpdated " +
            "WHERE owner = :owner AND testName = :testName AND testRunId = :testRunId " +
            "IF state in :finishedStates AND artifacts in (null, {})")
        void updateArtifactsIfFinishedButEmpty(Map<String, Long> artifacts, Date artifactsLastUpdated,
            String owner, String testName, UUID testRunId,
            List<TestRun.State> finishedStates);

        @Query("UPDATE test_runs " +
            "SET artifacts = :artifacts, artifactsLastUpdated = :artifactsLastUpdated " +
            "WHERE owner = :owner AND testName = :testName AND testRunId = :testRunId " +
            "IF EXISTS")
        void updateArtifactsIfExists(Map<String, Long> artifacts, Date artifactsLastUpdated,
            String owner, String testName, UUID testRunId);

        @Query("UPDATE test_runs " +
            "SET state = 'ABORTED', failedDuring = :failedDuring, finishedAt = :finishedAt, results = :results " +
            "WHERE owner = :owner AND testName = :testName AND testRunId = :testRunId " +
            "IF state = :originalState")
        ResultSet markAbortedIfUnchanged(
            TestRun.State failedDuring, Date finishedAt, String results,
            String owner, String testName, UUID testRunId,
            TestRun.State originalState);
    }

    @Accessor
    private interface FinishedTestRunAccessor
    {
        @Query("SELECT * FROM finished_test_runs WHERE finishedAtDate = :finishedAtDate LIMIT " +
            TestRunDAO.FINISHED_TEST_RUN_LIMIT)
        Result<FinishedTestRun> get(Date finishedAtDate);

        @Query("SELECT * FROM finished_test_runs WHERE finishedAtDate = :finishedAtDate")
        Result<FinishedTestRun> getAll(Date finishedAtDate);
    }

    @Accessor
    private interface DeletedTestRunAccessor
    {
        @Query("SELECT * FROM deleted_test_runs WHERE owner = :owner")
        Result<DeletedTestRun> getAll(String owner);

        @Query("SELECT * FROM deleted_test_runs WHERE owner = :owner AND testName = :testName")
        Result<DeletedTestRun> getAll(String owner, String testName);

        @Query("SELECT testname FROM deleted_test_runs WHERE owner = :owner")
        ResultSet getTestNamesOfDeletedTestRuns(String owner);

        @Query("SELECT * FROM deleted_test_runs")
        Result<DeletedTestRun> getAll();

        @Query("UPDATE deleted_test_runs " +
            "SET artifacts = :artifacts, artifactsLastUpdated = :artifactsLastUpdated " +
            "WHERE owner = :owner AND testName = :testName AND testRunId = :testRunId " +
            "IF EXISTS")
        void updateArtifactsIfExists(Map<String, Long> artifacts, Date artifactsLastUpdated,
            String owner, String testName, UUID testRunId);
    }

    private final CassandraDriverManager driverManager;

    private Mapper<TestRun> testRunMapper;
    private Mapper<DeletedTestRun> deletedTestRunMapper;
    TestRunAccessor testRunAccessor;
    private DeletedTestRunAccessor deletedTestRunAccessor;
    private Mapper<FinishedTestRun> finishedTestRunMapper;
    private FinishedTestRunAccessor finishedTestRunAccessor;
    private boolean isRunning = false;

    public TestRunDAO(CassandraDriverManager driverManager)
    {
        this.driverManager = driverManager;
    }

    public TestRun get(String owner, String testName, UUID testRunId)
    {
        return testRunMapper.get(owner, testName, testRunId);
    }

    public TestRun get(TestRunIdentifier tri)
    {
        return testRunMapper.get(tri.getTestOwner(), tri.getTestName(), tri.getTestRunId());
    }

    public TestRun getEvenIfDeleted(TestRunIdentifier tri)
    {
        return getEvenIfDeleted(tri.getTestOwner(), tri.getTestName(), tri.getTestRunId());
    }

    public TestRun getEvenIfDeleted(String owner, String testName, UUID testRunId)
    {
        TestRun testRun = testRunMapper.get(owner, testName, testRunId);
        if (testRun == null)
        {
            return deletedTestRunMapper.get(owner, testName, testRunId);
        }
        return testRun;
    }

    public Stream<TestRun> getAllEvenIfDeleted()
    {
        return Stream.concat(
            StreamSupport.stream(testRunAccessor.getAll().spliterator(), false),
            StreamSupport.stream(deletedTestRunAccessor.getAll().spliterator(), false));
    }

    public Stream<TestRun> getAll()
    {
        return StreamSupport.stream(testRunAccessor.getAll().spliterator(), false);
    }

    public List<TestRun> getAll(String owner, String testName)
    {
        return testRunAccessor.getAll(owner, testName).all();
    }

    public TestRun getByTestRunId(UUID testRunId)
    {
        return testRunAccessor.getByTestRunId(testRunId).one();
    }

    public List<TestRun> getQueued()
    {
        return Stream.concat(testRunAccessor.getWithState(TestRun.State.CREATED).all().stream(),
            testRunAccessor.getWithState(TestRun.State.WAITING_FOR_RESOURCES).all().stream())
            .filter(tr -> {
                if (tr == null)
                {
                    logger.error("Found null value in the queue.");
                    return false;
                }
                return true;
            })
            .toList();
    }

    public DeletedTestRun getDeleted(TestRunIdentifier tri)
    {
        return deletedTestRunMapper.get(tri.getTestOwner(), tri.getTestName(), tri.getTestRunId());
    }

    public DeletedTestRun getDeleted(String owner, String testName, UUID testRunId)
    {
        return deletedTestRunMapper.get(owner, testName, testRunId);
    }

    public List<DeletedTestRun> getAllDeleted(String owner, String testName)
    {
        return deletedTestRunAccessor.getAll(owner, testName).all();
    }

    public List<DeletedTestRun> getAllDeleted(String owner)
    {
        return deletedTestRunAccessor.getAll(owner).all();
    }

    public List<String> getTestNamesOfDeletedTestRuns(String owner)
    {
        return deletedTestRunAccessor.getTestNamesOfDeletedTestRuns(owner).all().stream()
            .map(row -> row.getString("testName"))
            .toList();
    }

    public void add(TestRun testRun)
    {
        testRunMapper.save(testRun);
    }

    private static boolean finishedTestRunShouldBeSaved(ReadOnlyTestRun testRun)
    {
        return testRun.getState() != null &&
            testRun.getState().finished() &&
            testRun.getFinishedAt() != null &&
            java.time.Duration
                .between(
                    testRun.getFinishedAt().toInstant().truncatedTo(ChronoUnit.DAYS),
                    Instant.now())
                .getSeconds() < FINISHED_TEST_RUN_TTL;
    }

    public void update(TestRun testRun)
    {
        testRunMapper.save(testRun);
        if (finishedTestRunShouldBeSaved(testRun))
        {
            finishedTestRunMapper.save(
                FinishedTestRun.fromTestRun(testRun), Mapper.Option.ttl(FINISHED_TEST_RUN_TTL));
        }
    }

    void delete(TestRun testRun)
    {
        if (testRun.keepForever())
        {
            var msg = String
                .format("Attempted to delete test run %s but it is marked as keep forever", testRun.getShortName());
            logger.error(msg);
            throw new IllegalArgumentException(msg);
        }
        DeletedTestRun deletedtestrun = DeletedTestRun.fromTestRun(testRun);

        BatchStatement batchStatement = new BatchStatement();
        batchStatement.add(testRunMapper.deleteQuery(testRun));
        batchStatement.add(deletedTestRunMapper.saveQuery(deletedtestrun));
        driverManager.getSession().execute(batchStatement);
    }

    void deleteAll(String owner, String testName)
    {
        List<TestRun> runsToDelete = getAll(owner, testName);
        for (TestRun run : runsToDelete)
        {
            delete(run);
        }
    }

    void restore(DeletedTestRun deletedTestRun)
    {
        BatchStatement batchStatement = new BatchStatement();
        batchStatement.add(deletedTestRunMapper.deleteQuery(deletedTestRun));
        batchStatement.add(testRunMapper.saveQuery(deletedTestRun));
        driverManager.getSession().execute(batchStatement);
    }

    void restoreAll(String owner, String name)
    {
        List<DeletedTestRun> runsToRestore = getAllDeleted(owner, name);
        for (DeletedTestRun run : runsToRestore)
        {
            restore(run);
        }
    }

    public void deleteAllForever(String owner, String name)
    {
        List<TestRun> runsToDelete = getAll(owner, name);
        List<DeletedTestRun> runsToDeleteForever = getAllDeleted(owner, name);
        for (TestRun run : runsToDelete)
        {
            finishedTestRunMapper.delete(FinishedTestRun.fromTestRun(run));
            testRunMapper.delete(run);
        }
        for (DeletedTestRun run : runsToDeleteForever)
        {
            deletedTestRunMapper.delete(run);
        }
    }

    private static final List<TestRun.State> unfinishedStates =
        Arrays.stream(TestRun.State.values())
            .filter(state -> !state.finished())
            .toList();

    private static final List<TestRun.State> finishedStates =
        Arrays.stream(TestRun.State.values())
            .filter(TestRun.State::finished)
            .toList();

    /** Update only the artifacts fields in the DB, and only if the {@link TestRun}} in the DB is not
     *  finished <em>or</em> the artifacts field is empty.  This prevents accidentally overwriting
     *  the artifacts on a TestRun that finished while we were calculating the artifacts. */
    public void updateArtifactsIfNeeded(TestRun testRun)
    {
        final ResultSet update = testRunAccessor.updateArtifactsIfNotFinished(
            testRun.getArtifacts(), testRun.getArtifactsLastUpdated(),
            testRun.getOwner(), testRun.getTestName(), testRun.getTestRunId(),
            unfinishedStates);

        if (!update.wasApplied())
        {
            testRunAccessor.updateArtifactsIfFinishedButEmpty(
                testRun.getArtifacts(), testRun.getArtifactsLastUpdated(),
                testRun.getOwner(), testRun.getTestName(), testRun.getTestRunId(),
                finishedStates);
        }
    }

    /** Update only the artifacts fields in the DB for a testrun, regardless of whether it's been deleted or not. */
    void updateArtifactsEvenIfDeleted(TestRun testRun)
    {
        deletedTestRunAccessor.updateArtifactsIfExists(testRun.getArtifacts(), testRun.getArtifactsLastUpdated(),
            testRun.getOwner(), testRun.getTestName(), testRun.getTestRunId());
        testRunAccessor.updateArtifactsIfExists(testRun.getArtifacts(), testRun.getArtifactsLastUpdated(),
            testRun.getOwner(), testRun.getTestName(), testRun.getTestRunId());
    }

    @VisibleForTesting
    public void maybeAddFinishedTestRunEndStop(Date date)
    {
        if (driverManager.getSession().execute("SELECT COUNT(*) FROM finished_test_runs").one().getLong(0) == 0)
        {
            addFinishedTestRunEndStop(finishedTestRunMapper, date);
        }
    }

    private static void addFinishedTestRunEndStop(Mapper<FinishedTestRun> finishedTestRunMapper, Date date)
    {
        logger.info("Writing finished_test_runs end stop");
        finishedTestRunMapper.save(FinishedTestRun.endStop(date));
    }

    public static void addFinishedTestRunEndStop(String keyspace, MappingManager mappingManager)
    {
        addFinishedTestRunEndStop(mappingManager.mapper(FinishedTestRun.class, keyspace), new Date());
    }

    public List<FinishedTestRun> getRecentFinishedTestRuns()
    {
        return getAllFinishedTestRunsThatFinishedBeforeInclusive(Instant.now())
            .limit(FINISHED_TEST_RUN_LIMIT)
            .toList();
    }

    /** Get all {@link FinishedTestRun}s that finished in the closed range [earliestInstant, latestInstant] */
    public Stream<FinishedTestRun> getAllFinishedTestRunsThatFinishedBetweenInclusive(Instant earliestInstant,
        Instant latestInstant)
    {
        return getAllFinishedTestRunsThatFinishedBeforeInclusive(latestInstant)
            // We can rely on testruns being retrieved in reverse order of finishedAt because the
            // table is ordered that way
            .takeWhile(fetched -> fetched.getFinishedAt().toInstant().isAfter(earliestInstant) ||
                fetched.getFinishedAt().toInstant().equals(earliestInstant));
    }

    private Stream<FinishedTestRun> getAllFinishedTestRunsThatFinishedBeforeInclusive(Instant latestInstant)
    {
        // For each day before latestInstant, fetch all the test runs that finished on that day,
        // stopping iteration only if we hit the endstop.  This is _why_ we have an endstop:
        // without it, we can't tell when to stop iterating, since an empty result from getAll(day)
        // just means there were no testruns on that day, not that we've run out of testruns.
        return Stream
            .iterate(latestInstant.truncatedTo(ChronoUnit.DAYS),
                day -> day.minus(1, ChronoUnit.DAYS))
            .flatMap(
                day -> StreamSupport.stream(finishedTestRunAccessor.getAll(Date.from(day))
                    .spliterator(), false))
            .takeWhile(fetched -> !fetched.isEndStop())

            // Results will include tests that finished after `now - timeBeforeNow` because they're
            // truncated to days; filter them out here.
            .filter(fetched -> fetched.getFinishedAt().toInstant().isBefore(latestInstant) ||
                fetched.getFinishedAt().toInstant().equals(latestInstant));
    }

    /** Given a list of all <b>known</b> existing {@link TestRun}s, compares that list
     *  with {@link TestRun}s in the DB that are  {@link TestRun.State#active}, and
     *  aborts any in the <b>active</b> list that are not in the <b>known</b> list.
     *
     *  <p>Abort is performed using LWT via {@link #abortStaleTestRunIfUnchanged}, which ensures that the state
     *  of the TestRun in the DB hasn't changed since we checked (which could(?) happen if a RUNNER update
     *  to the DB doesn't reach the nodes queried here by the time we queried here for the active state)
     */
    public void abortStaleTestRuns(List<ReadOnlyTestRun> knownActiveTestRuns)
    {
        logger.withScopedInfo("Aborting stale testruns").run(() -> {
            logger.info("Known active testruns:");
            knownActiveTestRuns.forEach(testRun -> logger.info("  {}", testRun.getTestRunIdentifier()));

            final var maybeStaleTestRuns = Arrays.stream(TestRun.State.values())
                .filter(TestRun.State::active)
                .flatMap(state -> testRunAccessor.getWithState(state).all().stream())
                .toList();

            logger.info("Active testruns in the DB:");
            maybeStaleTestRuns.forEach(testRun -> logger.info("  {}", testRun.getTestRunIdentifier()));

            final var knownActiveTestRunIds = knownActiveTestRuns.stream()
                .map(ReadOnlyTestRun::getTestRunId).collect(Collectors.toSet());

            maybeStaleTestRuns.stream()
                .filter(testRun -> !knownActiveTestRunIds.contains(testRun.getTestRunId()))
                .forEach(this::abortStaleTestRunIfUnchanged);
        });
    }

    private void abortStaleTestRunIfUnchanged(TestRun testRun)
    {
        final var result = testRunAccessor.markAbortedIfUnchanged(
            testRun.getState(), new Date(), "Stale test aborted during fallout startup",
            testRun.getOwner(), testRun.getTestName(), testRun.getTestRunId(),
            testRun.getState());

        logger.info("{} stale {} {} {}", result.wasApplied() ? "   " : "not",
            testRun.getOwner(), testRun.getTestName(), testRun.getTestRunId());
    }

    @Override
    public void start() throws Exception
    {
        // Since cassandra-driver-mapping 3.0, enum deserializers have to be explicitly registered
        CodecRegistry.DEFAULT_INSTANCE.register(new EnumNameCodec<>(TestRun.State.class));
        CodecRegistry.DEFAULT_INSTANCE.register(new EnumNameCodec<TestCompletionNotification>(
            TestCompletionNotification.class));

        OptionalCodec<String> optionalStringCodec = new OptionalCodec<>(TypeCodec.varchar());
        CodecRegistry.DEFAULT_INSTANCE.register(optionalStringCodec);

        testRunMapper = driverManager.getMappingManager().mapper(TestRun.class);
        deletedTestRunMapper = driverManager.getMappingManager().mapper(DeletedTestRun.class);
        testRunAccessor = driverManager.getMappingManager().createAccessor(TestRunAccessor.class);
        deletedTestRunAccessor = driverManager.getMappingManager().createAccessor(DeletedTestRunAccessor.class);

        finishedTestRunMapper = driverManager.getMappingManager().mapper(FinishedTestRun.class);
        finishedTestRunAccessor = driverManager.getMappingManager().createAccessor(FinishedTestRunAccessor.class);

        if (driverManager.isSchemaCreator())
        {
            maybeAddFinishedTestRunEndStop(new Date());
        }

        isRunning = true;
    }

    public boolean isRunning()
    {
        return isRunning;
    }

    @Override
    public void stop() throws Exception
    {
        isRunning = false;
    }
}
