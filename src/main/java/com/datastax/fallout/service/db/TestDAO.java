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
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.locks.ReentrantLock;

import io.dropwizard.lifecycle.Managed;
import org.apache.commons.lang3.tuple.Pair;

import com.datastax.driver.core.BatchStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.mapping.Mapper;
import com.datastax.driver.mapping.Result;
import com.datastax.driver.mapping.annotations.Accessor;
import com.datastax.driver.mapping.annotations.Query;
import com.datastax.fallout.service.core.DeletedTest;
import com.datastax.fallout.service.core.DeletedTestRun;
import com.datastax.fallout.service.core.Test;
import com.datastax.fallout.service.core.TestRun;
import com.datastax.fallout.service.core.TestRunIdentifier;
import com.datastax.fallout.util.LockHolder;
import com.datastax.fallout.util.ScopedLogger;

public class TestDAO implements Managed
{
    private static final ScopedLogger logger = ScopedLogger.getLogger(TestDAO.class);

    /** Lock used for operations that must be atomic with respect to deleted/not-deleted Test and TestRun tables */
    private final ReentrantLock deleteRestoreLock = new ReentrantLock();
    private Duration deletedTtl;

    @Accessor
    private interface TestAccessor
    {
        @Query("SELECT * FROM tests")
        Result<Test> getAll();

        @Query("SELECT * FROM tests WHERE owner = :ownerEmail")
        Result<Test> getAll(String ownerEmail);

        @Query("SELECT * FROM tests WHERE owner = :ownerEmail AND name = :name")
        Result<Test> get(String ownerEmail, String name);

        @Query("SELECT * FROM tests WHERE tags CONTAINS :tag")
        Result<Test> getTestFromTag(String tag);

        @Query("UPDATE tests SET sizeOnDiskBytes = :newSize " +
            "WHERE owner= :owner AND name = :name IF sizeOnDiskBytes = :oldSize")
        ResultSet updateTestSize(Long newSize, String ownerEmail, String name, Long oldSize);
    }

    @Accessor
    private interface DeletedTestAccessor
    {
        @Query("SELECT * FROM deleted_tests WHERE owner = :owner")
        Result<DeletedTest> getAll(String owner);

        @Query("SELECT * FROM deleted_tests")
        Result<DeletedTest> getAll();
    }

    private final CassandraDriverManager driverManager;
    private Mapper<Test> testMapper;
    private Mapper<DeletedTest> deletedTestMapper;
    private TestAccessor complexTestAccessor;
    private DeletedTestAccessor deletedTestAccessor;
    private final TestRunDAO testRunDAO;

    public TestDAO(CassandraDriverManager driverManager, TestRunDAO testRunDAO)
    {
        this.driverManager = driverManager;
        this.testRunDAO = testRunDAO;
    }

    public Test get(String owner, String name)
    {
        return testMapper.get(owner, name);
    }

    public Test getEvenIfDeleted(String owner, String name)
    {
        Test test = testMapper.get(owner, name);
        if (test == null)
        {
            return deletedTestMapper.get(owner, name);
        }
        return test;
    }

    public DeletedTest getDeleted(String owner, String name)
    {
        return deletedTestMapper.get(owner, name);
    }

    public List<Test> getAll(String owner)
    {
        return complexTestAccessor.getAll(owner).all();
    }

    public List<DeletedTest> getAllDeleted(String owner)
    {
        return deletedTestAccessor.getAll(owner).all();
    }

    public List<DeletedTest> getAllDeleted()
    {
        return deletedTestAccessor.getAll().all();
    }

    public List<Test> getTestsFromTag(List<String> tags)
    {
        List<List<Test>> superSet = new ArrayList<>();
        for (String tag : tags)
        {
            superSet.add(complexTestAccessor.getTestFromTag(tag).all());
        }
        List<Test> results = superSet.get(0);

        if (superSet.size() == 1)
            return results;

        for (int i = 1; i < superSet.size(); i++)
        {
            results.retainAll(superSet.get(i));
        }
        return results;
    }

    public void add(Test test)
    {
        testMapper.save(test);
    }

    public void update(Test test)
    {
        testMapper.save(test);
    }

    public void updateLastRunAt(TestRun testRun)
    {
        Test test = complexTestAccessor.get(testRun.getOwner(), testRun.getTestName()).one();
        test.setLastRunAt(testRun.getCreatedAt());
        update(test);
    }

    public void increaseSizeOnDiskBytesByTestRunSize(TestRun testRun)
    {
        changeSizeOnDiskBytes(testRun, testRun.getArtifactsSizeBytes().orElse(0L));
    }

    private void changeSizeOnDiskBytes(TestRun testRun, long change)
    {
        if (change == 0L)
        {
            return;
        }

        boolean applied = false;
        while (!applied)
        {
            Test test = complexTestAccessor.get(testRun.getOwner(), testRun.getTestName()).one();
            Long oldSize = test.getSizeOnDiskBytes();
            Long newSize = oldSize != null ? oldSize + change : change;
            applied = complexTestAccessor.updateTestSize(newSize, test.getOwner(), test.getName(), oldSize)
                .wasApplied();
        }
    }

    // ----------------------------------------------------------------------------------------------------------------
    // The following methods will also change the overall test size, so they must happen atomically with
    // respect to one another.  It would have been possible to handle this using a CQL batch, but for
    // the requirement that updateTestRunArtifacts must handle DeletedTestRuns (which do not require
    // Test.sizeOnDiskBytes to be updated) as well as TestRuns (which do require Test.sizeOnDiskBytes to be updated).

    public void deleteTestAndTestRuns(Test test)
    {
        try (var lockHolder = LockHolder.acquire(deleteRestoreLock))
        {
            testRunDAO.deleteAll(test.getOwner(), test.getName());
            delete(test);
        }
    }

    public void restoreTestAndTestRuns(DeletedTest deletedTest)
    {
        try (var lockHolder = LockHolder.acquire(deleteRestoreLock))
        {
            restore(deletedTest);
            testRunDAO.restoreAll(deletedTest.getOwner(), deletedTest.getName());
        }
    }

    public void deleteTestRun(TestRun testRun)
    {
        try (var lockHolder = LockHolder.acquire(deleteRestoreLock))
        {
            testRunDAO.delete(testRun);
            changeSizeOnDiskBytes(testRun, -testRun.getArtifactsSizeBytes().orElse(0L));
        }
    }

    public void restoreTestRun(DeletedTestRun deletedTestRun)
    {
        try (var lockHolder = LockHolder.acquire(deleteRestoreLock))
        {
            testRunDAO.restore(deletedTestRun);

            DeletedTest deletedTest = getDeleted(deletedTestRun.getOwner(), deletedTestRun.getTestName());
            if (deletedTest != null)
            {
                deletedTest.setSizeOnDiskBytes(0L);
                restore(deletedTest);
            }
            increaseSizeOnDiskBytesByTestRunSize(deletedTestRun);
        }
    }

    /** Updates the testRun with the artifacts and updates overall size in the corresponding {@link Test}.
     *  Returns the original and new sizes, or Optional.empty() if the testrun no longer exists */
    public Optional<Pair<Long, Long>> updateTestRunArtifacts(
        TestRunIdentifier testRunIdentifier, Map<String, Long> artifacts)
    {
        try (var lockHolder = LockHolder.acquire(deleteRestoreLock))
        {
            return Optional.ofNullable(testRunDAO.getEvenIfDeleted(testRunIdentifier)).map(testRun -> {
                final var oldSize = testRun.getArtifactsSizeBytes().orElse(0L);

                final var newSize = testRun.updateArtifacts(artifacts);
                final var change = newSize - oldSize;

                testRunDAO.updateArtifactsEvenIfDeleted(testRun);

                if (!(testRun instanceof DeletedTestRun))
                {
                    changeSizeOnDiskBytes(testRun, change);
                }

                return Pair.of(oldSize, newSize);
            });
        }
    }

    // ----------------------------------------------------------------------------------------------------------------

    private void delete(Test test)
    {
        DeletedTest deletedtest = DeletedTest.fromTest(test);

        BatchStatement batchStatement = new BatchStatement();
        batchStatement.add(testMapper.deleteQuery(test));
        batchStatement.add(deletedTestMapper.saveQuery(deletedtest));
        driverManager.getSession().execute(batchStatement);
    }

    private void restore(DeletedTest deletedTest)
    {
        BatchStatement batchStatement = new BatchStatement();
        batchStatement.add(deletedTestMapper.deleteQuery(deletedTest));
        batchStatement.add(testMapper.saveQuery(deletedTest));
        driverManager.getSession().execute(batchStatement);
    }

    public void deleteForever(String owner, String name)
    {
        Test test = getEvenIfDeleted(owner, name);
        testMapper.delete(test);
        deletedTestMapper.delete(DeletedTest.fromTest(test));
    }

    private void ensureLastRunAtIsPopulated()
    {
        Result<Test> tests = complexTestAccessor.getAll();

        logger.withScopedInfo("Ensuring Test.lastRunAt is populated").run(() -> {
            for (Test test : tests)
            {
                if (test.getLastRunAt() == null)
                {
                    Result<TestRun> testRuns = testRunDAO.testRunAccessor.getAll(test.getOwner(), test.getName());
                    for (TestRun testRun : testRuns)
                    {
                        if (test.getLastRunAt() == null || testRun.getCreatedAt().compareTo(test.getLastRunAt()) > 0)
                        {
                            logger.info("{} <- {}", test.getName(), testRun.getCreatedAt());
                            test.setLastRunAt(testRun.getCreatedAt());
                        }
                    }
                    testMapper.save(test, Mapper.Option.saveNullFields(false));
                }
            }
        });
    }

    public Duration deletedTtl()
    {
        return deletedTtl;
    }

    @Override
    public void start() throws Exception
    {
        testMapper = driverManager.getMappingManager().mapper(Test.class);
        deletedTestMapper = driverManager.getMappingManager().mapper(DeletedTest.class);
        complexTestAccessor = driverManager.getMappingManager().createAccessor(TestAccessor.class);
        deletedTestAccessor = driverManager.getMappingManager().createAccessor(DeletedTestAccessor.class);

        deletedTtl = Duration.ofSeconds(deletedTestMapper.getTableMetadata().getOptions().getDefaultTimeToLive());

        if (driverManager.isSchemaCreator())
        {
            ensureLastRunAtIsPopulated();
            maybePopulateTestSizeOndisk();
        }
    }

    private void maybePopulateTestSizeOndisk()
    {
        complexTestAccessor.getAll()
            .forEach(test -> {
                if (test.getSizeOnDiskBytes() == null)
                {
                    Long sizeOnDiskBytes = testRunDAO.getAll(test.getOwner(), test.getName())
                        .stream()
                        // To make keeping track of totals easier, only look at finished runs
                        .filter(testRun -> testRun.getState().finished())
                        .mapToLong(testRun -> testRun.getArtifactsSizeBytes().orElse(0L))
                        .sum();
                    test.setSizeOnDiskBytes(sizeOnDiskBytes);
                    update(test);
                }
            });
    }

    @Override
    public void stop() throws Exception
    {

    }
}
