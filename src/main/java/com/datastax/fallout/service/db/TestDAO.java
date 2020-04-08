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
package com.datastax.fallout.service.db;

import java.util.ArrayList;
import java.util.List;

import io.dropwizard.lifecycle.Managed;

import com.datastax.driver.core.BatchStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.mapping.Mapper;
import com.datastax.driver.mapping.Result;
import com.datastax.driver.mapping.annotations.Accessor;
import com.datastax.driver.mapping.annotations.Query;
import com.datastax.fallout.service.core.DeletedTest;
import com.datastax.fallout.service.core.Test;
import com.datastax.fallout.service.core.TestRun;
import com.datastax.fallout.util.ScopedLogger;

public class TestDAO implements Managed
{
    private static final ScopedLogger logger = ScopedLogger.getLogger(TestDAO.class);

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

    private boolean writeNewSizeToDB(Test test, Long oldSize, Long newSize)
    {
        return complexTestAccessor.updateTestSize(newSize, test.getOwner(), test.getName(), oldSize)
            .wasApplied();
    }

    public void changeSizeOnDiskBytes(TestRun testRun)
    {
        changeSizeOnDiskBytes(testRun, testRun.getArtifactsSizeBytes().orElse(0L));
    }

    public void changeSizeOnDiskBytes(TestRun testRun, Long change)
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
            applied = writeNewSizeToDB(test, oldSize, newSize);
        }
    }

    public void drop(String owner, String name)
    {
        Test test = get(owner, name);
        if (test == null)
        {
            return;
        }
        DeletedTest deletedtest = DeletedTest.fromTest(test);

        BatchStatement batchStatement = new BatchStatement();
        batchStatement.add(testMapper.deleteQuery(test));
        batchStatement.add(deletedTestMapper.saveQuery(deletedtest));
        driverManager.getSession().execute(batchStatement);
    }

    public void restore(DeletedTest deletedTest)
    {
        BatchStatement batchStatement = new BatchStatement();
        batchStatement.add(deletedTestMapper.deleteQuery(deletedTest));
        batchStatement.add(testMapper.saveQuery(deletedTest));
        driverManager.getSession().execute(batchStatement);
    }

    public void delete(String owner, String name)
    {
        Test test = getEvenIfDeleted(owner, name);
        testMapper.delete(test);
        deletedTestMapper.delete(DeletedTest.fromTest(test));
    }

    private void ensureLastRunAtIsPopulated()
    {
        Result<Test> tests = complexTestAccessor.getAll();

        try (ScopedLogger.Scoped ignored = logger.scopedInfo("Ensuring Test.lastRunAt is populated"))
        {
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
        }
    }

    @Override
    public void start() throws Exception
    {
        testMapper = driverManager.getMappingManager().mapper(Test.class);
        deletedTestMapper = driverManager.getMappingManager().mapper(DeletedTest.class);
        complexTestAccessor = driverManager.getMappingManager().createAccessor(TestAccessor.class);
        deletedTestAccessor = driverManager.getMappingManager().createAccessor(DeletedTestAccessor.class);

        if (driverManager.isSchemaCreator())
        {
            ensureLastRunAtIsPopulated();
            maybePopulateTestSizeOndisk();
        }
    }

    private void maybePopulateTestSizeOndisk()
    {
        complexTestAccessor.getAll().all().stream()
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
