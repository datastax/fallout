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

import java.util.List;

import com.google.common.collect.ImmutableList;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.experimental.categories.Category;

import com.datastax.driver.core.ColumnMetadata;
import com.datastax.driver.core.Session;
import com.datastax.fallout.service.core.DeletedTest;
import com.datastax.fallout.service.core.Test;
import com.datastax.fallout.test.utils.categories.RequiresDb;

import static com.datastax.fallout.service.db.CassandraDriverManagerHelpers.createDriverManager;
import static org.assertj.core.api.Assertions.assertThat;

@Category(RequiresDb.class)
public class TestDAOTest
{
    private static final String keyspace = "test_dao";
    private static Session session;
    private static CassandraDriverManager driverManager;
    private static TestDAO testDAO;
    private static TestRunDAO testRunDAO;
    private static final String OWNER = "owner";
    private static final String NAME = "testName";

    @BeforeClass
    public static void startCassandra() throws Exception
    {
        driverManager = createDriverManager(keyspace);

        testRunDAO = new TestRunDAO(driverManager);
        testDAO = new TestDAO(driverManager, testRunDAO);

        driverManager.start();
        testRunDAO.start();
        testDAO.start();

        session = driverManager.getSession();
    }

    @AfterClass
    public static void stopCassandra() throws Exception
    {
        testDAO.stop();
        testRunDAO.stop();
        driverManager.stop();
    }

    private Test createSavedTest()
    {
        // These tests do not need a valid definition
        final Test test = Test.createTest(OWNER, NAME, null);
        testDAO.update(test);

        return test;
    }

    @Before
    public void setUp()
    {
        ImmutableList.of("tests", "deleted_tests")
            .forEach(table -> session.execute(String.format("truncate %s.%s;", keyspace, table)));
    }

    @org.junit.Test
    public void delete_test_that_does_not_exist_does_not_throw()
    {
        testDAO.drop(OWNER, NAME);
    }

    @org.junit.Test
    public void test_moved_to_deleted_tests()
    {
        Test test = createSavedTest();
        assertThat(testDAO.getAllDeleted(OWNER)).doesNotContain(DeletedTest.fromTest(test));
        testDAO.drop(test.getOwner(), test.getName());

        assertThat(testDAO.getAllDeleted(OWNER)).contains(DeletedTest.fromTest(test));
        assertThat(testDAO.getAll(OWNER)).doesNotContain(DeletedTest.fromTest(test));
    }

    @org.junit.Test
    public void test_deleted_test_schema_equal()
    {
        List<ColumnMetadata> cm =
            session.getCluster().getMetadata().getKeyspace(keyspace).getTable("tests").getColumns();
        List<ColumnMetadata> cm2 =
            session.getCluster().getMetadata().getKeyspace(keyspace).getTable("deleted_tests").getColumns();

        for (ColumnMetadata c : cm)
        {
            assertThat(c).isIn(cm2);
        }
    }

}
