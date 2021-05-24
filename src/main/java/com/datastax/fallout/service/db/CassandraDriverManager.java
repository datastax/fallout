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

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.function.Consumer;
import java.util.function.Function;

import io.dropwizard.lifecycle.Managed;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ColumnMetadata;
import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.KeyspaceMetadata;
import com.datastax.driver.core.Metadata;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.TableMetadata;
import com.datastax.driver.core.UserType;
import com.datastax.driver.core.WriteType;
import com.datastax.driver.core.exceptions.DriverException;
import com.datastax.driver.core.exceptions.InvalidQueryException;
import com.datastax.driver.core.policies.DefaultRetryPolicy;
import com.datastax.driver.core.policies.RetryPolicy;
import com.datastax.driver.mapping.DefaultPropertyMapper;
import com.datastax.driver.mapping.Mapper;
import com.datastax.driver.mapping.MappingConfiguration;
import com.datastax.driver.mapping.MappingManager;
import com.datastax.driver.mapping.PropertyTransienceStrategy;
import com.datastax.fallout.service.core.PerformanceReport;
import com.datastax.fallout.service.core.TestRunIdentifier;
import com.datastax.fallout.util.ScopedLogger;

public class CassandraDriverManager implements Managed
{
    private static final ScopedLogger logger = ScopedLogger.getLogger(CassandraDriverManager.class);
    private Cluster cluster;
    private Session session;
    private MappingManager mappingManager;
    private final String host;
    private final int port;
    private final String keyspace;
    private final SchemaMode schemaMode;
    private final Consumer<CassandraDriverManager> preCreateSchemaCallback;
    private static final int MAX_DRIVER_RETRIES = 3;

    public enum SchemaMode
    {
        CREATE_SCHEMA,
        USE_EXISTING_SCHEMA
    }

    public CassandraDriverManager(String host, int port, String keyspace, SchemaMode schemaMode,
        Consumer<CassandraDriverManager> preCreateSchemaCallback)
    {
        this.host = host;
        this.port = port;
        this.keyspace = keyspace;
        this.schemaMode = schemaMode;
        this.preCreateSchemaCallback = preCreateSchemaCallback;
    }

    @Override
    public void start() throws Exception
    {
        cluster = Cluster.builder()
            .withClusterName(this.getClass().getSimpleName())
            .addContactPoint(host)
            .withPort(port)
            .withRetryPolicy(new RetryPolicy() {
                private RetryPolicy defaultRetryPolicy = DefaultRetryPolicy.INSTANCE;

                @Override
                public RetryDecision onReadTimeout(Statement statement, ConsistencyLevel cl, int requiredResponses,
                    int receivedResponses, boolean dataRetrieved, int nbRetry)
                {
                    logger.warn("onReadTimeout: {}  cl={}  responses={}/{}  dataRetrieved={}  retries={}",
                        statement, cl, receivedResponses, requiredResponses, dataRetrieved, nbRetry);
                    return nbRetry < MAX_DRIVER_RETRIES ? RetryDecision.retry(cl) : RetryDecision.rethrow();
                }

                @Override
                public RetryDecision onWriteTimeout(Statement statement, ConsistencyLevel cl, WriteType writeType,
                    int requiredAcks, int receivedAcks, int nbRetry)
                {
                    logger.warn("onWriteTimeout: {}  cl={}  writeType={}  acks={}/{}  retries={}",
                        statement, cl, writeType, receivedAcks, requiredAcks, nbRetry);
                    return nbRetry < MAX_DRIVER_RETRIES ? RetryDecision.retry(cl) : RetryDecision.rethrow();
                }

                @Override
                public RetryDecision onUnavailable(Statement statement, ConsistencyLevel cl, int requiredReplica,
                    int aliveReplica, int nbRetry)
                {
                    logger.warn("onUnavailable: {}  cl={}  replicas={}/{}  retries={}",
                        statement, cl, aliveReplica, requiredReplica, nbRetry);
                    return nbRetry < MAX_DRIVER_RETRIES ? RetryDecision.retry(cl) : RetryDecision.rethrow();
                }

                @Override
                public RetryDecision onRequestError(Statement statement, ConsistencyLevel cl, DriverException e,
                    int nbRetry)
                {
                    logger.warn("onRequestError: {}  cl={}  retries={}",
                        statement, cl, nbRetry);
                    logger.warn("onRequestError exception:", e);
                    return nbRetry < MAX_DRIVER_RETRIES ? RetryDecision.retry(cl) : RetryDecision.rethrow();
                }

                @Override
                public void init(Cluster cluster)
                {
                }

                @Override
                public void close()
                {
                }
            })
            .build();

        session = cluster.connect();

        if (schemaMode == SchemaMode.CREATE_SCHEMA)
        {
            preCreateSchemaCallback.accept(this);
            session.execute(String.format(
                "CREATE KEYSPACE IF NOT EXISTS %s WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};",
                keyspace));
            session.execute(String.format("USE %s", keyspace));
            createSchema();
            maybeMigrateSchema();
        }

        mappingManager = new MappingManager(session,
            MappingConfiguration.builder()
                .withPropertyMapper(new DefaultPropertyMapper()
                    .setPropertyTransienceStrategy(PropertyTransienceStrategy.OPT_IN))
                .build());

        if (schemaMode == SchemaMode.USE_EXISTING_SCHEMA)
        {
            try
            {
                session.execute(String.format("USE %s;", keyspace));
            }
            catch (InvalidQueryException e)
            {
                throw new RuntimeException("fallout schema seems to be missing from this C* cluster", e);
            }
        }
    }

    /** Return whether this client should perform schema mutations and migrations */
    public boolean isSchemaCreator()
    {
        return schemaMode == SchemaMode.CREATE_SCHEMA;
    }

    /**
     * Reads schema and issues commands to C*
     *
     * File can look like this:
     *
     * #This is a comment
     *
     * CREATE TABLE BAR(foo text PRIMARY KEY)
     * WITH compression = {};
     *
     * ...
     */
    private void createSchema()
    {
        try
        {
            InputStream cql = CassandraDriverManager.class.getClassLoader().getResourceAsStream("schema.cql");
            if (cql == null)
            {
                throw new RuntimeException("Missing schema cql file.");
            }

            executeCqlFile(new InputStreamReader(cql));
        }
        catch (Exception e)
        {
            e.printStackTrace();
            System.exit(2); //Fail hard
        }
    }

    private void maybeMigrateSchema()
    {
        Map<String, String> usersColumns = new HashMap<>();
        usersColumns.put("openstackusername", "text");
        usersColumns.put("openstackpassword", "text");
        usersColumns.put("openstacktenantname", "text");
        usersColumns.put("ironictenantname", "text");
        usersColumns.put("nebulaProjectName", "text");
        usersColumns.put("nebulaAppCreds", "set<frozen<nebulaAppCred>>");
        usersColumns.put("automatonSharedHandle", "text");
        usersColumns.put("emailPref", "text");
        usersColumns.put("slackPref", "text");
        usersColumns.put("defaultGoogleCloudServiceAccountEmail", "text");
        usersColumns.put("googleCloudServiceAccounts", "set<frozen<googleCloudServiceAccount>>");
        usersColumns.put("defaultAstraServiceAccountName", "text");
        usersColumns.put("astraServiceAccounts", "set<frozen<astraServiceAccount>>");
        usersColumns.put("defaultBackupServiceCred", "text");
        usersColumns.put("backupServiceCreds", "set<frozen<backupServiceCred>>");
        usersColumns.put("dockerRegistryCredentials", "set<frozen<dockerRegistryCredential>>");

        Map<String, String> testRunsColumns = new HashMap<>();
        testRunsColumns.put("parsedloginfo", "text");
        testRunsColumns.put("createdAt", "timestamp");
        testRunsColumns.put("failedDuring", "text");
        testRunsColumns.put("definition", "text");
        testRunsColumns.put("templateParams", "text");
        testRunsColumns.put("emailPref", "text");
        testRunsColumns.put("slackPref", "text");
        testRunsColumns.put("resourceRequirements", "set<frozen<resourceRequirement>>");
        testRunsColumns.put("artifacts", "frozen<map<text,bigint>>");
        testRunsColumns.put("artifactsLastUpdated", "timestamp");
        testRunsColumns.put("links", "frozen<map<text,text>>");

        Map<String, String> deletedTestRunsColumns = new HashMap<>();
        deletedTestRunsColumns.put("emailPref", "text");
        deletedTestRunsColumns.put("slackPref", "text");
        deletedTestRunsColumns.put("resourceRequirements", "set<frozen<resourceRequirement>>");
        deletedTestRunsColumns.put("artifacts", "frozen<map<text,bigint>>");
        deletedTestRunsColumns.put("artifactsLastUpdated", "timestamp");
        deletedTestRunsColumns.put("links", "frozen<map<text,text>>");

        Map<String, String> finishedTestRunsColumns = new HashMap<>();
        finishedTestRunsColumns.put("resourceRequirements", "set<frozen<resourceRequirement>>");

        Map<String, String> testsColumns = new HashMap<>();
        testsColumns.put("tags", "set<text>");
        testsColumns.put("lastRunAt", "timestamp");
        testsColumns.put("sizeOnDiskBytes", "bigint");

        Map<String, String> deletedTestsColumns = new HashMap<>();
        deletedTestsColumns.put("lastRunAt", "timestamp");
        deletedTestsColumns.put("sizeOnDiskBytes", "bigint");

        Map<String, Map<String, String>> migrations = new HashMap<>();
        migrations.put("users", usersColumns);
        migrations.put("test_runs", testRunsColumns);
        migrations.put("deleted_test_runs", deletedTestRunsColumns);
        migrations.put("finished_test_runs", finishedTestRunsColumns);
        migrations.put("tests", testsColumns);
        migrations.put("deleted_tests", deletedTestsColumns);

        KeyspaceMetadata keyspaceMeta = session.getCluster().getMetadata().getKeyspace(keyspace);
        for (Map.Entry<String, Map<String, String>> table : migrations.entrySet())
        {
            String tableName = table.getKey();
            Map<String, String> tableColumns = table.getValue();
            TableMetadata tableMeta = keyspaceMeta.getTable(tableName);
            for (Map.Entry<String, String> tableColumn : tableColumns.entrySet())
            {
                String columnName = tableColumn.getKey();
                String columnType = tableColumn.getValue();
                final ColumnMetadata columnMeta = tableMeta.getColumn(columnName);

                final Function<String, String> normalizeTypeName = typeName -> typeName
                    .toLowerCase()
                    .replaceAll("\\s", "")
                    .replaceAll(keyspace + "\\.", "");

                if (columnMeta == null)
                {
                    session.execute(String.format("ALTER TABLE \"%s\".\"%s\" ADD %s %s;", keyspace, tableName,
                        columnName, columnType));
                }
                else if (!normalizeTypeName.apply(columnMeta.getType().toString())
                    .equals(normalizeTypeName.apply(columnType)))
                {
                    // Don't do anything fancy, just blow up and let the user sort it out.
                    throw new RuntimeException(String.format("%s.%s column %s has wrong type %s != %s",
                        keyspace, tableName, columnName, columnMeta.getType(), columnType));
                }
            }
        }

        // ALTER UDT
        // field names are returned from C* as all lower case
        if (!keyspaceMeta.getUserType("resourceType").getFieldNames().contains("uniquename"))
        {
            session.execute(String.format("ALTER TYPE %s.resourceType ADD uniqueName text;", keyspace));
        }

        Map<String, String> dropUsersColumns = new HashMap<>();
        dropUsersColumns.put("defaultCaasUsername", "text");
        dropUsersColumns.put("caasCreds", "set<frozen<caasCred>>");
        dropUsersColumns.put("rightscaleEmail", "text");
        dropUsersColumns.put("rightscalePassword", "text");
        dropUsersColumns.put("rightscaleAccountId", "int");
        dropUsersColumns.put("privateRightscaleKey", "text");

        Map<String, String> dropTestRunsColumns = new HashMap<>();
        dropTestRunsColumns.put("deletedAt", "timestamp");

        Map<String, String> dropTestsColumns = new HashMap<>();
        dropTestsColumns.put("deletedAt", "timestamp");

        Map<String, Map<String, String>> dropMigrations = new HashMap<>();
        dropMigrations.put("users", dropUsersColumns);
        dropMigrations.put("test_runs", dropTestRunsColumns);
        dropMigrations.put("tests", dropTestsColumns);

        for (Map.Entry<String, Map<String, String>> table : dropMigrations.entrySet())
        {
            String tableName = table.getKey();
            Map<String, String> tableColumns = table.getValue();
            TableMetadata tableMeta = keyspaceMeta.getTable(tableName);
            for (Map.Entry<String, String> tableColumn : tableColumns.entrySet())
            {
                String columnName = tableColumn.getKey();
                if (tableMeta.getColumn(columnName) != null)
                {
                    session
                        .execute(String.format("ALTER TABLE \"%s\".\"%s\" DROP %s;", keyspace, tableName, columnName));
                }
            }
        }

        // ALTER TABLE RENAME
        Map<String, String> testRunRename = new HashMap<>();
        testRunRename.put("name", "testName");

        Map<String, Map<String, String>> renameMigrations = new HashMap<>();
        renameMigrations.put("test_runs", testRunRename);

        for (Map.Entry<String, Map<String, String>> tableRename : renameMigrations.entrySet())
        {
            String tableName = tableRename.getKey();
            Map<String, String> renameColumns = tableRename.getValue();
            TableMetadata tableMeta = keyspaceMeta.getTable(tableName);
            for (Map.Entry<String, String> renameColumn : renameColumns.entrySet())
            {
                String oldName = renameColumn.getKey();
                String newName = renameColumn.getValue();
                if (tableMeta.getColumn(oldName) != null)
                {
                    session.execute(String.format("ALTER TABLE \"%s\".\"%s\" RENAME %s TO %s;", keyspace, tableName,
                        oldName, newName));
                }
            }
        }

        TableMetadata oldMigrationsTable = keyspaceMeta.getTable("migrations");
        if (oldMigrationsTable != null)
        {
            // drop table that was used for schema migrations in older fallout versions
            session.execute(String.format("DROP TABLE IF EXISTS \"%s\".\"%s\";", keyspace, "migrations"));
        }

        TableMetadata oldClusterTable = keyspaceMeta.getTable("cluster");
        if (oldClusterTable != null)
        {
            session.execute(String.format("DROP TABLE IF EXISTS \"%s\".\"%s\";", keyspace, "cluster"));
        }

        UserType caasCredsType = keyspaceMeta.getUserType("caasCred");
        if (caasCredsType != null)
        {
            dropUserType(session, caasCredsType);
        }

        UserType oldNodeType = keyspaceMeta.getUserType("node");
        if (oldNodeType != null)
        {
            dropUserType(session, oldNodeType);
        }

        TableMetadata oldRegressionsTable = keyspaceMeta.getTable("regression_tests");
        if (oldRegressionsTable != null)
        {
            session.execute(String.format("DROP TABLE IF EXISTS \"%s\".\"%s\";", keyspace, "regression_tests"));
        }

        TableMetadata oldDashboardsTable = keyspaceMeta.getTable("dashboards");
        if (oldDashboardsTable != null)
        {
            session.execute(String.format("DROP TABLE IF EXISTS \"%s\".\"%s\";", keyspace, "dashboards"));
        }

        UserType testRunDataType = keyspaceMeta.getUserType("testrundata");
        if (testRunDataType != null)
        {
            dropUserType(session, testRunDataType);
        }

        TableMetadata testsMeta = keyspaceMeta.getTable("tests");
        if (testsMeta != null && testsMeta.getColumn("dashboards") != null)
        {
            session.execute("ALTER TABLE tests DROP dashboards");
        }

        // Migrate data type of performance_reports reportTests -> reportTestRuns
        // Only perform migration if performance_reports does not contain the reportTestRuns column
        TableMetadata perfReportsMeta = keyspaceMeta.getTable("performance_reports");
        if (perfReportsMeta.getColumn("reportTestRuns") == null)
        {
            session.execute("ALTER TABLE performance_reports ADD reportTestRuns set<frozen<testRunIdentifier>>");
            List<Row> perfRows = session.execute("SELECT * FROM performance_reports").all();
            logger.info("Found {} performance reports", perfRows.size());

            Mapper<PerformanceReport> performanceReportMapper =
                new MappingManager(getSession()).mapper(PerformanceReport.class);

            PreparedStatement testRunLookupPreparedStatement =
                session.prepare("SELECT owner FROM test_runs WHERE testRunId=:testRunId ALLOW FILTERING");

            for (Row row : perfRows)
            {
                String reportOwner = row.getString("email");
                UUID reportId = row.getUUID("reportGuid");

                // Map<testRunId, testName>
                Map<String, String> reportTests = row.getMap("reportTests", String.class, String.class);

                Set<TestRunIdentifier> reportTestRuns = new HashSet<>();

                reportTests.forEach((testRunId, testName) -> {
                    UUID testRunUUID = UUID.fromString(testRunId);
                    BoundStatement boundTestRunLookup =
                        testRunLookupPreparedStatement.bind().setUUID("testRunId", testRunUUID);
                    Row testRun = session.execute(boundTestRunLookup).one();

                    if (testRun == null)
                    {
                        logger.error("Found test run in report which has been deleted. TestName {} TestRunId {}",
                            testName, testRunUUID);
                        reportTestRuns.add(new TestRunIdentifier("Unknown", testName, testRunUUID));
                    }
                    else
                    {
                        reportTestRuns.add(new TestRunIdentifier(testRun.getString("owner"), testName, testRunUUID));
                    }
                });

                PerformanceReport migratedPerformanceReport = new PerformanceReport();
                migratedPerformanceReport.setEmail(reportOwner);
                migratedPerformanceReport.setReportGuid(reportId);
                migratedPerformanceReport.setReportDate(row.getTimestamp("reportDate"));
                migratedPerformanceReport.setReportName(row.getString("reportName"));
                migratedPerformanceReport.setReportTestRuns(reportTestRuns);
                migratedPerformanceReport.setReportArtifact(row.getString("reportArtifact"));

                performanceReportMapper.save(migratedPerformanceReport);
            }

            session.execute("ALTER TABLE performance_reports DROP reportTests");
            logger.info("Successfully migrated performance_reports");
        }
    }

    private void dropUserType(Session session, UserType userType)
    {
        String quotedTypeName = Metadata.quoteIfNecessary(userType.getKeyspace()) + "." +
            Metadata.quoteIfNecessary(userType.getTypeName());
        session.execute(String.format("DROP TYPE %s;", quotedTypeName));
    }

    private void executeCqlFile(InputStreamReader cqlStream) throws IOException
    {
        BufferedReader lines = new BufferedReader(cqlStream);
        StringBuilder sb = new StringBuilder();
        String line;
        while ((line = lines.readLine()) != null)
        {
            String trimmedLine = line.trim();
            if (trimmedLine.startsWith("--") || trimmedLine.startsWith("//") || trimmedLine.isEmpty())
            {
                continue;
            }

            sb.append(line);

            if (trimmedLine.endsWith(";"))
            {
                String cqlCmd = sb.toString();
                logger.info("Executing CQL: " + cqlCmd);
                session.execute(cqlCmd);
                sb = new StringBuilder();
            }
        }
    }

    @Override
    public void stop() throws Exception
    {
        if (session != null)
        {
            session.close();
        }

        if (cluster != null)
        {
            cluster.close();
        }
    }

    public String getKeySpace()
    {
        return keyspace;
    }

    public Session getSession()
    {
        return session;
    }

    public MappingManager getMappingManager()
    {
        return mappingManager;
    }
}
