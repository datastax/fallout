-- if you change this file you need to update CassandraDriverManager.maybeMigrateSchema

CREATE TYPE IF NOT EXISTS resourceType (
    provider text,
    tenant text,
    instanceType text,
    uniqueName text
);

CREATE TYPE IF NOT EXISTS resourceRequirement (
    resourceType frozen<resourceType>,
    reservationLockResourceType frozen<resourceType>,
    nodeCount int
);

CREATE TYPE IF NOT EXISTS googleCloudServiceAccount (
    email text,
    project text,
    privateKeyId text,
    keyFileJson text
);

CREATE TABLE IF NOT EXISTS users (
    email text PRIMARY KEY,
    name text,
    encpass blob,
    salt    blob,
    admin   boolean,
    oauthId uuid,
    resetToken uuid,
    defaultGoogleCloudServiceAccountEmail text,
    googleCloudServiceAccounts set<frozen<googleCloudServiceAccount>>,
    emailPref text,
    slackPref text
);

CREATE TABLE IF NOT EXISTS sessions (
    tokenId uuid PRIMARY KEY,
    tokenType text,
    userId text
);

CREATE TABLE IF NOT EXISTS tests (
    owner text,
    name text,
    testId uuid,
    createdAt timestamp,
    lastRunAt timestamp,
    definition text,
    tags set<text>,
    sizeOnDiskBytes bigint,
    PRIMARY KEY (owner, name)
);

CREATE INDEX IF NOT EXISTS test_tags ON tests( tags );

CREATE TABLE IF NOT EXISTS test_runs (
    owner text,
    testName text,
    testRunId uuid,
    definition text,
    createdAt timestamp,
    startedAt timestamp,
    finishedAt timestamp,
    state text,
    failedDuring text,
    results text,
    artifacts frozen<map<text,bigint>>,
    artifactsLastUpdated timestamp,
    parsedloginfo text,
    templateParams text,
    emailPref text,
    slackPref text,
    resourceRequirements set<frozen<resourceRequirement>>,
    PRIMARY KEY ((owner,testName), testRunId)
);

CREATE INDEX IF NOT EXISTS test_runs_state on test_runs(state);

CREATE INDEX IF NOT EXISTS test_runs_testRunId on test_runs(testRunId);

CREATE TABLE IF NOT EXISTS finished_test_runs (
    owner text,
    testName text,
    testRunId uuid,
    createdAt timestamp,
    startedAt timestamp,
    finishedAtDate timestamp,
    finishedAt timestamp,
    state text,
    failedDuring text,
    resourceRequirements set<frozen<resourceRequirement>>,
    PRIMARY KEY (finishedAtDate, finishedAt, testRunId)
)
WITH CLUSTERING ORDER BY (finishedAt DESC);

CREATE TABLE IF NOT EXISTS deleted_tests (
        owner text,
        name text,
        testId uuid,
        createdAt timestamp,
        lastRunAt timestamp,
        definition text,
        tags set<text>,
        sizeOnDiskBytes bigint,
        PRIMARY KEY (owner, name)
)
WITH default_time_to_live = 2592000;

CREATE TABLE IF NOT EXISTS deleted_test_runs (
        owner text,
        testName text,
        testRunId uuid,
        definition text,
        createdAt timestamp,
        startedAt timestamp,
        finishedAt timestamp,
        state text,
        failedDuring text,
        results text,
        artifacts frozen<map<text,bigint>>,
        artifactsLastUpdated timestamp,
        parsedloginfo text,
        templateParams text,
        emailPref text,
        slackPref text,
        resourceRequirements set<frozen<resourceRequirement>>,
        PRIMARY KEY ((owner,testName), testRunId)
)
WITH default_time_to_live = 2592000;

CREATE TYPE IF NOT EXISTS testRunIdentifier (
    testOwner text,
    testName text,
    testRunId uuid
);

CREATE TABLE IF NOT EXISTS performance_reports (
    email text,
    reportGuid uuid,
    reportDate timestamp,
    reportName text,
    reportTestRuns set<frozen<testRunIdentifier>>,
    reportArtifact text,
    PRIMARY KEY (email, reportGuid)
);
