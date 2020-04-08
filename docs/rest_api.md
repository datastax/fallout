# REST API

Fallout can be controlled via a REST API consisting of several relatively simple endpoints in the package [`com.datastax.fallout.service.resources`](../src/main/java/com/datastax/fallout/service/resources).

## Creating and running tests

Definitions are in [`TestResource`](../src/main/java/com/datastax/fallout/service/resources/server/TestResource.java):

| Path | Verb | Request Body | Response | Description |
|------|------|--------------|----------|-------------|
|`/tests/{name}/api` | **POST** | Name should be a friendly name for the test and payload is a test YAML with mime type "application/yaml". | 201 with redirect to resource | Creates a test |
|`/tests/{name}/api` | **PUT** | Name is the friendly name of the test to be updated or created, and payload is a test YAML with mime type "application/yaml". | 200 if update, 201 if create, with redirect to resource | Updates a test, or creates it if it doesn't exist |
|`/tests/api` | **GET** | | 200 with JSON list of tests owned by authenticated user | Return test list |
|`/tests/{userEmail}/api` | **GET** | | 200 with JSON list of tests owned by the specified user| Return test list|
|`/tests/{name}/api` | **GET** | | 200 with JSON object representing the specified test. This only works for tests owned by the authenticated user|Return test|
|`/tests/{userEmail}/{name}/api` | **GET** | | 200 with JSON object representing the specified test. The test must be owned by the user with specified email| Return test |
|`/tests/{name}/api` | **DELETE** | | 200. This only works if the test is owned by the authenticated user | Deletes a test, and its testruns |
|`/tests/{name}/runs/api` | **POST** | | 201 with redirect to resource | Creates a test run |
|`/tests/{userEmail}/{name}/runs/api` | **GET** | | 200 | Returns list of runs for a test|
|`/tests/{userEmail}/{name}/runs/{testRunId}/api` | **GET** | | 200 | Returns test run |
|`/tests/{userEmail}/{name}/runs/{testRunId}/api` | **DELETE** | 200 | Deletes test run and related artifacts if the test is owned by the authenticated user and has finished execution|
|`/tests/{userEmail}/{name}/runs/{testRunId}/abort/api` | **POST** | | 200 | Aborts test run if the test is owned by the authenticated user|
|`/tests/runs/{testRunId}/api` | **GET** | | 200 | Returns test run |


## Performance test comparisons

Fallout also supports performance testing via the Stress Module.  If you wish to generate comparisons of one or more test runs the following REST API is available:

Definitions are in [`PerformanceToolResource`](../src/main/java/com/datastax/fallout/service/resources/server/PerformanceToolResource.java):

| Path | Verb | Request Body | Response | Description |
|------|------|--------------|----------|-------------|
|`/performance/run/api` | **POST** | application/json: `{ 'reportName' : 'MyReport', 'reportTests' : {'test_run_1' : ['uuid1'], 'test_run_2' : ['uuid1', 'uuid2']} }` | 200 and performance test details | Details of the report run and location of the artifacts |
|`/performance/{userEmail}/api` | **GET** | | 200 with a list of all performance runs | Fetches all performance runs for a user|
|`/performance/{userEmail}/report/{reportId}/api` | **GET** | |200 and the report details | Fetches a single performance run for a user|
|`/performance/{userEmail}/report/{reportId}/api` | **DELETE** | | 200 | Deletes a performance run|


All **POST** and **DELETE** endpoints will only be available when authenticated as the user who created the test. In the case of test creation, the test will be owned by the user whose credentials are provided to make the initial **POST** request. **GET** endpoints scoped by *userEmail* are provided to make it easy to share read-only access to tests and test runs.
