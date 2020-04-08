# Fallout

Fallout is a tool for running local or large scale remote based distributed correctness, verification and performance tests. Fallout is run in production at DataStax facilitating mission critical testing of Apache Cassandra (TM) and DSE.

This is a preview release of the Fallout project, intended to gauge external interest. The core framework and application are present, but many of the components used at DataStax have been removed due to their reliance on internal infrastructure. The components contained in this release are focused on the Kubernetes integration with Fallout for performance testing DataStax Kubernetes operator managed clusters.

You can learn more about Fallout by reading the [docs](docs). Example tests can be found under [examples](examples).

Getting Started
------------------
### Docker Compose

Running:

```
./gradlew runServerInDocker
```

...will:

* run the `dockerTagLatest` task, which will run `docker` to build a new docker image using [`docker/Dockerfile`](docker/Dockerfile) and tag it as `datastax.com/fallout:latest`;
* run the `generateDockerCompose` task to update [`docker-compose.yml`](docker-compose.yml) using [`docker/docker-compose.yml.mustache`](docker/docker-compose.yml.mustache) as input;
* run `docker-compose up` on the updated `docker-compose.yml`.

Press `<RETURN>` to terminate the server. (Can take a minute on linux)

The `docker-compose.yml` defines two volumes to persist the fallout artifacts and cassandra data, so the next time you start, the data will still be there.

As [explained in detail below](#default-admin-user), you can create an initial admin user by setting `FALLOUT_ADMIN_CREDS` the first time you run fallout:

```
FALLOUT_AUTH_MODE=SINGLE_USER FALLOUT_ADMIN_CREDS=admin:admin@fallout.email:admin ./gradlew runServerInDocker
```

Note that the docker image contains tools for running k8s-based testruns. These tools are downloaded and packaged by gradle in the `getExternalTools` task.  The downloads will be cached by the gradle build cache, so it should be possible to avoid repetitive downloads by sharing the build cache in CI.

KIND is not installed when building with docker, thus the `kind` provisioner will not work. To use KIND, run Fallout in development mode as below.

### Building Directly

#### Prerequisites

You will need:

* an installation of Java 11 to build and run the server: set the environment variables `JAVA_HOME` and `JAVA11_HOME` to point at this;
* an installation of Java 8 to run Cassandra (the backend database) and some of the tests: set `JAVA8_HOME` to point at this;

In addition, Fallout relies on external tools for some functionality. The following must be installed and executable by the user running Fallout:
- [gcloud](https://cloud.google.com/sdk/docs/downloads-versioned-archives)
- [kubectl](https://kubernetes.io/docs/tasks/tools/install-kubectl/#install-kubectl-on-linux)
- [kind v.0.7.0](https://kind.sigs.k8s.io/docs/user/quick-start/)

Last you will need to define a simple configuration, `fallout.yml`:
```yaml
keyspace: fallout
```

#### Development

To run Fallout locally in development mode:
```
./gradlew runServer
```
Register an account at `http://localhost:8080/a/pages/register.html` and setup your user credentials.

In order to run tests on GKE, a service account json must be added to the user profile.

Navigate to `http://localhost:8080/tests/tools/create` to create your first test. The editor is pre-populated with a [default example test definition](src/main/resources/com/datastax/fallout/service/resources/server/default-test-definition.yaml).

If you want to run Fallout proxied by nginx (which will be used to serve test artifacts instead of Fallout itself):

```
./gradlew runServerWithNginx
```

#### Unit Tests

Run the tests like this:

```
./gradlew :test
```

Run individual tests using the `--tests` option:

```
./gradlew :test --tests '*.JepsenHarnessTest.testFakes'
```

Tests will only be executed if the source has changed; override this with
`:cleanTest`:

```
./gradlew :cleanTest :test --tests '*.JepsenHarnessTest.testFakes'
```

## Default Admin User

When you start up fallout for the first time, there will be no users configured.  To setup a default admin user, set the environment variable `FALLOUT_ADMIN_CREDS` to `<USERNAME>:<EMAIL>:<PASSWORD>` the first time you run fallout; for example,

```
FALLOUT_ADMIN_CREDS='Charlie Mouse:charlie@mouseorgan.com:chocolate-biscuit-machine' ./gradlew runServer
```

## MacOS and SSH_AUTH_SOCK forwarding

If you want to run the fallout docker image or the `docker-compose.yml` manually, on macOS you will need to make sure that SSH_AUTH_SOCK is set correctly to handle the special forwarding mechanisms on docker-for-mac (see [docker/entrypoint.sh](docker/entrypoint.sh) for the full details).  To run the docker image:

```
docker run -it -v /run/host-services/ssh-auth.sock:/ssh-agent -e SSH_AUTH_SOCK="/ssh-agent" datastax.com/fallout:latest
```

...(note that it expects a Cassandra server to be running at `$FALLOUT_CASSANDRA_HOST:$FALLOUT_CASSANDRA_PORT`), and to run docker-compose:

```
SSH_AUTH_SOCK=/run/host-services/ssh-auth.sock docker-compose up
```

## Contributing and Issues

Contributions are welcome, please see [CONTRIBUTING](CONTRIBUTING.md) for guidance.

If you encounter any bugs, please file a GitHub issue. 

Active Maintainers:
* [Jake Luciani](https://github.com/tjake)
* [Philip Thompson](https://github.com/ptnapoleon)
* [Guy Bolton King](https://github.com/guyboltonking)
* [James Trevino](https://github.com/JamesATrevino)
* [Sean McCarthy](https://github.com/smccarthy788)

Special thanks to:
* [Joel Knighton](https://github.com/jkni)
