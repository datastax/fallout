# Building Fallout

## Prerequisites

You will need:

* an installation of Java 11 to build and run the server: set the environment variable `JAVA_HOME` to point at this;
* an installation of Java 8 to run some of the tests (they currently use an older version of Cassandra that cannot run on Java 11): set `JAVA8_HOME` to point at this;
* an installation of Python 3.7, for some of the artifact checkers in [tools](tools), which will also need...
* an installation of [poetry](https://python-poetry.org/); you can run [`./docker/build-files/bootstrap-user-python-support`](docker/build-files/bootstrap-user-python-support) to do this.

## Compiling/Running

To build and run Fallout locally:

```
./gradlew runServer
```

This will start up a Cassandra server, and then a Fallout server on http://localhost:8080.  Type `Ctrl-C` to stop both.

To build and run the docker image:

```
./gradlew runServerInDocker
```

...which will also regenerate the docker image using the gradle `dockerBuild` task.

## Tests

To run all the tests:

```
./gradlew test
```

Run specific tests with the usual gradle `--tests 'TESTGLOB'` option e.g.

```
./gradlew :test --tests '*.JepsenHarnessTest.testFakes'
```

To control log levels in tests, set the gradle property `log.level` (for the root logger) or `log.LOGNAME.level` (for other loggers):

```
./gradlew -Plog.level=DEBUG :test --tests '*.JepsenHarnessTest.testFakes'
```

See [LogbackConfigurator.java](src/test/java/com/datastax/fallout/LogbackConfigurator.java) for the default log levels.

## Style checks

To check any changed code for style violations, use the gradle `lint` target; the `check` target will run `lint` and `test`.

## Updating dependencies

This project uses gradle's dependency locking support to ensure a consistent set of dependencies; if you want to update any wildcarded dependencies, or if you change any dependencies in any project gradle files, please run:

```
./gradlew lockDependencies --write-locks
```

...in the root of the repository, and add the updated `*.lockfile`s to the commit.
