# Fallout

Fallout is a tool for running local or large scale remote based distributed correctness, verification and performance tests. Fallout is run in production at DataStax facilitating mission critical testing of Apache Cassandra&#x2122;, Apache Pulsar&#x2122;, and many other products.

This repository contains the core framework and components of fallout: you can use it to run performance tests on Google Kubernetes Engine.  At DataStax, we have an internal version that builds on this code and integrates with our internal infrastructure.

There is a [recorded workshop](https://www.youtube.com/watch?v=45iTmTBjU0M) which covers the project's intent, architecture, and simple examples.

## Getting Started

The easiest way to get started is to use the [published docker image](https://hub.docker.com/r/datastax/fallout) (which bundles all the support tools with Fallout), and the `fallout exec` command which runs a single test definition:

```
$ docker run datastax/fallout:latest fallout exec --help
usage: java -jar fallout-0.1.0-SNAPSHOT-all.jar
       exec [--params TEMPLATE-PARAMS-YAML-FILE]
       [--use-unique-output-dir] [--config FILE] [-h] test-yaml-file
       creds-yaml-file output-dir [template-params [template-params ...]]

Run a single testrun in a standalone fallout process and exit

positional arguments:
  test-yaml-file         Test definition YAML file
  creds-yaml-file        Credentials YAML file
  output-dir             Where to write  testrun  artifacts;  it's an error
                         if the directory isn't empty
  template-params        Template params  for  test-yaml-file  in  the form
                         param=value

named arguments:
  --params TEMPLATE-PARAMS-YAML-FILE
                         Template parameters YAML file
  --use-unique-output-dir
                         Write     testrun     artifacts     to     output-
                         dir/TEST_NAME/TESTRUN_ID instead of  directly into
                         output-dir (default: false)
  --config FILE          Application configuration file
  -h, --help             show this help message and exit
```

For an example, see the Pulsar tests at https://github.com/datastax/pulsar-fallout.

## Running a Server

Fallout can be run as a multi-user service (that's how we deploy it at DataStax).  Before describing how to do this, one important caveat: **please do not run Fallout as a service on the public internet**.  In its current form, it is not secure.

You can use `docker compose` to run [`docker-compose.yml`](docker/docker-compose.yml).  When starting for the first time you'll want to create a default admin user; you can do this by setting the `FALLOUT_ADMIN_CREDS` environment variable to `<USERNAME>:<EMAIL>:<PASSWORD>` on the very first start-up:

```
FALLOUT_ADMIN_CREDS=admin:admin@fallout.example.com:admin \
  docker compose --file fallout-oss/docker/docker-compose.yml up
```

...and connect to http://localhost:8080.

The `docker-compose.yml` defines two volumes to persist the Fallout artifacts and cassandra data, so the next time you start, the data will still be there; also you won't need the `FALLOUT_ADMIN_CREDS`:

```
docker compose --file fallout-oss/docker/docker-compose.yml up
```

### Single-user Mode

If you set the environment variable `FALLOUT_AUTH_MODE` to `SINGLE_USER`, then fallout will automatically log you in as the user defined in `FALLOUT_ADMIN_CREDS` above.  In this case, you have to specify both environment variables every time you start:

```
FALLOUT_AUTH_MODE=SINGLE_USER \
FALLOUT_ADMIN_CREDS=admin:admin@fallout.example.com:admin \
  docker compose --file fallout-oss/docker/docker-compose.yml up
```


## The Future

We intend to move more functionality into this, the core repository, as time goes on.  There will be better documentation, and more examples.

## Building, Contributing and Issues

See [BUILDING.md](BUILDING.md) for how to build and run the project.

Contributions are welcome, please see [CONTRIBUTING](CONTRIBUTING.md) for guidance.

If you encounter any bugs, please file a GitHub issue.
