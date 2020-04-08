# ============================================================================
# Python 2/3 compatibility boilerplate
from __future__ import (absolute_import, division, print_function,
                        unicode_literals)

from future import standard_library
from future.builtins import *  # noqa: F401, F403

standard_library.install_aliases()

__metaclass__ = type

# ============================================================================

import logging
import os
import sys
import subprocess

import cassandra.cluster
import psutil
import pytest
import requests
from ruamel.yaml import YAML
import shutil


THIS_DIR = os.path.dirname(__file__)
PROJECT_ROOT = os.path.abspath(os.path.join(THIS_DIR, "..", ".."))
FALLOUTCTL_COMMAND = os.path.join(
    PROJECT_ROOT, "build", "install", "fallout", "bin", "falloutctl")
FALLOUT_TEST_SERVER_URL = "http://localhost:8080"
FALLOUT_HOME = os.path.join(THIS_DIR, "..", "build", "fallout-home")
SIMPLE_TEST_YAML = os.path.join(THIS_DIR, "simple-test.yaml")
SEARCH_TEST_YAML = os.path.join(THIS_DIR, "search-test.yaml")
TEST_USER_EMAIL = "fallout-cli-unittest-user@datastax.com"


def ensure_empty_dir_exists(dirpath):
    if os.path.exists(dirpath):
        shutil.rmtree(dirpath)
    os.makedirs(dirpath)
    return dirpath


@pytest.fixture(scope="session")
def fallout_home():
    return ensure_empty_dir_exists(FALLOUT_HOME)


@pytest.fixture(scope="session")
def tmp_dir():
    return ensure_empty_dir_exists(os.path.join(THIS_DIR, "..", "build", "tmp"))


@pytest.fixture(scope="session")
def artifact_path(fallout_home):
    return os.path.join(fallout_home, "tests")


def assertServerProcessIsAlive():
    with open(os.path.join(FALLOUT_HOME, "run", "fallout.pid"), "r") as pidfile:
        pid = int(pidfile.read())
        assert psutil.pid_exists(pid)
        return pid


@pytest.fixture(scope="session")
def _fallout_test_server(fallout_home, artifact_path):
    server_yaml_path = os.path.join(fallout_home, "fallout.yml")
    with open(server_yaml_path, "w") as server_yaml:
        YAML().dump({
            "keyspace": "test",
            "logTestRunsToConsole": "true",
            "artifactPath": artifact_path,
            "forceJava8ForLocalCommands": "true"
        }, server_yaml)

    def fallout_server_responding_to_ping():
        try:
            return requests.get(FALLOUT_TEST_SERVER_URL).ok
        except requests.ConnectionError as e:
            return False

    if fallout_server_responding_to_ping():
        raise Exception((
            "A fallout server is already running at {}; please " +
            "terminate it as these tests need to run their own " +
            "server").format(FALLOUT_TEST_SERVER_URL))

    if not os.path.exists(FALLOUTCTL_COMMAND):
        raise Exception((
            "Cannot run server; {} does not exist.  Run " +
            "`./gradlew :installDist` in the fallout " +
            "directory").format(FALLOUTCTL_COMMAND))

    os.environ['FALLOUT_HOME'] = fallout_home

    # Make the server log to stderr, which means pytest will capture the
    # log and display it for failing tests.
    assert subprocess.call(
        args=[FALLOUTCTL_COMMAND, "start"],
        cwd=fallout_home,
        stderr=sys.stderr,
        stdout=sys.stderr) == 0

    yield assertServerProcessIsAlive()

    assert subprocess.call(
        args=[FALLOUTCTL_COMMAND, "stop", "--force"],
        cwd=fallout_home,
        stderr=sys.stderr,
        stdout=sys.stderr) == 0


@pytest.fixture
def fallout_test_server(_fallout_test_server):
    return assertServerProcessIsAlive()


@pytest.fixture(scope="session")
def cassandra_session(_fallout_test_server):
    logging.getLogger("cassandra").setLevel(logging.WARNING)
    cluster = cassandra.cluster.Cluster(contact_points=["localhost"], port=9096)
    return cluster.connect()


@pytest.fixture(scope="session")
def oauth_id(cassandra_session):
    assert requests.post(
        FALLOUT_TEST_SERVER_URL + "/account/register",
        data={
            "name": "fallout-cli-unittest-user",
            "email": TEST_USER_EMAIL,
            "password": "123",
            "group": "APOLLO_TESTENG",
        }).status_code == requests.status_codes.codes.OK

    rows = cassandra_session.execute(
        "select oauthId from test.users where email = %s", [TEST_USER_EMAIL])

    return str(rows.one()[0])


@pytest.fixture
def clean_database(cassandra_session):
    cassandra_session.execute("use test")
    cassandra_session.execute("truncate tests")
    cassandra_session.execute("truncate test_runs")
