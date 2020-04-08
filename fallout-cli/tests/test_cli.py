# ============================================================================
# Python 2/3 compatibility boilerplate
from __future__ import (absolute_import, division, print_function,
                        unicode_literals)

from future import standard_library
from future.builtins import *  # noqa: F401, F403

standard_library.install_aliases()

__metaclass__ = type

# ============================================================================

import functools
import os
import subprocess
import uuid

import pytest
from future.utils import PY3
from .conftest import FALLOUT_TEST_SERVER_URL, SIMPLE_TEST_YAML, SEARCH_TEST_YAML, THIS_DIR, TEST_USER_EMAIL

INVALID_TEST_YAML = os.path.join(THIS_DIR, "invalid-test.yaml")
TEMPLATED_TEST_YAML = os.path.join(
    THIS_DIR, "templated-test-fails-by-default.yaml")


class Command:
    def __init__(self, oauth_id, *args):
        env = os.environ.copy()
        env["FALLOUT_OAUTH_TOKEN"] = oauth_id

        process = subprocess.Popen(
            ["fallout", "--fallout-url=" + FALLOUT_TEST_SERVER_URL] +
            list(args), env=env,
            stdout=subprocess.PIPE, stderr=subprocess.PIPE)

        self.__stdout, self.__stderr = process.communicate()
        self.__status = process.wait()

        self.__expected_status = 0

        if PY3:
            self.__stdout = self.__stdout.decode()
            self.__stderr = self.__stderr.decode()

        print("=== cmd: " + repr(args))
        if len(self.__stdout):
            print("--- stdout:")
            print(self.__stdout, end='')
        if len(self.__stderr):
            print("--- stderr:")
            print(self.__stderr, end='')

    def __output(self):
        return self.__stdout

    def __error_output(self):
        return self.__stderr

    def __output_lines(self):
        return [line for line in self.__stdout.split('\n') if len(line.strip())]

    def __output_lines_fields(self, max_fields):
        return [line.split()[:max_fields] for line in self.__output_lines()]

    def output_fields(self, pos):
        return [fields[pos] for fields in self.__output_lines_fields(pos + 1)]

    def fails(self):
        self.exits_with(1)
        # detect internal errors that shouldnt happen
        # <python3 error when error handling has an error>
        # During handling of the above exception, another exception occurred:
        self.does_not_output_error('another exception occurred')
        self.does_not_output_error('AttributeError')
        return self

    def __has_expected_status(self):
        assert self.__status == self.__expected_status
        return self

    def exits_with(self, status):
        self.__expected_status = status
        self.__has_expected_status()
        return self

    def output_contains(self, substring):
        self.__has_expected_status()
        assert substring in self.__output()
        return self

    def outputs_line(self, line):
        self.__has_expected_status()
        assert line in self.__output_lines()
        return self

    def outputs_exactly(self, output):
        self.__has_expected_status()
        assert self.__output() == output
        return self

    def outputs_nothing(self):
        self.outputs_exactly("")
        return self

    def outputs_something(self):
        self.__has_expected_status()
        assert self.__output() != ""
        return self

    def outputs_fields(self, fields):
        self.__has_expected_status()
        assert self.__output_lines_fields(len(fields[0])) == fields
        return self

    def outputs_field(self, pos, field):
        self.__has_expected_status()
        assert self.output_fields(pos) == field
        return self

    def does_not_output_error(self, error):
        assert error not in self.__error_output()


@pytest.fixture
def cmd(fallout_test_server, oauth_id, clean_database):
    return functools.partial(Command, oauth_id)


@pytest.fixture
def invalid_auth_cmd(fallout_test_server, clean_database):
    invalid_oauth_id = "f000f000-f000-f000-f000-f000f000f000"
    return functools.partial(Command, invalid_oauth_id)


def generated_test_name(yaml):
    return os.path.splitext(os.path.basename(yaml))[0]


def create_and_validate_test_yaml(cmd, yaml):
    cmd("list-tests").outputs_nothing()

    testname = generated_test_name(yaml)

    cmd("test-info", testname).fails()
    cmd("create-test", yaml).outputs_nothing()
    cmd("test-info", testname).outputs_something()

    cmd("list-testruns", testname) \
        .outputs_fields([[testname, "*"]])
    cmd("list-testruns") \
        .outputs_fields([[testname, "*"]])

    return testname


def delete_test_and_validate_removal(cmd, testrun_ids, testname):
    cmd("delete-test", testname).outputs_nothing()
    for testrun_id in testrun_ids:
        cmd("testrun-info", "--testrun", testrun_id, testname).fails()
    cmd("test-info", testname).fails()

    cmd("delete-test", testname).outputs_nothing()


def create_and_validate_testrun(cmd, testname, create_cmd, status,
                                expected_testruns):
    testrun_id = cmd(*create_cmd) \
        .outputs_field(0, [testname]) \
        .output_fields(1)[0]

    wait_for_testrun_and_check_status(
        cmd, testname, testrun_id, status)

    expected_testruns.append([testname, testrun_id, status])

    cmd("list-testruns", testname) \
        .outputs_fields(expected_testruns)


def check_fallout_shared_is_in_artifacts(cmd, testname, testrun_id):
    cmd("testrun-info", "--testrun", testrun_id, "--artifacts", testname) \
        .outputs_line("  fallout-shared.log")


def wait_for_testrun_and_check_status(cmd, testname, testrun_id, status):
    cmd("testrun-info", "--testrun", testrun_id, testname).outputs_something()
    cmd("testrun-info", "--testrun", testrun_id,
        "--wait", "--wait-interval=5", testname) \
        .outputs_fields([[testname, testrun_id, status]])
    check_fallout_shared_is_in_artifacts(cmd, testname, testrun_id)


def clear_artifacts_entry_for_testrun(testname, testrun_id, cassandra_session):
    cassandra_session.execute(
        "update test.test_runs set artifacts={} where owner= %s and testname= %s and testrunid= %s",
        [TEST_USER_EMAIL, testname, uuid.UUID(testrun_id)])


def check_compressed_artifacts_are_shown_as_uncompressed(cmd, testname, testrun_id, artifact_path, cassandra_session):
    artifact_path = os.path.join(
        artifact_path, TEST_USER_EMAIL, testname, testrun_id)

    fallout_shared_log_path = os.path.join(artifact_path, "fallout-shared.log")

    os.rename(fallout_shared_log_path, fallout_shared_log_path + ".gz")
    clear_artifacts_entry_for_testrun(testname, testrun_id, cassandra_session)

    check_fallout_shared_is_in_artifacts(cmd, testname, testrun_id)

    tarball_path = os.path.join(artifact_path, "tarball.tar.gz")
    clear_artifacts_entry_for_testrun(testname, testrun_id, cassandra_session)

    with open(tarball_path, 'w') as tarball:
        tarball.write("")

    cmd("testrun-info", "--testrun", testrun_id, "--artifacts", testname) \
        .outputs_line("  tarball.tar.gz")


def test_normal_test_creation_and_run(cmd, artifact_path, cassandra_session):
    testname = create_and_validate_test_yaml(cmd, SIMPLE_TEST_YAML)

    testrun_id = \
        cmd("create-testrun", testname) \
        .outputs_field(0, [testname]) \
        .output_fields(1)[0]

    wait_for_testrun_and_check_status(cmd, testname, testrun_id, "PASSED")

    check_compressed_artifacts_are_shown_as_uncompressed(
        cmd, testname, testrun_id, artifact_path, cassandra_session)

    cmd("list-testruns", testname) \
        .outputs_fields([[testname, testrun_id, "PASSED"]])

    delete_test_and_validate_removal(cmd, [testrun_id], testname)


def test_templated_test_creation_and_run(cmd, tmp_dir):
    testname = create_and_validate_test_yaml(cmd, TEMPLATED_TEST_YAML)

    expected_testruns = []

    create_and_validate_testrun(
        cmd, testname,
        ["create-testrun", testname],
        "FAILED", expected_testruns)

    create_and_validate_testrun(
        cmd, testname,
        ["create-testrun", testname, "required_regex=h.+ll"],
        "PASSED", expected_testruns)

    json_params_filename = os.path.join(tmp_dir, "params.json")
    with open(json_params_filename, "w") as json_params:
        json_params.write('{ "required_regex": "h.+ll" }')

    create_and_validate_testrun(
        cmd, testname,
        ["create-testrun", "--params=" + json_params_filename, testname],
        "PASSED", expected_testruns)

    yaml_params_filename = os.path.join(tmp_dir, "params.yaml")
    with open(yaml_params_filename, "w") as yaml_params:
        yaml_params.write('required_regex: h.+ll')

    create_and_validate_testrun(
        cmd, testname,
        ["create-testrun", "--params=" + yaml_params_filename, testname],
        "PASSED", expected_testruns)

    delete_test_and_validate_removal(
        cmd, [t[1] for t in expected_testruns], testname)


def test_auth_failure(invalid_auth_cmd):
    invalid_auth_cmd("test-info", 'non-existing-test-name').fails()


def test_validate_valid_test_yaml(cmd):
    with open(SIMPLE_TEST_YAML, 'r', encoding='utf-8') as f:
        simple_test_yaml = f.read()
    cmd("validate-test", "--show", SIMPLE_TEST_YAML) \
        .outputs_exactly(simple_test_yaml)


def test_validate_invalid_test_yaml(cmd):
    cmd("validate-test", INVALID_TEST_YAML) \
        .exits_with(2) \
        .output_contains("The yaml is invalid:\n")


def test_validate_templated_test_yaml(cmd, tmp_dir):
    with open(TEMPLATED_TEST_YAML, 'r', encoding='utf-8') as f:
        templated_test_yaml = f.read()

    templated_test_yaml = \
        templated_test_yaml[templated_test_yaml.find('---') + 4:] \
        .replace('{{required_regex}}', 'hello')

    cmd("validate-test", "--show", TEMPLATED_TEST_YAML, "required_regex=hello") \
        .outputs_exactly(templated_test_yaml)

    json_params_filename = os.path.join(tmp_dir, "validate-params.json")
    with open(json_params_filename, "w") as json_params:
        json_params.write('{ "required_regex": "hello" }')

    cmd("validate-test", "--show", "--params=" + json_params_filename,
        TEMPLATED_TEST_YAML) \
        .outputs_exactly(templated_test_yaml)

    yaml_params_filename = os.path.join(tmp_dir, "validate-params.yaml")
    with open(yaml_params_filename, "w") as yaml_params:
        yaml_params.write('required_regex: hello')

    cmd("validate-test", "--show", "--params=" + yaml_params_filename,
        TEMPLATED_TEST_YAML) \
        .outputs_exactly(templated_test_yaml)


def test_search_yaml(cmd):
    create_and_validate_test_yaml(cmd, SEARCH_TEST_YAML)
    regex_pass = "managed_files"
    regex_fail = "bdset"

    cmd("search-yaml", regex_pass).outputs_something()
    cmd("search-yaml", regex_fail).outputs_nothing()


def test_aborting_a_test_run(cmd):
    testname = create_and_validate_test_yaml(cmd, SIMPLE_TEST_YAML)

    testrun_id = \
        cmd("create-testrun", testname) \
        .outputs_field(0, [testname]) \
        .output_fields(1)[0]

    cmd("abort-testrun", "--testrun=" + testrun_id, testname) \
        .exits_with(0) \
        .outputs_nothing()
