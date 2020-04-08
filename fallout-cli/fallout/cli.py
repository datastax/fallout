# Copyright 2020 DataStax, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""
Fallout command-line client.

USAGE:
    PROG help
    PROG create-test [options] [--delete] [--name=TEST] TEST_YAML
    PROG validate-test [options] [--show] [--params=FILE] TEST_YAML
        [PARAMS ...]
    PROG list-tests [options] [--user=USER]
    PROG test-info [options] [--user=USER] TEST
    PROG delete-test [options] TESTS ...
    PROG create-testrun [options]
        [--params=FILE|--re-run=TESTRUN_ID|--clone=TESTRUN_ID] TEST
        [PARAMS ...]
    PROG list-testruns [options] [--user=USER] [--latest]
        [--show-params [--no-headers]] [TESTS ...]
    PROG testrun-info [options] [--user=USER] [--testrun=TESTRUN_ID]
        [--wait] [--state-only|--show-params [--no-headers]] [--artifacts] TEST
    PROG testrun-artifact [options] [--user=USER] [--testrun=TESTRUN_ID]
        [--wait] [--stdout] TEST [PATH]
    PROG delete-testrun [options] [--user=USER] [--testrun=TESTRUN_ID] TEST
    PROG delete-testruns [options] [--user=USER] TEST
    PROG run-script [options] [--user=USER] [--testrun=TESTRUN_ID] SCRIPT TEST
        [PARAMS ...]
    PROG abort-testrun [options] [--user=USER] [--testrun=TESTRUN_ID] TEST
    PROG search-yaml [options] [--user=USER] [--all-users] REGEX [--json]
    PROG version

OPTIONS:
    --fallout-url=FALLOUT-URL  The URL of the fallout host.  If unspecified,
                               then the value of the environment variable
                               FALLOUT_URL is used; if the latter is unset,
                               then this defaults to localhost:8080

    --color                    Force usage of color output, even when output
                               is not a terminal

    --no-color                 Disable usage of color output

    --field-sep=SEP            Separate output fields with SEP instead of using
                               fixed width fields.

    --wait-interval=<seconds>  [default: 30] Commands that have a --wait
                               option poll the server for status; this option
                               sets the interval between polls.  Values less
                               than 5 will be replaced with 5.

    --debug                    Show debugging output

COMMANDS:

Commands that contact the fallout server expect the environment variable
FALLOUT_OAUTH_TOKEN to be set (you can find your OAuth ID on your /account page.
This is also used to specify the current user.

help                        Show this help

create-test                 Create or update a test from the file TEST_YAML;
                            the test will be given the same name as the file
                            without the [.fallout].yaml suffix. Specify a
                            different name with the --name option. The --delete
                            option will delete any existing test first.

validate-test               Validate a test from the file TEST_YAML. This will
                            not create a new test, just ensure all properties
                            are valid.  PARAMS arguments and --params option
                            have the same effect as those in create-testrun.
                            The --show option shows the test definition with
                            parameters replaced.

list-tests                  List all tests for either the current user or the
                            specified USER.

test-info                   Dump the YAML and additional info for the specified
                            TEST belonging to the current user or the specified
                            USER.

delete-test                 Delete the specified TESTS belonging to the current
                            user.

create-testrun              Run the specified TEST.  If the test definition is a
                            templated test, then you can specify template
                            PARAMS on the command line as param=value.
                            Alternatively, use the --params option to provide
                            them as JSON or YAML in a FILE with a .json or
                            .yaml suffix, or use --re-run to use the params from
                            the specified TESTRUN_ID.  --clone re-runs the
                            specified TESTRUN_ID with the same params and
                            definition as it was first run with.
                            Prints the new testrun_id.

abort-testrun               Aborts the specified TEST's TESTRUN_ID for either
                            the current user or the specified USER. If no
                            TESTRUN_ID is specified the last testrun is aborted.

list-testruns               List all testruns for the current user or the
                            specified USER.  Limit the output by specifying
                            which TESTS to list.  --show-params will show
                            template params if they exist; --no-headers will
                            disable the headers that --show-params displays.

testrun-info                Show the status of the specified TEST's TESTRUN_ID,
                            or of the latest testrun, for either the current
                            user or the specified USER.  The --wait option
                            will prevent this command from doing anything
                            until the testrun is complete.  The --artifacts
                            option will list all the artifacts.  --state-only
                            will restrict the output to the current state,
                            and --show-params will show template params if they
                            exist; --no-headers will disable the headers
                            that --show-params displays.

testrun-artifact            Fetch the artifact at PATH from TEST's
                            TESTRUN_ID, or the latest testrun if omitted, for
                            either the current user or the specified USER.  The
                            artifact will be written to a file with the same
                            name without the directory path, unless the --stdout
                            option is given, which will pipe the file to stdout.
                             If PATH is omitted, then all the artifacts will be
                            downloaded to a directory named TEST/TESTRUN_ID.
                            The --wait option will prevent this command from
                            doing anything until the testrun is complete.

delete-testrun              Delete the specified TEST's TESTRUN_ID, or the
                            latest testrun, for either the current user or the
                            specified USER.

delete-testruns             Delete all testruns for TEST (except those that
                            cannot be deleted because they're running or linked
                            to performance reports), for either the current
                            user or the specified USER.

run-script                  Run a SCRIPT on the specified TEST's TESTRUN_ID, or
                            of the latest test run, for either the current user
                            or the specified USER. Additional PARAMS may be
                            passed as key=value pairs and will be send as-is to
                            fallout

search-yaml                 Searches for REGEX in all tests belonging to the
                            current user, the specified USER, or all users
                            if --all-users is used.

version                     Print the client version and exit
"""

# ============================================================================
# Python 2/3 compatibility boilerplate
from __future__ import (absolute_import, division, print_function,
                        unicode_literals)

from future import standard_library
from future.builtins import *  # noqa: F401, F403

standard_library.install_aliases()

__metaclass__ = type

# ============================================================================

from collections import OrderedDict
import io
import os
import re
import sys
import datetime
import time
import zipfile

import blessed
from docopt import docopt
from future.utils import PY3, native_str
from ruamel.yaml import YAML
import requests

from fallout.api import FalloutAPI
from fallout.version import FULL_VERSION_STRING


def to_unicode(obj):
    """Ensure that strings are unicode"""
    if PY3:
        return obj
    if isinstance(obj, dict):
        return dict((k, to_unicode(v)) for k, v in obj.items())
    if isinstance(obj, list):
        return list(to_unicode(x) for x in obj)
    if isinstance(obj, native_str):
        return obj.decode()
    return obj


def write_bytes_to_stdout(data):
    if PY3:
        sys.stdout.buffer.write(data)
    else:
        sys.stdout.write(data)


def timestamp_to_iso8601(timestamp):
    if timestamp is None:
        return '*'
    return datetime.datetime.utcfromtimestamp(
        timestamp / 1000).isoformat()


def timestamps_to_duration(start, finish):
    if finish is None or start is None:
        return '*'

    return str(datetime.timedelta(milliseconds=finish - start))


terminal = blessed.Terminal()
sep = None


def fixed_width(string, width):
    if sep is None:
        return string.ljust(width)
    return string


def field_sep():
    if sep is None:
        return " "
    return sep


def use_ascii_underline():
    return sep is None and not terminal.does_styling


def empty_field():
    if sep is None:
        return "*"
    return ""


MAX_TESTRUN_STATE_LEN = 21


def testrun_state_pad(testrun_state):
    return fixed_width(testrun_state, MAX_TESTRUN_STATE_LEN)


def testrun_state_to_string(testrun_state):
    color = terminal.yellow

    if testrun_state in ("FAILED", "ABORTED"):
        color = terminal.red
    elif testrun_state == "UNKNOWN":
        color = terminal.black
    elif testrun_state == "PASSED":
        color = terminal.green

    return color(testrun_state_pad(testrun_state))


def testrun_failed_during_to_string(testrun_state):
    if testrun_state is not None:
        return terminal.red(testrun_state_pad(testrun_state))
    return terminal.white(testrun_state_pad(empty_field()))


def testrun_finished(testrun):
    return testrun['state'] in ("FAILED", "PASSED", "ABORTED")


yaml = YAML(typ='safe')
yaml.default_flow_style = True


def from_yaml(data):
    if data is None:
        return {}
    stream = io.StringIO(data)
    return yaml.load(stream)


def set_templateParams_from_yaml(testrun):
    testrun['templateParams'] = from_yaml(testrun['templateParams'])


def to_yaml(obj):
    stream = io.StringIO()
    yaml.dump(obj, stream)
    dumped = stream.getvalue()
    # If we dump a scalar, we end up with the end-of-doc sequence '...' as well
    if dumped.endswith("\n...\n"):
        dumped = dumped[:-5]
    return dumped.strip()


MAX_ID_LEN = 36
MAX_DATE_LEN = 26


def testrun_heading(max_testname_width=None, param_widths=OrderedDict()):
    name = 'name'
    if max_testname_width is not None:
        name = fixed_width(name, max_testname_width)

    field_widths = \
        [len(name),
         MAX_ID_LEN] + \
        list(param_widths.values()) + \
        [MAX_TESTRUN_STATE_LEN,
         MAX_TESTRUN_STATE_LEN,
         MAX_DATE_LEN,
         MAX_DATE_LEN,
         MAX_DATE_LEN]

    text = field_sep().join(
        terminal.underline(fixed_width(field, width))
        for (field, width) in zip(
            [name,
             'testrun'] +
            list(param_widths.keys()) +
            ['state',
             'failed during',
             'start',
             'end',
             'duration'],
            field_widths))

    if use_ascii_underline():
        text += "\n" + field_sep().join(
            "-" * width for width in field_widths)

    return text


def testrun_to_row(name, testrun=None, max_testname_width=None, param_widths=OrderedDict()):
    if max_testname_width is not None:
        name = fixed_width(name, max_testname_width)

    if testrun is None:
        return field_sep().join(
            [terminal.bold(name)] +
            [terminal.white(field) for field in
                [fixed_width(empty_field(), MAX_ID_LEN)] +
                [fixed_width(empty_field(), width)
                 for width in param_widths.values()] +
                [testrun_state_pad(empty_field()),
                 testrun_state_pad(empty_field()),
                 fixed_width(empty_field(), MAX_DATE_LEN),
                 fixed_width(empty_field(), MAX_DATE_LEN),
                 fixed_width(empty_field(), MAX_DATE_LEN)]])

    testrun_params = testrun['templateParams']

    def param_string(param, width):
        if param in testrun_params:
            return terminal.blue(fixed_width(to_yaml(testrun_params[param]), width))
        return terminal.white(fixed_width(empty_field(), width))

    return field_sep().join(
        [terminal.bold(name),
         testrun['testRunId']] +
        [param_string(k, v) for (k, v) in param_widths.items()] +
        [testrun_state_to_string(testrun['state']),
         testrun_failed_during_to_string(testrun['failedDuring']),
         fixed_width(timestamp_to_iso8601(testrun['startedAt']), MAX_DATE_LEN),
         fixed_width(timestamp_to_iso8601(testrun['finishedAt']), MAX_DATE_LEN),
         fixed_width(timestamps_to_duration(
             testrun['startedAt'], testrun['finishedAt']), MAX_DATE_LEN)])


def create_test(fallout_api, delete, test_name, test_yaml):
    if test_name is None:
        test_name = re.sub(
            r"(\.fallout)?\.ya?ml$", "", os.path.basename(test_yaml))
    if delete:
        delete_test(fallout_api, [test_name])
    if not fallout_api.create_or_update_test(test_name, test_yaml):
        print(test_name + " already exists")
        sys.exit(1)


def validate_test(fallout_api, test_yaml, params, params_filename, show):
    if params_filename is not None:
        with open(params_filename, 'r', encoding='utf-8') as params_file:
            if params_filename.endswith(".yaml"):
                result = fallout_api.validate_test(test_yaml, yaml=params_file.read())
            else:
                result = fallout_api.validate_test(test_yaml, json=params_file.read())
    else:
        result = fallout_api.validate_test(test_yaml, params=params)

    (valid, content) = result

    if not valid:
        print("The yaml is invalid:\n{}".format(content))
        sys.exit(2)

    if show:
        print(content, end='')
    else:
        print("The yaml is valid")


def list_tests(fallout_api, user):
    tests = fallout_api.list_tests(user)
    if len(tests):
        max_test_width = max(len(test['name']) for test in tests)
        for test in tests:
            print(" ".join([
                terminal.bold(test['name'].ljust(max_test_width)),
                timestamp_to_iso8601(test['createdAt']).ljust(MAX_DATE_LEN)
            ]))


def test_info(fallout_api, user, test):
    info = fallout_api.test_info(test, user)
    print("### " + info['name'])
    print("### ID " + info['testId'])
    print("### Created at " + timestamp_to_iso8601(info['createdAt']))
    print("### Created by " + info['owner'])
    print(info['definition'])


def delete_test(fallout_api, tests):
    for test in tests:
        fallout_api.delete_test(test)


def create_testrun(fallout_api, test, params, params_filename, testrun_id, clone):
    if testrun_id is not None:
        user = fallout_api.test_info(test)['owner']
        info = fallout_api.re_run_testrun(user, test, testrun_id, clone)
    elif params_filename is not None:
        with open(params_filename, 'r', encoding='utf-8') as params_file:
            if params_filename.endswith(".yaml"):
                info = fallout_api.create_testrun(test, yaml=params_file.read())
            else:
                info = fallout_api.create_testrun(test, json=params_file.read())
    else:
        info = fallout_api.create_testrun(test, params=params)

    print("{}\t{}".format(test, info['testRunId']))


def param_widths(testruns):
    all_params = OrderedDict()

    for testrun in testruns:
        for (k, v) in testrun['templateParams'].items():
            width = max(len(str(k)), len(to_yaml(v)))
            if k not in all_params or width > all_params[k]:
                all_params[k] = width

    return all_params


def list_testruns(fallout_api, tests, user, latest, show_params, no_headers):
    testinfos = [fallout_api.test_info(test, user) for test in tests]

    if len(testinfos) == 0:
        testinfos = fallout_api.list_tests(user)

    max_testname_width = max([len(testinfo['name']) for testinfo in testinfos])

    testrow = 0

    def format_row(row):
        if not latest and testrow % 2:
            return terminal.on_bright_white(row)
        return row

    for testinfo in testinfos:

        testruns = fallout_api.list_testruns(
            testinfo['owner'], testinfo['name'])

        if latest:
            testruns = testruns[-1:]

        for testrun in testruns:
            set_templateParams_from_yaml(testrun)

        params = OrderedDict()
        if show_params:
            params = param_widths(testruns)

            if not no_headers:
                print(format_row(testrun_heading(
                    param_widths=params,
                    max_testname_width=max_testname_width)))

        if len(testruns) == 0:
            print(format_row(testrun_to_row(
                testinfo['name'], max_testname_width=max_testname_width)))

        for testrun in testruns:
            print(format_row(testrun_to_row(
                testinfo['name'], testrun,
                param_widths=params,
                max_testname_width=max_testname_width)))

        testrow += 1


def latest_testrun_id(fallout_api, user, test):
    testruns = fallout_api.list_testruns(user, test)

    if len(testruns) == 0:
        return None

    return testruns[-1]['testRunId']


def testrun_info_ignoring_outages(fallout_api, user, test, testrun_id, wait_interval):
    timeout = 3600
    start = time.time()
    while time.time() - start < timeout:
        try:
            return fallout_api.testrun_info(user, test, testrun_id)
        except requests.HTTPError as ex:
            if ex.response.status_code == 502:
                print("Ignoring probable redeployment outage: {}".format(ex),
                      file=sys.stderr)
            else:
                raise
        time.sleep(wait_interval)
    raise Exception("Timed out waiting for testrun_info")


def maybe_wait_for_testrun_info(fallout_api, user, test, testrun_id,
                                wait_interval):

    if not wait_interval:
        result = fallout_api.testrun_info(user, test, testrun_id)
    else:
        result = testrun_info_ignoring_outages(
            fallout_api, user, test, testrun_id, wait_interval)

        while not testrun_finished(result):
            time.sleep(wait_interval)
            result = testrun_info_ignoring_outages(
                fallout_api, user, test, testrun_id, wait_interval)

    set_templateParams_from_yaml(result)

    return result


def testrun_info(fallout_api, test, testrun_id, user, wait_interval, state_only,
                 artifacts, show_params, no_headers):
    if user is None:
        user = fallout_api.test_info(test)['owner']

    if testrun_id is None:
        testrun_id = latest_testrun_id(fallout_api, user, test)

    if testrun_id is None:
        print(testrun_to_row(test))
        return

    result = maybe_wait_for_testrun_info(
        fallout_api, user, test, testrun_id, wait_interval)

    if state_only:
        print(testrun_state_to_string(result['state']))
    else:
        if show_params:
            params = param_widths([result])

            if not no_headers:
                print(testrun_heading(
                    param_widths=params, max_testname_width=len(test)))

            print(testrun_to_row(
                test, result, param_widths=params))
        else:
            print(testrun_to_row(test, result))
        if artifacts:
            for artifact in result['artifacts']:
                print("  " + artifact)


def abort_testrun(fallout_api, user, test, testrun_id):
    if user is None:
        user = fallout_api.test_info(test)['owner']

    if testrun_id is None:
        testrun_id = latest_testrun_id(fallout_api, user, test)

    if testrun_id is None:
        print("No testruns for {}. Nothing to abort.".format(test))
        sys.exit(1)

    fallout_api.abort_testrun(user, test, testrun_id)


def testrun_artifact(fallout_api, test, testrun_id, path, user, wait_interval, stdout):
    if user is None:
        user = fallout_api.test_info(test)['owner']

    if testrun_id is None:
        testrun_id = latest_testrun_id(fallout_api, user, test)

    if testrun_id is None:
        print("No testruns for {}; cannot download artifact".format(test))
        sys.exit(1)

    maybe_wait_for_testrun_info(
        fallout_api, user, test, testrun_id, wait_interval)

    if path is None:
        if stdout:
            print("Full artifact download requested; ignoring --stdout")

        result = fallout_api.testrun_artifacts(user, test, testrun_id)
        archive = zipfile.ZipFile(io.BytesIO(result))
        archive.extractall()
        return

    result = fallout_api.testrun_artifact(
        user, test, testrun_id, path.split('/'))

    if stdout:
        write_bytes_to_stdout(result)
        return

    target_dir = os.path.join(user, test, testrun_id, os.path.dirname(path))
    if not os.path.exists(target_dir):
        os.makedirs(target_dir)
    with open(os.path.join(target_dir, os.path.basename(path)),
              "wb") as artifact_file:
        artifact_file.write(result)


def delete_testrun(fallout_api, user, test, testrun_id):
    if user is None:
        user = fallout_api.test_info(test)['owner']

    if testrun_id is None:
        testrun_id = latest_testrun_id(fallout_api, user, test)

    if testrun_id is None:
        print("No testruns for {}. Nothing to delete.".format(test))
        sys.exit(1)

    fallout_api.delete_testrun(user, test, testrun_id)


def delete_testruns(fallout_api, user, test):
    if user is None:
        user = fallout_api.test_info(test)['owner']

    testruns = fallout_api.list_testruns(user, test)

    for testrun in testruns:
        testrun_id = testrun['testRunId']
        try:
            fallout_api.delete_testrun(user, test, testrun_id)
            print("Deleted {}".format(testrun_id))
        except requests.HTTPError as ex:
            print("Did not delete {}: {}:\n{}".format(
                testrun_id, ex, ex.response.text), file=sys.stderr)


def run_script(fallout_api, user, test, testrun_id, script, params):
    if user is None:
        user = fallout_api.test_info(test)['owner']

    if testrun_id is None:
        testrun_id = latest_testrun_id(fallout_api, user, test)

    if testrun_id is None:
        print("Cannot find any test run for test %s and user %s" % (test, user))
        sys.exit(1)

    info = fallout_api.testrun_info(user, test, testrun_id)
    if not testrun_finished(info):
        print("Test run %s is not finished yet, a script cannot be run" % testrun_id)
        sys.exit(1)

    result = fallout_api.run_script(user, test, testrun_id, script, params)
    print(result)


def search_yaml(fallout_api, user, all_users, regex):
    matches = fallout_api.search_yaml(user, all_users, regex)

    if all_users:
        for test in matches:
            max_testname_width = max(len(test['name']) for test in matches)
            print(" ".join([
                terminal.bold(test['name'].ljust(max_testname_width)),
                test['owner']
            ]))
    else:
        for test in matches:
            print(terminal.bold(test['name']))


class ParameterError(RuntimeError):
    pass


def parse_key_value_parameters(params_as_string_list):
    params = {}
    for param in params_as_string_list:
        if '=' not in param:
            raise ParameterError("Template parameter {} is not of the form PARAM=VALUE".format(param))
        k, v = param.split('=', 2)
        params[k] = v
    return params


def main(argv=sys.argv):
    if len(argv) >= 2 and argv[1] == "help":
        argv[1] = "--help"

    # Don't call to_unicode on sys.argv: docopt doesn't like being given
    # unicode sys.argv on py2.
    docopt_dict = to_unicode(
        docopt(__doc__.replace("PROG", os.path.basename(argv[0])), argv[1:]))

    fallout_oauth_token = os.getenv('FALLOUT_OAUTH_TOKEN')
    if fallout_oauth_token is None:
        print("FALLOUT_OAUTH_TOKEN environment variable is not set",
              file=sys.stderr)
        sys.exit(1)

    global terminal
    if docopt_dict['--color']:
        terminal = blessed.Terminal(force_styling=True)
    elif docopt_dict['--no-color']:
        terminal = blessed.Terminal(force_styling=None)

    global sep
    sep = docopt_dict['--field-sep']

    def wait_interval():
        if docopt_dict['--wait']:
            return max(int(docopt_dict['--wait-interval']), 5)
        return 0

    if docopt_dict['version']:
        print(FULL_VERSION_STRING)
        return

    try:
        fallout_url = \
            docopt_dict['--fallout-url'] or \
            os.getenv('FALLOUT_URL') or \
            'https://localhost:8080'

        fallout_api = FalloutAPI(fallout_url, os.getenv('FALLOUT_OAUTH_TOKEN'),
                                 debug=docopt_dict['--debug'])
        fallout_api.validate_server_version()

        if docopt_dict['create-test']:
            create_test(fallout_api, docopt_dict['--delete'],
                        docopt_dict['--name'],
                        docopt_dict['TEST_YAML'])

        elif docopt_dict['validate-test']:
            validate_test(fallout_api, docopt_dict['TEST_YAML'],
                          parse_key_value_parameters(docopt_dict['PARAMS']),
                          docopt_dict['--params'],
                          docopt_dict['--show'])

        elif docopt_dict['list-tests']:
            list_tests(fallout_api, docopt_dict['--user'])

        elif docopt_dict['test-info']:
            test_info(fallout_api, docopt_dict['--user'], docopt_dict['TEST'])

        elif docopt_dict['delete-test']:
            delete_test(fallout_api, docopt_dict['TESTS'])

        elif docopt_dict['create-testrun']:
            create_testrun(fallout_api, docopt_dict['TEST'],
                           parse_key_value_parameters(docopt_dict['PARAMS']),
                           docopt_dict['--params'],
                           docopt_dict['--re-run'] or docopt_dict['--clone'],
                           bool(docopt_dict['--clone']))

        elif docopt_dict['list-testruns']:
            list_testruns(fallout_api, docopt_dict['TESTS'],
                          docopt_dict['--user'],
                          docopt_dict['--latest'],
                          docopt_dict['--show-params'],
                          docopt_dict['--no-headers'])

        elif docopt_dict['testrun-info']:
            testrun_info(fallout_api, docopt_dict['TEST'],
                         docopt_dict['--testrun'],
                         docopt_dict['--user'],
                         wait_interval(),
                         docopt_dict['--state-only'],
                         docopt_dict['--artifacts'],
                         docopt_dict['--show-params'],
                         docopt_dict['--no-headers'])

        elif docopt_dict['abort-testrun']:
            abort_testrun(fallout_api, docopt_dict['--user'],
                          docopt_dict['TEST'],
                          docopt_dict['--testrun'])

        elif docopt_dict['testrun-artifact']:
            testrun_artifact(fallout_api,
                             docopt_dict['TEST'],
                             docopt_dict['--testrun'],
                             docopt_dict['PATH'],
                             docopt_dict['--user'],
                             wait_interval(),
                             docopt_dict['--stdout'])

        elif docopt_dict['delete-testrun']:
            delete_testrun(fallout_api, docopt_dict['--user'],
                           docopt_dict['TEST'],
                           docopt_dict['--testrun'])

        elif docopt_dict['delete-testruns']:
            delete_testruns(fallout_api, docopt_dict['--user'],
                            docopt_dict['TEST'])

        elif docopt_dict['run-script']:
            # Parse the rest of the parameters
            params = parse_key_value_parameters(docopt_dict['PARAMS'])

            run_script(fallout_api,
                       docopt_dict['--user'],
                       docopt_dict['TEST'],
                       docopt_dict['--testrun'],
                       docopt_dict['SCRIPT'],
                       params)

        elif docopt_dict['search-yaml']:
            search_yaml(fallout_api,
                        docopt_dict['--user'],
                        docopt_dict['--all-users'],
                        docopt_dict['REGEX'])

    except requests.HTTPError as ex:
        print("{}:\n{}".format(ex, ex.response.text), file=sys.stderr)
        sys.exit(1)

    except ParameterError as ex:
        print(ex)
        sys.exit(1)


if __name__ == "__main__":
    main()
