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

from __future__ import unicode_literals
# ============================================================================
# Python 2/3 compatibility boilerplate
from __future__ import (absolute_import, division, print_function,
                        unicode_literals)

from future.builtins import *  # noqa: F401, F403
from future import standard_library
standard_library.install_aliases()

__metaclass__ = type

# ============================================================================

import logging
import re
import os
from distutils.version import LooseVersion
from functools import reduce
from http.client import HTTPConnection
from urllib.parse import urljoin, quote_plus
from requests.utils import default_user_agent
from fallout.version import VERSION_STRING

from requests import Session
from requests.status_codes import codes
import json as jsonlib

LOG = logging.getLogger(__file__)
INCOMPATIBLE_WITH_SERVER_VERSIONS_UP_TO = LooseVersion("fallout-1.71.0")


class AuthPreservingRedirectSession(Session):

    def __init__(self, oauth_token):
        super().__init__()
        self.__oauth_token = oauth_token
        self.auth = self.__add_oauth_header

    def rebuild_auth(self, prepared_request, response):
        """Override Session.rebuild_auth to reinsert our auth after a
        redirect"""
        super().rebuild_auth(prepared_request, response)
        self.__add_oauth_header(prepared_request)

    def __add_oauth_header(self, request):
        request.headers['Authorization'] = 'Bearer ' + self.__oauth_token
        return request


class FalloutAPI:

    def __init__(self, endpoint_url, oauth_token, debug=False):
        self.endpoint_url = endpoint_url
        self.session = AuthPreservingRedirectSession(oauth_token)
        build_url = "" if os.getenv("BUILD_URL") is None else " BUILD_URL={}".format(os.getenv("BUILD_URL"))
        self.session.headers['User-Agent'] = '{} fallout-cli/{}{}'.\
            format(default_user_agent(), VERSION_STRING, build_url)

        if debug:
            HTTPConnection.debuglevel = 1
            logging.basicConfig(level=logging.DEBUG)
            requests_log = logging.getLogger("requests.packages.urllib3")
            requests_log.setLevel(logging.DEBUG)
            requests_log.propagate = True

    @staticmethod
    def __urlappend(l, r):
        return urljoin(l.rstrip('/') + '/', r)

    def __path(self, path_components):
        return reduce(
            FalloutAPI.__urlappend,
            [self.endpoint_url] + path_components)

    def __api(self, path_components):
        return urljoin(self.__path(path_components) + "/", "api")

    def __get(self, path_components, **kwargs):
        return self.session.get(self.__api(path_components), **kwargs)

    def __get_path(self, path_components, **kwargs):
        return self.session.get(self.__path(path_components), **kwargs)

    def __get_json(self, path_components, **kwargs):
        return self.session.get(
            self.__api(path_components),
            headers={'Accept': 'application/json'}, **kwargs)

    def __put(self, path_components, **kwargs):
        return self.session.put(self.__api(path_components), **kwargs)

    def __post(self, path_components, **kwargs):
        return self.session.post(self.__api(path_components), **kwargs)

    def __post_json(self, path_components, **kwargs):
        return self.session.post(
            self.__api(path_components),
            headers={'Accept': 'application/json'}, **kwargs)

    def __delete(self, path_components, **kwargs):
        return self.session.delete(self.__api(path_components), **kwargs)

    @staticmethod
    def __handle(response):
        LOG.debug(response.reason)
        response.raise_for_status()
        return response

    def validate_server_version(self):
        response = FalloutAPI.__handle(self.__get(['version']))
        if LooseVersion(response.text) <= INCOMPATIBLE_WITH_SERVER_VERSIONS_UP_TO:
            raise RuntimeError("Connected to a fallout server of version {} but "
                               "this version of fallout-cli can only connect to versions higher than {}"
                               .format(response.text, INCOMPATIBLE_WITH_SERVER_VERSIONS_UP_TO))

    def create_or_update_test(self, test_name, test_filename):
        """Create or update test_name with test_filename.  Returns True on
        success and throws in all other cases."""

        with open(test_filename, 'r', encoding='utf-8') as test_file:
            response = self.__put(
                ['tests', quote_plus(test_name)],
                headers={'Content-Type': 'application/yaml'},
                data=test_file.read())

            FalloutAPI.__handle(response)

            return True

    def validate_test(self, test_filename, params={}, json=None, yaml=None):
        with open(test_filename, 'r', encoding='utf-8') as test_file:

            response = self.__post(['tests/validate'],
                                   headers={'content-type': 'application/json'},
                                   json={
                                       'params': params,
                                       'jsonParams': json,
                                       'yamlParams': yaml,
                                       'testYaml': test_file.read()})

        if response.status_code == codes.BAD_REQUEST:
            return (False, response.content.decode())

        FalloutAPI.__handle(response)

        return (True, response.content.decode())

    def list_tests(self, email=None):
        path = ['tests']
        if email is not None:
            path.append(email)

        response = self.__get_json(path)

        if response.status_code == codes.NOT_FOUND:
            return []

        return FalloutAPI.__handle(response).json()

    def test_info(self, test_name, email=None):
        path = ['tests']
        if email is not None:
            path.append(email)
        path.append(test_name)

        return FalloutAPI.__handle(self.__get_json(path)).json()

    def delete_test(self, test_name):
        """Delete test_name.  Succeeds if test_name exists or doesn't.  Throws
        on other failures."""

        FalloutAPI.__handle(self.__delete(['tests', test_name]))

    def create_testrun(self, test_name, params={}, json=None, yaml=None):
        response = None
        path = ['tests', test_name, 'runs']

        if json is not None:
            response = self.__post_json(
                path, json={"jsonTemplateParams": jsonlib.loads(json)})
        elif yaml is not None:
            response = self.__post_json(
                path, json={"yamlTemplateParams": yaml})
        else:
            response = self.__post_json(path, json={"jsonTemplateParams": params})

        return FalloutAPI.__handle(response).json()

    def abort_testrun(self, email, test_name, testrun_id):
        return FalloutAPI.__handle(self.__post_json(
            ['tests', email, test_name, 'runs', testrun_id, 'abort']))

    def re_run_testrun(self, email, test_name, testrun_id, clone):
        return FalloutAPI.__handle(self.__post_json(
            ['tests', email, test_name, 'runs', testrun_id, 'rerun'],
            json={"clone": clone})).json()

    def list_testruns(self, email, test_name):
        response = self.__get_json(['tests', email, test_name, 'runs'])

        if response.status_code == codes.NOT_FOUND:
            return []

        return sorted(FalloutAPI.__handle(response).json(),
                      key=lambda x: x['createdAt'])

    def testrun_info(self, email, test_name, testrun_id):
        return FalloutAPI.__handle(self.__get_json(
            ['tests', email, test_name, 'runs', testrun_id])).json()

    def testrun_artifact(self, email, test_name, testrun_id, path):
        return FalloutAPI.__handle(self.__get_path(
            ['artifacts', email, test_name, testrun_id] + path)).content

    def testrun_artifacts(self, email, test_name, testrun_id):
        return FalloutAPI.__handle(self.__get_path(
            ['tests', 'ui', email, test_name, testrun_id,
             'artifactsArchive'])).content

    def delete_testrun(self, email, test_name, testrun_id):
        return FalloutAPI.__handle(self.__delete(
            ['tests', email, test_name, 'runs', testrun_id]))

    def run_script(self, user, test_name, testrun_id, script, params):
        return FalloutAPI.__handle(
            self.__post(['scripts', user, test_name, testrun_id, script], data=params)
        ).text

    def search_yaml(self, user, all_users, regex):
        matches = []

        if all_users:
            users = self.get_all_users_map()
            for user in sorted(users, key=lambda k: k['email']):
                tests = self.list_tests(user['email'])
                for test in tests:
                    definition = test['definition']
                    if re.search(r"{}".format(regex), definition):
                        matches.append(test)
        else:
            tests = self.list_tests(user)
            for test in tests:
                definition = test['definition']
                if re.search(r"{}".format(regex), definition):
                    matches.append(test)
        return matches

    def get_all_users_map(self):
        return FalloutAPI.__handle(self.__get_json(['account', 'users'])).json()

    def expanded_testrun_definition(self, email, test_name, testrun_id):
        return FalloutAPI.__handle(self.__get_path(
            ['tests', email, test_name, testrun_id, 'expanded-yaml', 'api'])).text
