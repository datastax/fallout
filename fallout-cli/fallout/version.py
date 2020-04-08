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

import os
import re
import sys

import pkg_resources


def make_semver(version):
    match = re.match(r"^(?P<tag>\d+\.\d+\.\d+)(?:\.post(?P<commit_count>\d+)\+g(?P<commit>[a-f\d]+)(?P<local_changes>\.d\d+)?)?", version)  # noqa: E501

    if match:
        if match.group('commit_count') is None:
            return match.group('tag')
        elif match.group('local_changes') is None:
            return "{}+{} (git {})".format(*match.groups())
        else:
            return "{}+{} (git {} + diff)".format(*match.groups())

    return version


def version_tuple():
    dist = pkg_resources.get_distribution('fallout-client')

    # Detect if we've been installed with pip -e; see
    # https://stackoverflow.com/a/42583363/322152
    def dist_is_editable(dist):
        """Is distribution an editable install?"""
        for path_item in sys.path:
            egg_link = os.path.join(path_item, dist.project_name + '.egg-link')
            if os.path.isfile(egg_link):
                return True
        return False

    if dist_is_editable(dist):
        import setuptools_scm
        # Get the current version of the checked out code, _not_ what was
        # used when this was installed with pip install -e
        version = "{version} [dev]".format(
            version=make_semver(
                setuptools_scm.get_version(root='../..', relative_to=__file__, version_scheme='post-release')))
    else:
        version = make_semver(dist.version)
    return version, dist.location


VERSION_STRING, LOCATION = version_tuple()
FULL_VERSION_STRING = "fallout-client {} ({})".format(VERSION_STRING, LOCATION)
