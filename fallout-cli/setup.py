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

from setuptools import setup, find_packages
import os

here = os.path.abspath(os.path.dirname(__file__))

with open(os.path.join(here, 'README.md')) as f:
    long_description = f.read()


# -----------------------------------------------------------------------------
# Stolen from https://github.com/apache/arrow/pull/2413, to workaround
# https://github.com/pypa/setuptools_scm/issues/176

# If the event of not running from a git clone (e.g. from a git archive
# or a Python sdist), then show a suitable error message
if not os.path.exists('../.git'):
    if os.path.exists('PKG-INFO'):
        # We're probably in a Python sdist, setuptools_scm will handle fine
        pass
    else:
        raise RuntimeError(
            "`pip install .` does not work with fallout-cli; please "
            "use `pip install -e .`")

# -----------------------------------------------------------------------------


SETUPTOOLS_SCM_REQUIREMENT = 'setuptools_scm~=3.3.3'

setup(
    name='fallout-client',
    version='1.0.0',
    description='Fallout API and Command-line Client',
    long_description=long_description,
    author='DSE Test Tools',
    author_email='oss-fallout@datastax.com',
    url='https://github.com/datastax/fallout',
    packages=find_packages(),
    entry_points={'console_scripts': ['fallout = fallout.cli:main']},
    install_requires=[
        'requests~=2.19',
        'docopt~=0.6',
        'pystache~=0.5',
        'future~=0.16',
        'blessed~=1.15',
        'ruamel.yaml==0.15.89',
        SETUPTOOLS_SCM_REQUIREMENT
    ],
    classifiers=[
        "License :: OSI Approved :: Apache Software License",
        "Programming Language :: Python",
        'Programming Language :: Python :: 2.7',
        'Programming Language :: Python :: 3.5'
    ],
    setup_requires=[
        SETUPTOOLS_SCM_REQUIREMENT
    ],
    use_scm_version={
        "root": "..",
        "relative_to": __file__,
        "version_scheme": "post-release"
    }
)
