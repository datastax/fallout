This is a python package containing:

* `fallout.api`, a simple python API for fallout;
* `fallout.cli`, a simple command-line client built on the above.

## Install

Whether you're using the CLI only, or the CLI and the API, you should install the correct version to use with the live server:

### CLI only - use Python 3

Python 3.6 or higher is _strongly_ preferred; python 2.7.15 is supported, but won't be one day.

### API

If you're using Python 2, or you need to use the API from your own code, then [using `pyenv` and `pyenv-virtualenv` to manage your installed pythons and virtualenvs](https://datastax.jira.com/wiki/spaces/~741246479/pages/827785323/Coping+with+python+environments#Copingwithpythonenvironments-Installpyenv) is highly recommended.

#### Using the API from Python 2

Python 2/3 compatibility is enabled using the [future](https://pypi.org/project/future/) library; you'll need to use it to use the API.  In your code, you should add this boilerplate:

```python
# ============================================================================
# Python 2/3 compatibility boilerplate
from __future__ import (absolute_import, division, print_function,
                        unicode_literals)

from future.builtins import *  # noqa: F401, F403
from future import standard_library
standard_library.install_aliases()

__metaclass__ = type

# ============================================================================
```

## Get help

```
fallout help
```

## Using the API from Python 2 code

If you have no option but to use Python 2, then you _must_

```
from __future__ import unicode_literals
```

...before using the api.

## Developing

You can build a development install of fallout and run all the tests using:

```
../gradlew tox
```

The only requirement is that `python3` is on your `PATH`.

Give arguments to tox using the gradle `toxArgs` property:

```
../gradlew tox -PtoxArgs="-e py37-ci -- -k test_normal_test_creation_and_run"
```

The `tox.ini` is set up to pass all arguments after `--` to `pytest`.

### Iterating

If you want to speed up testing, you don't need to repeat all of the steps each time; if you don't need to build fallout every time, then you can build fallout once:

```
../gradlew :installDist
```

...and run tox directly using:

```
./ci-tools/tox-bootstrap
```

This is a wrapper around `tox` that installs `tox` to a venv using whatever version of `python3` is in your `PATH` to `.tox/bootstrap` using `requirements_ci.txt`; it will forward its arguments to `tox`.

If you want to run tox directly, then install it to your development venv:

```
pip install -r requirements_ci.txt
```

If you want to run `pytest` or `flake8` directly, then install `requirements.txt` to your development venv:

```
pip install -r requirements.txt
```
