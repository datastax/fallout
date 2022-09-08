# README

The `histo-freq` tool runs automatically when executing a test in Fallout and generates a
histogram graph given a list of latency buckets and a list of their corresponding frequencies.

For ease of visualisation, by default, it plots the histogram vertically and with bars of equal width.
Nevertheless, to suit various preferences regarding its visualisation, it provides parameters for a developer/user to 
plot the histogram horizontally and/or with bars of adjusted width based on the range of values in each latency bucket.

Please follow the instructions below to set up the required environment and run the tool via `poetry`, 
and/or run the tests via `pytest` (with or without showing the test coverage).

Please do not edit the `poetry.lock` file; should you wish to add any dependencies in the project's file, 
please edit the `pyproject.toml` file.

<br />

First, install `poetry`:
```
https://python-poetry.org/docs/#osx--linux--bashonwindows-install-instructions
```

To install the project:
```
poetry install
```

To activate the environment:
```
poetry shell
```

To deactivate the environment:
```
exit
```

To add a dependency:
```
poetry add <library_name>
```

To update all dependencies to the latest possible versions:
```
poetry update
```

Please move to the following directory before running the tests: 
```
cd Fallout/fallout-oss/tools/artifact-checkers/histo-freq
```

To run the tests **without** showing the test coverage:
```
pytest tests
```

To run the tests **and** show the test coverage:
```
pytest --cov=src tests --cov-report term-missing
```
