# README

This tool generates a histogram graph illustrating latency buckets and their corresponding frequencies. 

For ease of visualisation, by default, it plots the histogram horizontally and with bars of equal width.
Nevertheless, to suit various preferences regarding its visualisation, it provides parameters for a developer/user to 
plot the histogram vertically and/or with bars of adjusted width based on the range of values in each latency bucket.

Please follow the instructions below to set up the required environment and run the tool via `poetry`, 
and/or run the tests via `pytest` (with or without showing the test coverage).

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

To add a dependency:
```
poetry add <library_name>
```

To run the `histo-freq` tool to generate a histogram graph from the command line
(the two lists of buckets and corresponding frequencies below are just an example):
```
poetry run histo-freq "[[1, 2, 3], [4, 5]]" "[3, 2]"
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
