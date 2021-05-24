# MTTR calculation tool

This tool calculates MTTR metrics for chaos tests. It does its calculation based on the CSV files produced by the `hdr_split_to_csv` artifact checker.

It takes one or more [globs](https://docs.python.org/fr/3.6/library/glob.html) as parameter. They are used to select which CSV files the MTTR metrics should be calculated for.

      artifact_checkers:
        mttr:
          artifact_checker: tool
          properties:
            tool: mttr
            args: >-
              phase*one--*.csv
              phase*two--*.csv
