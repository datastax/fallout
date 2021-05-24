import os

import filecmp
from pathlib import Path
from shutil import copyfile

from mttr.main import calculate_mttr


def test_that_it_works(tmp_path):
    pwd = os.getcwd()
    # Copy the CSV input file to analyse in the temporary directory for analysis
    # Also copy the expected output so that it can be verified
    csv_filename = "phase3-start-tenant-tenant1-read-workload.block9--read-from-table-one--success.phase3-start-tenant-tenant1-read-workload.union.csv"
    copyfile(f"{pwd}/tests/{csv_filename}", f"{tmp_path}/{csv_filename}")
    copyfile(f"{pwd}/tests/expected-mttr.csv", f"{tmp_path}/expected-mttr.csv")
    print(f"Temp dir is {tmp_path}")
    try:
        os.chdir(tmp_path)
        expected_path = Path("mttr.csv")
        assert not expected_path.exists()
        calculate_mttr(["phase3-*.csv"])
        assert filecmp.cmp(f"{tmp_path}/expected-mttr.csv", f"{tmp_path}/mttr.csv")
    finally:
        os.chdir(pwd)
