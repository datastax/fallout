import os

from pathlib import Path

from no_op.main import run


def test_that_it_works(tmp_path):
    pwd = os.getcwd()
    try:
        os.chdir(tmp_path)
        expected_path = Path("no-op-was-here")
        assert not expected_path.exists()
        run()
        assert expected_path.exists()
    finally:
        os.chdir(pwd)
