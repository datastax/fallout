import sys
import unittest

import pandas as pd

from pandas.testing import assert_frame_equal

from src.histo_freq.utils import create_df_w_lats_ranges_and_freqs

sys.path.append("...")  # To ensure Python adds the higher-level directory to the module's path.


class TestUtils(unittest.TestCase):

    def test_create_df_w_lats_ranges_and_freqs(self):

        first_bucket = [1, 2, 3]
        second_bucket = [4, 5]
        list_of_buckets = [first_bucket, second_bucket]
        list_of_freqs = [3, 2]

        expected_df = pd.DataFrame({
            'buckets_ranges': ['1-3', '4-5'],
            'frequencies_of_latencies': list_of_freqs
        })

        result_df = create_df_w_lats_ranges_and_freqs(
            list_of_buckets=list_of_buckets,
            list_of_frequencies=list_of_freqs
        )

        assert_frame_equal(expected_df, result_df)
