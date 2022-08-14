# This file contains utility-type of functions to assist with the creation of a histogram graph, e.g., for preparing
# the input data before plotting them as a histogram.

from typing import List

import pandas as pd

from .constants import COL_NAME_BUCKETS, COL_NAME_FREQS, PRECISION_VAL


def create_df_w_lats_ranges_and_freqs(
    list_of_buckets: List[List[float]],
    list_of_frequencies: List[int]
) -> pd.DataFrame:
    """
    Create a dataframe of latency buckets' ranges and frequencies given two lists containing a list of buckets of
    latencies and a list of frequencies.

    Args:
        list_of_buckets: List[List[float]]
                        a list of lists containing latencies (floats), wherein each sub-list is a bucket.
        list_of_frequencies: List[int]
                            a list of frequencies (integers).

    Returns:
        a dataframe of latency buckets' ranges (strings) and frequencies (integers).
    """

    list_of_min_and_max = []
    for bucket in list_of_buckets:
        list_of_min_and_max.append(
            f"{str(round(min(bucket), PRECISION_VAL))}{'-'}{str(round(max(bucket), PRECISION_VAL))}"
        )

    df_w_lats_ranges_and_freqs = pd.DataFrame({
        COL_NAME_BUCKETS: list_of_min_and_max,
        COL_NAME_FREQS: list_of_frequencies
    })

    return df_w_lats_ranges_and_freqs
