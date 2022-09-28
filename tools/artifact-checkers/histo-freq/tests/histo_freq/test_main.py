import json
import os
import unittest

from src.histo_freq.main import plot_histogram


class TestMain(unittest.TestCase):
    def test_plot_histogram_works_correctly(self):

        cwd = os.getcwd()
        json_input_filename = "READ-st.json"
        path_json_input_file = f"{cwd}/tests/histo_freq/{json_input_filename}"
        with open(path_json_input_file) as file:
            json_dict = json.load(file)

            list_of_buckets = json_dict.get("listOfBuckets")
            list_of_frequencies = json_dict.get("listOfFrequencies")

            # Remove empty lists and zero frequencies from the json files as they were mostly created
            # due to the aggregated histograms being passed.
            list_of_buckets = [bucket for bucket in list_of_buckets if len(bucket) > 0]
            list_of_frequencies = [
                frequency for frequency in list_of_frequencies if frequency != 0
            ]

            file_name_wo_ext = json_input_filename.split(".json")[0]

            # Call 'plot_histogram' function, thus ensuring it works correctly.
            # If an error occurred, unittest would flag it.
            plot_histogram(
                list_of_buckets=list_of_buckets,
                list_of_frequencies=list_of_frequencies,
                file_name_wo_ext=file_name_wo_ext,
            )
