import glob
import os
import pandas as pd
import sys


def calculate_mttr(input_files_globs):
    print(f"# Calculating MTTR from {input_files_globs}")
    output_file = "mttr.csv"

    raw_results = []
    for input_files_glob in input_files_globs:
        print(f"# Current input_file glob is {input_files_glob}")
        for index, input_file in enumerate(sorted(glob.glob(input_files_glob))):
            print(f"# Processing {index}th file {os.path.basename(input_file)}...")
            dataframe = pd.read_csv(input_file)

            # An outlier is when a metric goes above `mean + n * stddev`
            # Throughput is special, it is constant in our tests, so 1x stddev difference is already a lot
            # Average latencies accepts more variance, so 3x stddev for the threshold
            for metric, threshold in [("Avg", 3), ("Throughput", 1)]:
                mean = dataframe[metric].mean()
                std = dataframe[metric].std()
                dataframe[f"{metric} outlier"] = (
                    False
                    | (dataframe[metric] < (mean - threshold * std))
                    | (dataframe[metric] > (mean + threshold * std))
                )
                print(
                    f"# Found {len(dataframe[dataframe[f'{metric} outlier'] == True])} {metric} outliers"
                )

            # Chaotic period is when we have `x` consecutive outliers, except during the first and last `t` seconds of the test
            # For now, x=1 and t=30
            # I.e. all phases during which there is a single outlier, except for the first and last 30 seconds, as taken into account
            chaotic_period_duration = 1
            ignored_period_in_seconds = 30
            dataframe["Chaotic period"] = (
                True
                & (
                    dataframe["Relative timestamp"]
                    > dataframe["Relative timestamp"].min() + ignored_period_in_seconds
                )
                & (
                    dataframe["Relative timestamp"]
                    < dataframe["Relative timestamp"].max() - ignored_period_in_seconds
                )
                & (False | dataframe["Avg outlier"] | dataframe["Throughput outlier"])
            ).rolling(chaotic_period_duration).apply(lambda l: all(l)) == 1
            print(
                f"# Found {len(dataframe[dataframe['Chaotic period'] == True])} chaotic periods"
            )

            dataframe["Chaotic period start"] = (
                dataframe["Chaotic period"].apply(lambda b: int(b)).diff() == 1
            )
            dataframe["Chaotic period end"] = (
                dataframe["Chaotic period"].apply(lambda b: int(b)).diff() == -1
            )

            # MTTR is given by the total duration of all chaotic periods divided by the number of chaotic periods
            num_separate_chaotic_periods = len(
                dataframe[dataframe["Chaotic period start"]]
            )
            if num_separate_chaotic_periods == 0:
                total_chaotic_period_duration = 0
                MTTR = 0
                chaotic_periods_timestamps = ""
            else:
                total_chaotic_period_duration = dataframe[["Chaotic period"]].sum()[0]
                MTTR = total_chaotic_period_duration / num_separate_chaotic_periods
                chaotic_periods_timestamps = (
                    pd.DataFrame(
                        {
                            "From timestamp": dataframe[
                                dataframe["Chaotic period start"]
                            ].reset_index()["Relative timestamp"],
                            "To timestamp": dataframe[
                                dataframe["Chaotic period end"]
                            ].reset_index()["Relative timestamp"],
                        }
                    )
                    .apply(lambda l: f"{l[0]}->{l[1]}", axis=1)
                    .str.cat(sep=" ")
                )

            raw_results.append(
                {
                    "File": os.path.basename(input_file),
                    "MTTR": round(MTTR, 2),
                    "Total chaotic period duration": total_chaotic_period_duration,
                    "Number of chaotic periods": num_separate_chaotic_periods,
                    "Chaotic periods timestamps": chaotic_periods_timestamps,
                }
            )
    pd.DataFrame(raw_results).to_csv(output_file, index=False)


def run():
    calculate_mttr(sys.argv[1:])


if __name__ == "__main__":
    run()
