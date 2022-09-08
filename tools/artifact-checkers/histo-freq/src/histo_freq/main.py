# This file contains the function used to plot a histogram of frequencies for buckets of latencies.

import glob
import json
from typing import List

import plotly.express as px

from .constants import (ADJUST_HIST_BAR_WIDTHS, BAR_GAP, COL_NAME_BUCKETS,
                        COL_NAME_FREQS, HIST_FILE_EXT, HIST_TITLE, IS_VERTICAL,
                        LABEL_FREQ, LABEL_LAT_RANGES, ORIENTATION_HORIZ,
                        ORIENTATION_VERT, TICK_MODE)
from .utils import create_df_w_lats_ranges_and_freqs


def plot_histogram(
    list_of_buckets: List[List[float]],
    list_of_frequencies: List[int],
    file_name_wo_ext: str,
    adjust_hist_bar_widths: bool = ADJUST_HIST_BAR_WIDTHS,
    bar_gap: float = BAR_GAP,
    file_ext: str = HIST_FILE_EXT,
    is_vertical: bool = IS_VERTICAL
) -> None:
    """
    Generate a vertical (by default) or horizontal histogram graph and save it to a .html file.

    Args:
        list_of_buckets: List[List[float]]
                        a list of lists containing latencies (floats), wherein each sub-list is a bucket.
        list_of_frequencies: List[int]
                            a list of frequencies (integers).
        file_name_wo_ext: str
                        The file_name_wo_ext to add to the histogram's file name, e.g., 'READ-st' or 'WRITE-st',
                        preceding the file's extension.
        adjust_hist_bar_widths: bool
                            whether the histogram's bars' widths have to be adjusted to reflect each bucket's range of
                            values (False by default as some ranges of latencies may be too high with respect to
                            other ranges of latencies, and thus it is suggested not to adjust them. However, this
                            parameter is included here to make it reusable for other purposes in future if needed.).
        bar_gap: float
                the gap between the bars of the histogram.
        file_ext: str
                The file's extension. By default, it is '.html', so that the histogram graph is interactive, e.g.,
                            by hovering the mouse over the bars, the x and y values corresponding to each bar can be
                            visualised interactively.
        is_vertical: bool
                    a boolean indicating whether the histogram should be plotted vertically (if True, by default) or
                    horizontally (as False).
    """

    df_w_two_cols = create_df_w_lats_ranges_and_freqs(
        list_of_buckets=list_of_buckets,
        list_of_frequencies=list_of_frequencies
    )

    if is_vertical:
        y_axis = COL_NAME_FREQS
        y_label = LABEL_FREQ
        x_label = LABEL_LAT_RANGES
        x_axis = COL_NAME_BUCKETS
        orientation = ORIENTATION_VERT
        tickvals = df_w_two_cols[COL_NAME_BUCKETS]
    else:
        y_axis = COL_NAME_BUCKETS
        y_label = LABEL_LAT_RANGES
        x_label = LABEL_FREQ
        x_axis = COL_NAME_FREQS
        orientation = ORIENTATION_HORIZ
        tickvals = df_w_two_cols[COL_NAME_FREQS]

    ticktext = tickvals
    tickmode = TICK_MODE

    fig = px.bar(
        df_w_two_cols,
        y=y_axis,
        x=x_axis,
        labels=
        {
            COL_NAME_BUCKETS: LABEL_LAT_RANGES,
            COL_NAME_FREQS: LABEL_FREQ
        },
        orientation=orientation).update_layout(
        bargap=bar_gap,
        title={'text': HIST_TITLE},
        yaxis_title=y_label,
        yaxis=dict(title=y_label, tickmode=tickmode, tickvals=ticktext, ticktext=ticktext),
        xaxis=dict(title=x_label, tickmode=tickmode, tickvals=ticktext, ticktext=ticktext)
    )

    if adjust_hist_bar_widths:
        # Update the width of the bars in the histogram to reflect each bucket's range of values.
        list_of_diffs = []
        for bucket in list_of_buckets:
            list_of_diffs.append(max(bucket) - min(bucket))
        fig.update_traces(width=list_of_diffs)

    # As suggested on GitHub at https://github.com/plotly/plotly.py/issues/2876#issuecomment-1111339561,
    # the line below overwrites the default 'sum of' shown by plotly when hovering the mouse on each bar
    # in the histogram, in this case with nothing (an empty string), as not needed, so that the y-axis label
    # defined above is kept as is.
    fig.for_each_trace(lambda t: t.update(hovertemplate=t.hovertemplate.replace("sum of", "")))

    # The filename and the .html extension are passed to export a html file with the histogram graph on it.
    fig.write_html(f"{file_name_wo_ext}{file_ext}")


def run():

    files_to_find = ["**/WRITE-st.json", "**/READ-st.json"]

    # After identifying the above-mentioned json files in their directory, output .html files with the same file names
    # as above (i.e., WRITE-st.html and READ-st.html), but having the corresponding interactive histogram graph
    # displayed on each of them.
    for file_to_find in files_to_find:

        file_name_w_ext = glob.glob(file_to_find, recursive=True)
        file_name_w_ext_str = file_name_w_ext[0]

        with open(file_name_w_ext_str) as file:
            json_dict = json.load(file)

            list_of_buckets = json_dict.get('listOfBuckets')
            list_of_frequencies = json_dict.get('listOfFrequencies')

            # Remove empty lists and zero frequencies from the json files as they were mostly created
            # due to the aggregated histograms being passed.
            list_of_buckets = [bucket for bucket in list_of_buckets if len(bucket) > 0]
            list_of_frequencies = [frequency for frequency in list_of_frequencies if frequency != 0]

            file_name_wo_ext = file_name_w_ext[0].split('.json')[0]
            plot_histogram(
                list_of_buckets=list_of_buckets,
                list_of_frequencies=list_of_frequencies,
                file_name_wo_ext=file_name_wo_ext
            )


if __name__ == "__main__":
    run()
