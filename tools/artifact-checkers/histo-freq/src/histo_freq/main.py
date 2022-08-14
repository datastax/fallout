# This file contains the constants used to plot a histogram of frequencies for buckets of latencies.

import ast
import sys

import plotly.express as px

from typing import List

from .utils import create_df_w_lats_ranges_and_freqs

from .constants import (
    ADJUST_HIST_BAR_WIDTHS,
    BAR_GAP,
    COL_NAME_BUCKETS,
    COL_NAME_FREQS,
    HIST_FILENAME_W_EXT,
    HIST_TITLE,
    IS_VERTICAL,
    LABEL_FREQ,
    LABEL_LAT_RANGES,
    ORIENTATION_HORIZ,
    ORIENTATION_VERT
)


def plot_histogram(
    list_of_buckets: List[List[float]],
    list_of_frequencies: List[int],
    adjust_hist_bar_widths: bool = ADJUST_HIST_BAR_WIDTHS,
    bar_gap: float = BAR_GAP,
    html_filename_w_ext: str = HIST_FILENAME_W_EXT,
    is_vertical: bool = IS_VERTICAL
) -> None:
    """
    Generate a vertical or horizontal (by default) histogram graph and save it to a .html file.

    Args:
        list_of_buckets: List[List[float]]
                        a list of lists containing latencies (floats), wherein each sub-list is a bucket.
        list_of_frequencies: List[int]
                            a list of frequencies (integers).
        adjust_hist_bar_widths: bool
                            whether the histogram's bars' widths have to be adjusted to reflect each bucket's range of
                            values (False by default as some ranges of latencies may be too high with respect to
                            other ranges of latencies, and thus it is suggested not to adjust them. However, this
                            parameter is included here to make it reusable for other purposes in future if needed.).
        bar_gap: float
                the gap between the bars of the histogram.
        html_filename_w_ext: str
                            the filename and extension (.html) of the html in which the histogram is to be saved. Note
                            that it is saved in html format, so that the histogram graph is interactive, e.g.,
                            by hovering the mouse over the bars, the x and y values corresponding to each bar can be
                            visualised interactively.
        is_vertical: bool
                    a boolean indicating whether the histogram should be plotted vertically (if True) or horizontally
                    (by default, as False).
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
    tickmode = 'linear'

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

    fig.write_html(html_filename_w_ext)


def run():
    plot_histogram(list_of_buckets=ast.literal_eval(sys.argv[1]), list_of_frequencies=ast.literal_eval(sys.argv[2]))


if __name__ == "__main__":
    run()
