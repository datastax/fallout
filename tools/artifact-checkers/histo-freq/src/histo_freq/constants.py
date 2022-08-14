# This file contains the constants used to plot a histogram of frequencies for buckets of latencies.

# To adjust the histogram's bars' width based on the range of the values in each bucket, i.e.,
# bar width = (maximum of each bucket - minimum of each bucket).
ADJUST_HIST_BAR_WIDTHS = False

BAR_GAP = 0.1

COL_NAME_BUCKETS = 'buckets_ranges'
COL_NAME_FREQS = 'frequencies_of_latencies'

HIST_FILENAME_W_EXT = 'hist_graph_latency_freqs.html'
HIST_TITLE = "Latencies' buckets by frequency"

# Boolean determining whether the histogram should be plotted vertically (False if horizontally, True if vertically).
IS_VERTICAL = False

LABEL_FREQ = 'Frequencies'
LABEL_LAT_RANGES = 'Latency ranges (ms)'

# Orientation of the histogram as per plotly's nomenclature ('h' for horizontal, 'v' for vertical)
ORIENTATION_HORIZ = 'h'
ORIENTATION_VERT = 'v'

# Precision to be used for rounding minima and maxima of latencies for showing them as buckets' ranges on the
# histogram graph.
PRECISION_VAL = 2
