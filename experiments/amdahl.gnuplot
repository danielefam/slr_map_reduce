# amdahl.gnuplot — render the strong-scaling / Amdahl's-law plot.
#
# Reads the CSV produced by run_benchmark.sh:
#     nodes,median_seconds,speedup,runs
# and plots measured speedup (column 3) vs node count (column 1) with:
#   - x-axis on a log-base-2 scale (tics at powers of two)
#   - y-axis linear (speedup)
#   - the measured points
#   - a fitted Amdahl's-law curve  S(N) = 1 / ((1 - p) + p/N)
#   - an ideal linear-speedup reference line  S(N) = N
#
# Usage:
#   gnuplot -e "datafile='results.csv'; outfile='amdahl.png'" amdahl.gnuplot
#
# datafile / outfile may be omitted; they default to the values below.

if (!exists("datafile")) datafile = "results.csv"
if (!exists("outfile"))  outfile  = "amdahl.png"

set datafile separator comma
set datafile missing ""

set terminal pngcairo size 960,640 enhanced font "sans,11"
set output outfile

set title "Strong Scaling — Amdahl's Law" font "sans,14"
set xlabel "Number of worker nodes (log_2 scale)"
set ylabel "Speedup  S(N) = T(1) / T(N)"

set logscale x 2
set xrange [0.9:160]
set yrange [0:*]
set xtics (1, 2, 4, 8, 16, 32, 64, 128)
set grid xtics ytics
set key top left

# ── Fit Amdahl's law to the measured speedups ────────────────────────────────
# p = parallelizable fraction of the work.
amdahl(x) = 1.0 / ((1.0 - p) + p / x)
p = 0.9
set fit quiet
set fit logfile "/dev/null"
fit amdahl(x) datafile using 1:3 via p

# Clamp p into [0,1] for display purposes.
p_disp = (p < 0 ? 0 : (p > 1 ? 1 : p))
set label 1 sprintf("Amdahl fit: p = %.4f  (serial = %.4f)", p_disp, 1.0 - p_disp) \
    at graph 0.30, 0.92 font "sans,11"

# ── Plot: measured points, fitted curve, ideal reference ─────────────────────
set samples 400
plot \
    datafile using 1:3 every ::1 with points pt 7 ps 1.6 lc rgb "#1f77b4" \
        title "Measured speedup", \
    amdahl(x) with lines lw 2 lc rgb "#d62728" \
        title sprintf("Amdahl fit (p=%.3f)", p_disp), \
    x with lines dt 2 lw 1.5 lc rgb "#7f7f7f" \
        title "Ideal linear speedup"
