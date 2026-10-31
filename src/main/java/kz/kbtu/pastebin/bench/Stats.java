package kz.kbtu.pastebin.bench;

import java.util.Arrays;

final class Stats {
    final double[] sorted; // milliseconds
    final double mean;

    Stats(double[] ms) {
        this.sorted = ms.clone();
        Arrays.sort(sorted);
        double s = 0;
        for (double v : sorted) s += v;
        mean = sorted.length == 0 ? 0 : s / sorted.length;
    }

    double pct(double p) {
        if (sorted.length == 0) return 0;
        int i = (int) Math.ceil(p / 100.0 * sorted.length) - 1;
        return sorted[Math.max(0, Math.min(sorted.length - 1, i))];
    }

    int n() {
        return sorted.length;
    }
}
