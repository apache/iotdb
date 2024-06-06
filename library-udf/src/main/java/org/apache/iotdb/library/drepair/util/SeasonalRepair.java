package org.apache.iotdb.library.drepair.util;

import java.util.ArrayList;

public class SeasonalRepair {
    private final long[] td_time;
    private final double[] td_dirty;
    private final double[] td_repair;
    private final int period;
    private final double k;  // k*mad
    private final int max_iter;

    private double mu, sigma;
    private double[] seasonal, trend, residual;
    private final int size;
    private final long cost_time;

    public SeasonalRepair(long[] td_time, double[] td_dirty, int period, double k, int max_iter) throws Exception {
        this.td_time = td_time;
        this.td_dirty = td_dirty;
        this.td_repair = new double[td_dirty.length];
        this.period = period;
        this.k = k;
        this.max_iter = max_iter;

        this.size = td_dirty.length;

        long startTime = System.currentTimeMillis();
        this.repair();
        long endTime = System.currentTimeMillis();
        this.cost_time = endTime - startTime;
//        System.out.println("SRRD time cost:" + cost_time + "ms");
    }

    private void repair() throws Exception {
        System.arraycopy(td_dirty, 0, td_repair, 0, td_dirty.length);

        int h = 0;
        for (; h < max_iter; ++h) {
            Decomposition de = new Decomposition(td_time, td_repair, period);
            seasonal = de.getSeasonal();
            trend = de.getTrend();
            residual = de.getResidual();

            estimate();

            boolean flag = true;
            for (int i = 0; i < size; ++i) {
                if (sub(residual[i], mu) > k * sigma) {
                    flag = false;
                    td_repair[i] = generate(i);
                }
            }
            if (flag) break;
        }
//        System.out.println("Stop after " + (h + 1) + " iterations");
    }


    private void estimate() {
        // get the sum of array
        double sum = 0.0;
        for (double i : residual) {
            sum += i;
        }

        // get the mean of array
        int length = residual.length;

        mu = sum / length;

        // calculate the standard deviation
        double standardDeviation = 0.0;
        for (double num : residual) {
            standardDeviation += Math.pow(num - mu, 2);
        }

        sigma = Math.sqrt(standardDeviation / length);
    }

    private double generate(int pos) {
        // in each cycle
        int i = pos % period;
        ArrayList<Double> arr = new ArrayList<>();

        for (int j = 0; j < size / period; ++j)
            if (j * period + i != pos)  // remove anomaly
                arr.add(residual[j * period + i]);
        if (i < size % period && i + (size / period) * period != pos)
            arr.add(residual[i + (size / period) * period]);

        double[] cal_median = new double[arr.size()];
        for (int j = 0; j < arr.size(); j++)
            cal_median[j] = arr.get(j);

        return get_median(cal_median) + seasonal[pos % period] + trend[pos];
    }

    private double sub(double a, double b) {
        return a > b ? a - b : b - a;
    }

    private double get_median(double[] A) {
        return quickSelect(A, 0, A.length - 1, A.length / 2);
    }

    private double quickSelect(double[] A, int l, int r, int k) {
        if (l >= r) return A[k];

        double x = A[l];
        int i = l - 1, j = r + 1;
        while (i < j) {
            do i++; while (A[i] < x);
            do j--; while (A[j] > x);
            if (i < j) {
                double temp = A[i];
                A[i] = A[j];
                A[j] = temp;
            }
        }

        if (k <= j) return quickSelect(A, l, j, k);
        else return quickSelect(A, j + 1, r, k);
    }

    public double[] getTd_repair() {
        return td_repair;
    }

    public long getCost_time() {
        return cost_time;
    }
}

