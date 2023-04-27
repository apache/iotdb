/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.iotdb.commons.udf.builtin;

import org.apache.iotdb.commons.udf.utils.DecompositionUtil;
import org.apache.iotdb.commons.udf.utils.DualHeapUtil;

import org.apache.iotdb.db.query.udf.api.UDTF;
import org.apache.iotdb.db.query.udf.api.access.Row;
import org.apache.iotdb.db.query.udf.api.access.RowIterator;
import org.apache.iotdb.db.query.udf.api.access.RowWindow;
import org.apache.iotdb.db.query.udf.api.collector.PointCollector;
import org.apache.iotdb.db.query.udf.api.customizer.config.UDTFConfigurations;
import org.apache.iotdb.db.query.udf.api.customizer.parameter.UDFParameterValidator;
import org.apache.iotdb.db.query.udf.api.customizer.parameter.UDFParameters;
import org.apache.iotdb.db.query.udf.api.customizer.strategy.SlidingSizeWindowAccessStrategy;

import java.util.ArrayList;

public class UDTFSeasonalRepair implements UDTF {
    private final ArrayList<Long> time = new ArrayList<>();
    private final ArrayList<Double> original = new ArrayList<>();

    private String method;
    private int period;
    private double k;
    private int max_iter;

    @Override
    public void validate(UDFParameterValidator validator) throws Exception {
        validator
                .validateInputSeriesNumber(1)
                .validate(
                        method -> ((String) method).equalsIgnoreCase("classical")
                                || ((String) method).equalsIgnoreCase("improved"),
                        "\"method\" should be \"classical\" or \"improved\"",
                        validator.getParameters().getStringOrDefault("method", "classical"))
                .validate(
                        period -> (int) period > 0,
                        "\"period\" must be a positive integer.",
                        validator.getParameters().getInt("period"))
                .validate(
                        k -> (double) k > 0.0,
                        "\"k\" must be a positive float number.",
                        validator.getParameters().getDoubleOrDefault("k", 9.0))
                .validate(
                        max_iter -> (int) max_iter > 0,
                        "\"max_iter\" must be a positive integer.",
                        validator.getParameters().getIntOrDefault("max_iter", 10));
    }

    @Override
    public void beforeStart(UDFParameters udfParameters, UDTFConfigurations udtfConfigurations) throws Exception {
        udtfConfigurations.setAccessStrategy(new SlidingSizeWindowAccessStrategy(Integer.MAX_VALUE))
                .setOutputDataType(udfParameters.getDataType(0));
        method = udfParameters.getStringOrDefault("method", "classical");
        period = udfParameters.getInt("period");
        k = udfParameters.getDoubleOrDefault("k", 9.0);
        max_iter = udfParameters.getIntOrDefault("max_iter", 10);
    }

    @Override
    public void transform(RowWindow rowWindow, PointCollector collector) throws Exception {
        RowIterator rowIterator = rowWindow.getRowIterator();
        while (rowIterator.hasNextRow()) {  // load data
            Row row = rowIterator.next();
            time.add(row.getTime());
            double value = switch (row.getDataType(0)) {
                case INT32 -> row.getInt(0);
                case INT64 -> row.getLong(0);
                case FLOAT -> row.getFloat(0);
                case DOUBLE -> row.getDouble(0);
                default -> throw new Exception();
            };
            original.add(value);
        }
        processNaN();
    }

    @Override
    public void terminate(PointCollector collector) throws Exception {
        if (method.equalsIgnoreCase("classical")) {
            ClassicalSeasonal classicalSeasonal = new ClassicalSeasonal(time, original, period, k, max_iter);
            ArrayList<Double> repaired = classicalSeasonal.getTd_repair();
            for (int i = 0; i < original.size(); ++i) {
                collector.putDouble(time.get(i), repaired.get(i));
            }
        } else if (method.equalsIgnoreCase("improved")) {
            ErrorTolerantSeasonal errorTolerantSeasonal = new ErrorTolerantSeasonal(time, original, period, k, max_iter);
            ArrayList<Double> repaired = errorTolerantSeasonal.getTd_repair();
            for (int i = 0; i < original.size(); ++i) {
                collector.putDouble(time.get(i), repaired.get(i));
            }
        }
    }

    private void processNaN() throws Exception {
        int index1 = 0, index2, n = original.size();
        while (index1 < n && Double.isNaN(original.get(index1))) {
            index1++;
        }
        index2 = index1 + 1;
        while (index2 < n && Double.isNaN(original.get(index2))) {
            index2++;
        }
        if (index2 >= n) {
            throw new Exception("At least two non-NaN values are needed");
        }
        for (int i = 0; i < index2; i++) {
            original.set(i, original.get(index1) + (original.get(index2) - original.get(index1)) * (time.get(i) - time.get(index1)) / (time.get(index2) - time.get(index1)));
        }
        for (int i = index2 + 1; i < n; i++) {
            if (!Double.isNaN(original.get(i))) {
                index1 = index2;
                index2 = i;
                for (int j = index1 + 1; j < index2; j++) {
                    original.set(j, original.get(index1) + (original.get(index2) - original.get(index1)) * (time.get(j) - time.get(index1)) / (time.get(index2) - time.get(index1)));
                }
            }
        }
        for (int i = index2 + 1; i < n; i++) {
            original.set(i, original.get(index1) + (original.get(index2) - original.get(index1)) * (time.get(i) - time.get(index1)) / (time.get(index2) - time.get(index1)));
        }
    }
}


class ClassicalSeasonal {
    private final ArrayList<Long> td_time;
    private final ArrayList<Double> td_dirty;
    private final ArrayList<Double> td_repair = new ArrayList<>();
    private final int period;
    private final double k;  // k*std
    private final int max_iter;

    private double mean, std;
    private ArrayList<Double> seasonal, trend, residual;
    private final int size;

    public ClassicalSeasonal(ArrayList<Long> td_time, ArrayList<Double> td_dirty, int period, double k, int max_iter) throws Exception {
        this.td_time = td_time;
        this.td_dirty = td_dirty;
        this.period = period;
        this.k = k;
        this.max_iter = max_iter;

        this.size = td_dirty.size();

        long startTime = System.currentTimeMillis();
        this.repair();
        long endTime = System.currentTimeMillis();
        System.out.println("ClassicalSeasonal time cost:" + (endTime - startTime) + "ms");
    }

    private void repair() throws Exception {
        td_repair.addAll(td_dirty);

        for (int h = 0; h < max_iter; ++h) {
            Decomposition de = new Decomposition(td_time, td_repair, period, "classical");
            seasonal = de.getSeasonal();
            trend = de.getTrend();
            residual = de.getResidual();

            System.out.println(residual);

            estimate();

            boolean flag = true;
            for (int i = 0; i < size; ++i) {
                if (sub(residual.get(i), mean) > k * std) {
                    flag = false;
                    td_repair.set(i, generate(i));
                }
            }
            if (flag) break;
        }
    }

    private void estimate() throws Exception {
        mean = 0.0;
        double cnt = 0.0;
        // mean
        for (double d : residual) {
            if (!Double.isNaN(d)) {
                cnt += 1;
                mean += d;
            }
        }
        mean /= cnt;

        std = 0.0;
        // std
        for (double d : residual) {
            if (!Double.isNaN(d)) {
                std += (d - mean) * (d - mean);
            }
        }
        std /= cnt;
        std = Math.sqrt(std);
    }

    private double generate(int pos) throws Exception {
        // in each cycle
        int i = pos % period;
        double sum = 0.0, cnt = 0.0, rtn;
        for (int j = 0; j < size / period; ++j)
            if (j * period + i != pos && !Double.isNaN(residual.get(j * period + i))) {  // remove anomaly
                sum += residual.get(j * period + i);
                cnt += 1.0;
            }
        if (i < size % period && i + (size / period) * period != pos && !Double.isNaN(residual.get(i + (size / period) * period))) {
            sum += residual.get(i + (size / period) * period);
            cnt += 1.0;
        }

        rtn = sum / cnt + seasonal.get(pos) + trend.get(pos);
        return rtn;
    }

    private double sub(double a, double b) {
        return a > b ? a - b : b - a;
    }

    public ArrayList<Double> getTd_repair() {
        return td_repair;
    }

}

class ErrorTolerantSeasonal {
    private final ArrayList<Long> td_time;
    private final ArrayList<Double> td_dirty;
    private final ArrayList<Double> td_repair = new ArrayList<>();
    private final int period;
    private final double k;  // k*mad
    private final int max_iter;

    private double mid, mad;
    private ArrayList<Double> seasonal, trend, residual;
    private final DualHeap dh = new DualHeap();
    private final int size;

    public ErrorTolerantSeasonal(ArrayList<Long> td_time, ArrayList<Double> td_dirty, int period, double k, int max_iter) throws Exception {
        this.td_time = td_time;
        this.td_dirty = td_dirty;
        this.period = period;
        this.k = k;
        this.max_iter = max_iter;

        this.size = td_dirty.size();

        long startTime = System.currentTimeMillis();
        this.repair();
        long endTime = System.currentTimeMillis();
        System.out.println("ImprovedSeasonal time cost:" + (endTime - startTime) + "ms");
    }

    private void repair() throws Exception {
        td_repair.addAll(td_dirty);

        for (int h = 0; h < max_iter; ++h) {
            Decomposition de = new Decomposition(td_time, td_repair, period, "improved", "constant");
            seasonal = de.getSeasonal();
            trend = de.getTrend();
            residual = de.getResidual();

            estimate();

            boolean flag = true;
            for (int i = 0; i < size; ++i) {
                if (sub(residual.get(i), mid) > k * mad) {
                    flag = false;
                    td_repair.set(i, generate(i));
                }
            }
            if (flag) break;
        }
    }

    private void estimate() {
        // mid
        for (double d : residual)
            dh.insert(d);
        this.mid = dh.getMedian();
        dh.clear();
        //mad
        for (double d : residual)
            dh.insert(sub(d, mid));
        this.mad = dh.getMedian();
        dh.clear();
    }

    private double generate(int pos) {
        // in each cycle
        int i = pos % period;
        double rtn;
        for (int j = 0; j < size / period; ++j)
            if (j * period + i != pos)  // remove anomaly
                dh.insert(residual.get(j * period + i));
        if (i < size % period && i + (size / period) * period != pos)
            dh.insert(residual.get(i + (size / period) * period));

        rtn = dh.getMedian() + seasonal.get(pos) + trend.get(pos);
        dh.clear();
        return rtn;
    }

    private double sub(double a, double b) {
        return a > b ? a - b : b - a;
    }

    public ArrayList<Double> getTd_repair() {
        return td_repair;
    }

}
