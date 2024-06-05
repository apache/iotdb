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

package org.apache.iotdb.library.drepair;

import org.apache.iotdb.library.drepair.util.SeasonalRepair;
import org.apache.iotdb.udf.api.UDTF;
import org.apache.iotdb.udf.api.access.Row;
import org.apache.iotdb.udf.api.access.RowIterator;
import org.apache.iotdb.udf.api.access.RowWindow;
import org.apache.iotdb.udf.api.collector.PointCollector;
import org.apache.iotdb.udf.api.customizer.config.UDTFConfigurations;
import org.apache.iotdb.udf.api.customizer.parameter.UDFParameterValidator;
import org.apache.iotdb.udf.api.customizer.parameter.UDFParameters;
import org.apache.iotdb.udf.api.customizer.strategy.SlidingSizeWindowAccessStrategy;
import org.apache.iotdb.udf.api.type.Type;

import java.util.ArrayList;
import java.util.List;

/**
 * This function is used to repair the value of the time series.
 */
public class UDTFSeasonalRepair implements UDTF {
    private double k;
    private int period;
    private int max_iter;

    private final List<Long> times = new ArrayList<>();
    private final List<Double> values = new ArrayList<>();

    @Override
    public void validate(UDFParameterValidator validator) throws Exception {
        validator
                .validateInputSeriesNumber(1)
                .validateInputSeriesDataType(0, Type.FLOAT, Type.DOUBLE, Type.INT32, Type.INT64)
                .validate(
                        period -> (int) period > 0,
                        "Parameter \"period\" should be a positive integer.",
                        validator.getParameters().getInt("period"))
                .validate(
                        k -> (double) k > 0.0,
                        "parameter \"k\" should be a positive float number.",
                        validator.getParameters().getDoubleOrDefault("k", 9.0))
                .validate(
                        max_iter -> (int) max_iter > 0,
                        "parameter \"max_iter\" should be a positive integer.",
                        validator.getParameters().getIntOrDefault("max_iter", 10));
    }

    @Override
    public void beforeStart(UDFParameters parameters, UDTFConfigurations configurations) throws Exception {
        configurations.setAccessStrategy(new SlidingSizeWindowAccessStrategy(Integer.MAX_VALUE))
                .setOutputDataType(parameters.getDataType(0));
        period = parameters.getInt("period");
        k = parameters.getDoubleOrDefault("k", 9.0);
        max_iter = parameters.getIntOrDefault("max_iter", 10);
    }

    @Override
    public void transform(RowWindow rowWindow, PointCollector collector) throws Exception {
        RowIterator rowIterator = rowWindow.getRowIterator();
        while (rowIterator.hasNextRow()) { // load data
            Row row = rowIterator.next();
            times.add(row.getTime());
            double value = switch (row.getDataType(0)) {
                case INT32 -> row.getInt(0);
                case INT64 -> row.getLong(0);
                case FLOAT -> row.getFloat(0);
                case DOUBLE -> row.getDouble(0);
                default -> throw new Exception();
            };
            values.add(value);
        }
        processNaN();
    }

    @Override
    public void terminate(PointCollector collector) throws Exception {
        SeasonalRepair seasonalRepair = new SeasonalRepair(getTimesArray(), getValuesArray(), period, k, max_iter);
        double[] repaired = seasonalRepair.getTd_repair();
        for (int i = 0; i < values.size(); i++) {
            collector.putDouble(times.get(i), repaired[i]);
        }
    }

    private long[] getTimesArray() {
        long[] rtn = new long[times.size()];
        for (int i = 0; i < times.size(); ++i)
            rtn[i] = times.get(i);
        return rtn;
    }

    private double[] getValuesArray() {
        double[] rtn = new double[values.size()];
        for (int i = 0; i < values.size(); ++i)
            rtn[i] = values.get(i);
        return rtn;
    }

    private void setValues(int i, int idx1, int idx2) {
        values.set(i, values.get(idx1) + (values.get(idx2) - values.get(idx1)) * (times.get(i) - times.get(idx1)) / (times.get(idx2) - times.get(idx1)));
    }

    private void processNaN() throws Exception {
        int idx1 = 0, idx2, n = values.size();
        while (idx1 < n && Double.isNaN(values.get(idx1))) {
            idx1++;
        }
        idx2 = idx1 + 1;
        while (idx2 < n && Double.isNaN(values.get(idx2))) {
            idx2++;
        }
        if (idx2 >= n) {
            throw new Exception("At least two non-NaN values are needed");
        }
        for (int i = 0; i < idx2; i++) {
            setValues(i, idx1, idx2);
        }
        for (int i = idx2 + 1; i < n; i++) {
            if (!Double.isNaN(values.get(i))) {
                idx1 = idx2;
                idx2 = i;
                for (int j = idx1 + 1; j < idx2; j++) {
                    setValues(j, idx1, idx2);
                }
            }
        }
        for (int i = idx2 + 1; i < n; i++) {
            setValues(i, idx1, idx2);
        }
    }
}
