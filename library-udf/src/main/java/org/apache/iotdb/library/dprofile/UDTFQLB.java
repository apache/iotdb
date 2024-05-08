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
package org.apache.iotdb.library.dprofile;

import org.apache.iotdb.library.util.Util;
import org.apache.iotdb.udf.api.UDTF;
import org.apache.iotdb.udf.api.access.Row;
import org.apache.iotdb.udf.api.collector.PointCollector;
import org.apache.iotdb.udf.api.customizer.config.UDTFConfigurations;
import org.apache.iotdb.udf.api.customizer.parameter.UDFParameterValidator;
import org.apache.iotdb.udf.api.customizer.parameter.UDFParameters;
import org.apache.iotdb.udf.api.customizer.strategy.RowByRowAccessStrategy;
import org.apache.iotdb.udf.api.type.Type;

import org.apache.commons.math3.distribution.ChiSquaredDistribution;
import org.eclipse.collections.impl.list.mutable.primitive.DoubleArrayList;

/** This function calculates acf, and then calculates p-value of corresponding Q_LB statistics. */
public class UDTFQLB implements UDTF {

  private final DoubleArrayList valueArrayList = new DoubleArrayList();
  private int m = 0;
  private double qlb = 0.0f;

  @Override
  public void validate(UDFParameterValidator validator) throws Exception {
    validator
        .validateInputSeriesNumber(1)
        .validateInputSeriesDataType(0, Type.INT32, Type.INT64, Type.FLOAT, Type.DOUBLE)
        .validate(
            x -> (int) x >= 0,
            "Parameter $lag$ should be an positive integer, or '0' for default value.",
            validator.getParameters().getIntOrDefault("lag", 0));
  }

  @Override
  public void beforeStart(UDFParameters parameters, UDTFConfigurations configurations)
      throws Exception {
    configurations.setAccessStrategy(new RowByRowAccessStrategy()).setOutputDataType(Type.DOUBLE);
    m = parameters.getIntOrDefault("lag", 0);
    valueArrayList.clear();
  }

  @Override
  public void transform(Row row, PointCollector collector) throws Exception {
    if (row.isNull(0)) {
      valueArrayList.add(Double.NaN);
    } else {
      double v = Util.getValueAsDouble(row);
      if (Double.isFinite(v)) {
        valueArrayList.add(v);
      } else {
        valueArrayList.add(0d);
      }
    }
  }

  @Override
  public void terminate(PointCollector collector) throws Exception {
    int n = valueArrayList.size();
    if (m <= 0 || m >= n) {
      m = n - 2;
    }
    for (int shift = 1; shift <= m; shift++) {
      double correlation = 0.0;
      for (int i = 0; i < n - shift; i++) {
        if (Double.isFinite(valueArrayList.get(shift + i))
            && Double.isFinite(valueArrayList.get(i))) {
          correlation += valueArrayList.get(shift + i) * valueArrayList.get(i);
        }
      }
      correlation = correlation / n;
      collector.putDouble((long) n + shift, correlation);
      qlb += correlation * correlation / (n - shift) * n * (n + 2);
      ChiSquaredDistribution qlbdist = new ChiSquaredDistribution(shift);
      double qlbprob = 1.0 - qlbdist.cumulativeProbability(qlb);
      collector.putDouble(shift, qlbprob);
    }
  }

  @Override
  public void beforeDestroy() {
    valueArrayList.clear();
  }
}
