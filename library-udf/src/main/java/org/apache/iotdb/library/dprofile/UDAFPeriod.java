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
import org.apache.iotdb.udf.api.access.RowIterator;
import org.apache.iotdb.udf.api.access.RowWindow;
import org.apache.iotdb.udf.api.collector.PointCollector;
import org.apache.iotdb.udf.api.customizer.config.UDTFConfigurations;
import org.apache.iotdb.udf.api.customizer.parameter.UDFParameterValidator;
import org.apache.iotdb.udf.api.customizer.parameter.UDFParameters;
import org.apache.iotdb.udf.api.customizer.strategy.SlidingSizeWindowAccessStrategy;
import org.apache.iotdb.udf.api.type.Type;

import org.apache.commons.math3.util.Pair;
import org.eclipse.collections.impl.list.mutable.primitive.DoubleArrayList;
import org.eclipse.collections.impl.list.mutable.primitive.IntArrayList;

/** The function is used to compute the period of a numeric time series. */
public class UDAFPeriod implements UDTF {
  @Override
  public void validate(UDFParameterValidator validator) throws Exception {
    validator
        .validateInputSeriesNumber(1)
        .validateInputSeriesDataType(0, Type.FLOAT, Type.DOUBLE, Type.INT32, Type.INT64);
  }

  @Override
  public void beforeStart(UDFParameters parameters, UDTFConfigurations configurations)
      throws Exception {
    configurations
        .setAccessStrategy(new SlidingSizeWindowAccessStrategy(Integer.MAX_VALUE))
        .setOutputDataType(Type.INT32);
  }

  @Override
  public void transform(RowWindow rowWindow, PointCollector collector) throws Exception {
    DoubleArrayList value = new DoubleArrayList();
    RowIterator iterator = rowWindow.getRowIterator();
    while (iterator.hasNextRow()) {
      Row row = iterator.next();
      double v = Util.getValueAsDouble(row);
      if (Double.isFinite(v)) {
        value.add(v);
      } else {
        value.add(value.getLast());
      }
    }
    double[] corr = autoCorrelation(value.toArray());
    int[] peeks = findPeeks(corr).toArray();
    int period = 0;
    if (peeks.length > 1) {
      int[] gap = Util.variation(peeks);
      period = (int) new IntArrayList(gap).median();
    }
    collector.putInt(0, period);
  }

  private IntArrayList findPeeks(double[] x) {
    int window = 100;
    IntArrayList peeks = new IntArrayList();
    peeks.add(0);
    double threshold = 0.5;
    for (int i = 1; i < Math.min(x.length - 1, window); i++) {
      if (x[i] > x[i - 1] && x[i] > x[i + 1] && x[i] > threshold) {
        window = i;
        break;
      }
    }
    for (int i = 0; i + window <= x.length; i++) {
      double v = x[i + window / 2];
      if (v > threshold) {
        Pair<Double, Integer> p = max(x, i, i + window);
        if (p.getSecond() == i + window / 2) {
          peeks.add(p.getSecond());
        }
      }
    }
    return peeks;
  }

  private Pair<Double, Integer> max(double[] x, int startIndex, int endIndex) {
    double maxValue = -Double.MAX_VALUE;
    int maxIndex = -1;
    for (int i = startIndex; i < endIndex; i++) {
      if (x[i] > maxValue) {
        maxValue = x[i];
        maxIndex = i;
      }
    }
    return Pair.create(x[maxIndex], maxIndex);
  }

  private double[] autoCorrelation(double[] x) {
    double[] corr = new double[x.length];
    for (int i = 0; i < x.length; i++) {
      corr[i] = pearson(x, x.length - i);
    }
    return corr;
  }

  /** auto correlation (pearson of subseries) */
  private double pearson(double[] x, int subLength) {
    double sum_x = 0;
    double sum_y = 0;
    double sum_xx = 0;
    double sum_yy = 0;
    double sum_xy = 0;
    int s1 = 0;
    int s2 = x.length - subLength;
    for (int i = 0; i < subLength; i++) {
      sum_x += x[s1 + i];
      sum_y += x[s2 + i];
      sum_xx += x[s1 + i] * x[s1 + i];
      sum_yy += x[s2 + i] * x[s2 + i];
      sum_xy += x[s1 + i] * x[s2 + i];
    }
    return (subLength * sum_xy - sum_x * sum_y)
        / Math.sqrt(subLength * sum_xx - sum_x * sum_x)
        / Math.sqrt(subLength * sum_yy - sum_y * sum_y);
  }
}
