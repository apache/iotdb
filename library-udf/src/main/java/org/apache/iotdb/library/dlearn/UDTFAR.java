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

package org.apache.iotdb.library.dlearn;

import org.apache.iotdb.library.util.Util;
import org.apache.iotdb.udf.api.UDTF;
import org.apache.iotdb.udf.api.access.Row;
import org.apache.iotdb.udf.api.collector.PointCollector;
import org.apache.iotdb.udf.api.customizer.config.UDTFConfigurations;
import org.apache.iotdb.udf.api.customizer.parameter.UDFParameters;
import org.apache.iotdb.udf.api.customizer.strategy.RowByRowAccessStrategy;
import org.apache.iotdb.udf.api.exception.UDFException;
import org.apache.iotdb.udf.api.type.Type;

import java.util.ArrayList;
import java.util.List;

public class UDTFAR implements UDTF {
  private int p;
  private long interval;
  private List<Long> timeWindow = new ArrayList<>();
  private List<Double> valueWindow = new ArrayList<>();

  @Override
  public void beforeStart(UDFParameters udfParameters, UDTFConfigurations udtfConfigurations)
      throws Exception {
    udtfConfigurations
        .setAccessStrategy(new RowByRowAccessStrategy())
        .setOutputDataType(udfParameters.getDataType(0));
    this.p = udfParameters.getIntOrDefault("p", 1);
    udtfConfigurations.setAccessStrategy(new RowByRowAccessStrategy());
    udtfConfigurations.setOutputDataType(Type.DOUBLE);
  }

  @Override
  public void transform(Row row, PointCollector collector) throws Exception {
    if (!row.isNull(0)) {
      timeWindow.add(row.getTime());
      valueWindow.add(Util.getValueAsDouble(row));
    }
  }

  @Override
  public void terminate(PointCollector collector) throws Exception {
    int length = timeWindow.size();
    if (length <= this.p) {
      throw new UDFException("Illegal input.");
    }

    int count = 0;
    long maxFreqInterval = timeWindow.get(1) - timeWindow.get(0);
    for (int i = 2; i < timeWindow.size(); i++) {
      if (maxFreqInterval == timeWindow.get(i) - timeWindow.get(i - 1)) count++;
      else {
        count--;
        if (count == 0) {
          maxFreqInterval = timeWindow.get(i) - timeWindow.get(i - 1);
          count++;
        }
      }
    }
    this.interval = maxFreqInterval;

    List<Long> imputedTimeWindow = new ArrayList<>();
    List<Double> imputedValueWindow = new ArrayList<>();

    imputedTimeWindow.add(timeWindow.get(0));
    imputedValueWindow.add(valueWindow.get(0));
    for (int i = 1; i < timeWindow.size(); i++) {
      if (timeWindow.get(i) - timeWindow.get(i - 1) > interval) {
        int gap = (int) ((timeWindow.get(i) - timeWindow.get(i - 1)) / interval);
        double step = (valueWindow.get(i) - valueWindow.get(i - 1)) / gap;
        for (int j = 1; j < gap; j++) {
          imputedTimeWindow.add(timeWindow.get(i - 1) + j * interval);
          imputedValueWindow.add(valueWindow.get(i - 1) + j * step);
        }
      }
      imputedTimeWindow.add(timeWindow.get(i));
      imputedValueWindow.add(valueWindow.get(i));
    }

    int newLength = imputedTimeWindow.size();

    double[] resultCovariances = new double[this.p + 1];
    for (int i = 0; i <= p; i++) {
      resultCovariances[i] = 0;
      for (int j = 0; j < newLength - i; j++) {
        if (j + i < newLength)
          resultCovariances[i] += imputedValueWindow.get(j) * imputedValueWindow.get(j + i);
      }
      resultCovariances[i] /= newLength - i;
    }

    double[] epsilons = new double[p + 1];
    double[] kappas = new double[p + 1];
    double[][] alphas = new double[p + 1][p + 1];
    // alphas[i][j] denotes alpha_i^{(j)}
    epsilons[0] = resultCovariances[0];
    for (int i = 1; i <= p; i++) {
      double tmpSum = 0.0;
      for (int j = 1; j <= i - 1; j++) tmpSum += alphas[j][i - 1] * resultCovariances[i - j];
      kappas[i] = (resultCovariances[i] - tmpSum) / epsilons[i - 1];
      alphas[i][i] = kappas[i];
      if (i > 1) {
        for (int j = 1; j <= i - 1; j++)
          alphas[j][i] = alphas[j][i - 1] - kappas[i] * alphas[i - j][i - 1];
      }
      epsilons[i] = (1 - kappas[i] * kappas[i]) * epsilons[i - 1];
    }
    for (int i = 1; i <= p; i++) {
      collector.putDouble(i, alphas[i][p]);
    }
  }
}
