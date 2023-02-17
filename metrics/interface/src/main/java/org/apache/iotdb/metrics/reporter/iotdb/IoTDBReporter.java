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

package org.apache.iotdb.metrics.reporter.iotdb;

import org.apache.iotdb.metrics.impl.DoNothingMetric;
import org.apache.iotdb.metrics.reporter.Reporter;
import org.apache.iotdb.metrics.type.IMetric;
import org.apache.iotdb.metrics.utils.IoTDBMetricsUtils;
import org.apache.iotdb.metrics.utils.MetricInfo;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;

import java.util.HashMap;
import java.util.Map;

/** The reporter to IoTDB. */
public abstract class IoTDBReporter implements Reporter {
  /**
   * Write metric into IoTDB.
   *
   * @param metric the target metric
   * @param name the name of metric
   * @param tags the tags of metric
   */
  public void writeMetricToIoTDB(IMetric metric, String name, String... tags) {
    if (!(metric instanceof DoNothingMetric)) {
      Map<String, Object> values = new HashMap<>();
      metric.constructValueMap(values);
      writeMetricToIoTDB(
          values, IoTDBMetricsUtils.generatePath(name, tags), System.currentTimeMillis());
    }
  }

  /**
   * Write metric into IoTDB.
   *
   * @param valueMap sensor -> value
   * @param prefix device
   * @param time write time
   */
  protected abstract void writeMetricToIoTDB(
      Map<String, Object> valueMap, String prefix, long time);

  /**
   * Write metrics into IoTDB.
   *
   * @param metricMap metricInfo -> IMetric
   */
  public void writeMetricToIoTDB(Map<MetricInfo, IMetric> metricMap) {
    Map<String, Map<String, Object>> values = new HashMap<>();
    for (Map.Entry<MetricInfo, IMetric> metricEntry : metricMap.entrySet()) {
      String prefix = IoTDBMetricsUtils.generatePath(metricEntry.getKey());
      IMetric metric = metricEntry.getValue();
      if (!(metric instanceof DoNothingMetric)) {
        Map<String, Object> value = new HashMap<>();
        metric.constructValueMap(value);
        values.put(prefix, value);
      }
    }
    writeMetricsToIoTDB(values, System.currentTimeMillis());
  }

  /**
   * Write metrics into IoTDB.
   *
   * @param valueMap device -> sensor -> value
   * @param time write time
   */
  protected abstract void writeMetricsToIoTDB(Map<String, Map<String, Object>> valueMap, long time);

  /** Infer type from object. */
  protected TSDataType inferType(Object value) {
    TSDataType dataType;
    if (value instanceof Boolean) {
      dataType = TSDataType.BOOLEAN;
    } else if (value instanceof Integer) {
      dataType = TSDataType.INT32;
    } else if (value instanceof Long) {
      dataType = TSDataType.INT64;
    } else if (value instanceof Double) {
      dataType = TSDataType.DOUBLE;
    } else {
      dataType = TSDataType.TEXT;
    }
    return dataType;
  }
}
