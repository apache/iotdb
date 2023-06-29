/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iotdb.metrics.utils;

import org.apache.iotdb.metrics.config.MetricConfig;
import org.apache.iotdb.metrics.config.MetricConfigDescriptor;

import java.util.Map;

import static org.apache.iotdb.tsfile.common.constant.TsFileConstant.BACK_QUOTE_STRING;
import static org.apache.iotdb.tsfile.common.constant.TsFileConstant.DOUBLE_BACK_QUOTE_STRING;

public class IoTDBMetricsUtils {
  private static final MetricConfig METRIC_CONFIG =
      MetricConfigDescriptor.getInstance().getMetricConfig();

  /** Generate the path of metric by metricInfo. */
  public static String generatePath(MetricInfo metricInfo) {
    return generatePath(metricInfo.getName(), metricInfo.getTags());
  }

  /** Generate the path of metric with tags array. */
  public static String generatePath(String name, String... tags) {
    StringBuilder stringBuilder = generateMetric(name);
    for (int i = 0; i < tags.length; i += 2) {
      stringBuilder
          .append(".")
          .append("`")
          .append(tags[i].replace(BACK_QUOTE_STRING, DOUBLE_BACK_QUOTE_STRING))
          .append("=")
          .append(tags[i + 1].replace(BACK_QUOTE_STRING, DOUBLE_BACK_QUOTE_STRING))
          .append("`");
    }
    return stringBuilder.toString();
  }

  /** Generate the path of metric with tags map. */
  public static String generatePath(String name, Map<String, String> tags) {
    StringBuilder stringBuilder = generateMetric(name);
    for (Map.Entry<String, String> entry : tags.entrySet()) {
      stringBuilder
          .append(".")
          .append("`")
          .append(entry.getKey().replace(BACK_QUOTE_STRING, DOUBLE_BACK_QUOTE_STRING))
          .append("=")
          .append(entry.getValue().replace(BACK_QUOTE_STRING, DOUBLE_BACK_QUOTE_STRING))
          .append("`");
    }
    return stringBuilder.toString();
  }

  /** Generate the path of metric. */
  private static StringBuilder generateMetric(String name) {
    StringBuilder stringBuilder = new StringBuilder();
    stringBuilder
        .append(METRIC_CONFIG.getInternalDatabase())
        .append(".")
        .append(METRIC_CONFIG.getIoTDBReporterConfig().getLocation())
        .append(".`")
        .append(METRIC_CONFIG.getClusterName())
        .append("`.")
        .append(METRIC_CONFIG.getNodeType().toString())
        .append(".`")
        .append(METRIC_CONFIG.getNodeId())
        .append("`")
        .append(".")
        .append("`")
        .append(name.replace(BACK_QUOTE_STRING, DOUBLE_BACK_QUOTE_STRING))
        .append("`");
    return stringBuilder;
  }
}
