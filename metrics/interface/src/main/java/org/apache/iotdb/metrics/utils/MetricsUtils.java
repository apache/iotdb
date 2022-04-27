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

public class MetricsUtils {
  private static final MetricConfig metricConfig =
      MetricConfigDescriptor.getInstance().getMetricConfig();

  public static String generatePath(String name, Map<String, String> labels) {
    StringBuilder stringBuilder = new StringBuilder();
    stringBuilder
        .append("root.")
        .append(metricConfig.getIoTDBReporterConfig().getDatabase())
        .append(".\"")
        .append(metricConfig.getInstanceHost())
        .append(":")
        .append(metricConfig.getInstancePort())
        .append("\"")
        .append(".")
        .append("\"")
        .append(name)
        .append("\"");
    for (Map.Entry<String, String> entry : labels.entrySet()) {
      stringBuilder
          .append(".")
          .append("\"")
          .append(entry.getKey())
          .append("=")
          .append(entry.getValue())
          .append("\"");
    }
    return stringBuilder.toString();
  }
}
