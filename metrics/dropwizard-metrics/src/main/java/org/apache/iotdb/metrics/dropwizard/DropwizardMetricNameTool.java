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

package org.apache.iotdb.metrics.dropwizard;

import org.apache.iotdb.metrics.utils.MetricInfo;
import org.apache.iotdb.metrics.utils.MetricType;

import java.util.Map;
import java.util.stream.Collectors;

public class DropwizardMetricNameTool {
  private static final String TAG_SEPARATOR = ".";

  /**
   * Transform flat string and metric type to metricInfo.
   *
   * @param metricType the type of metric
   * @param flatString the flat string of metricInfo
   */
  public static MetricInfo transformFromString(MetricType metricType, String flatString) {
    MetricInfo metricInfo;
    String name;
    int firstIndex = flatString.indexOf("{");
    int lastIndex = flatString.indexOf("}");
    if (firstIndex == -1 || lastIndex == -1) {
      name = flatString.replaceAll("[^a-zA-Z0-9:_\\]\\[]", "_");
      metricInfo = new MetricInfo(metricType, name);
    } else {
      name = flatString.substring(0, firstIndex).replaceAll("[^a-zA-Z0-9:_\\]\\[]", "_");
      String tagsPart = flatString.substring(firstIndex + 1, lastIndex);
      if (0 == tagsPart.length()) {
        metricInfo = new MetricInfo(metricType, name);
      } else {
        metricInfo = new MetricInfo(metricType, name, tagsPart.split("\\."));
      }
    }
    return metricInfo;
  }

  /**
   * Transform metricInfo to flat string.
   *
   * @param metricInfo the info of metric
   */
  public static String toFlatString(MetricInfo metricInfo) {
    String name = metricInfo.getName();
    Map<String, String> tags = metricInfo.getTags();
    return name.replaceAll("\\{|\\}", "")
        + "{"
        + tags.entrySet().stream()
            .map(
                t ->
                    t.getKey().replace(TAG_SEPARATOR, "")
                        + TAG_SEPARATOR
                        + t.getValue().replace(TAG_SEPARATOR, ""))
            .collect(Collectors.joining(TAG_SEPARATOR))
            .replaceAll("\\{|\\}", "")
        + "}";
  }
}
