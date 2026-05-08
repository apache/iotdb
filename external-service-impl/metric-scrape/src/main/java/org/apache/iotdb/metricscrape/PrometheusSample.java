/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iotdb.metricscrape;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

public class PrometheusSample {

  private final String metricFamilyName;
  private final String metricName;
  private final Map<String, String> labels;
  private final double value;
  private final long timestamp;

  public PrometheusSample(
      String metricName, Map<String, String> labels, double value, long timestamp) {
    this(metricName, metricName, labels, value, timestamp);
  }

  public PrometheusSample(
      String metricFamilyName,
      String metricName,
      Map<String, String> labels,
      double value,
      long timestamp) {
    this.metricFamilyName = metricFamilyName;
    this.metricName = metricName;
    this.labels = Collections.unmodifiableMap(new LinkedHashMap<>(labels));
    this.value = value;
    this.timestamp = timestamp;
  }

  public String getMetricFamilyName() {
    return metricFamilyName;
  }

  public String getMetricName() {
    return metricName;
  }

  public Map<String, String> getLabels() {
    return labels;
  }

  public double getValue() {
    return value;
  }

  public long getTimestamp() {
    return timestamp;
  }
}
