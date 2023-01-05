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

import org.apache.iotdb.metrics.type.AutoGauge;
import org.apache.iotdb.metrics.type.IMetric;
import org.apache.iotdb.metrics.utils.InternalReporterType;
import org.apache.iotdb.metrics.utils.MetricInfo;
import org.apache.iotdb.metrics.utils.MetricType;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public abstract class IoTDBInternalReporter extends IoTDBReporter {
  protected final Map<MetricInfo, IMetric> autoGauges = new ConcurrentHashMap<>();

  /**
   * Add autoGauge into internal reporter.
   *
   * @param autoGauge autoGauge
   * @param name the name of autoGauge
   * @param tags the tags of autoGauge
   */
  public void addAutoGauge(AutoGauge autoGauge, String name, String... tags) {
    MetricInfo metricInfo = new MetricInfo(MetricType.AUTO_GAUGE, name, tags);
    autoGauges.put(metricInfo, autoGauge);
  }

  /**
   * Add autoGauges into internal reporter.
   *
   * @param gauges the map of autoGauge
   */
  public void addAutoGauge(Map<MetricInfo, IMetric> gauges) {
    autoGauges.putAll(gauges);
  }

  /** Get all autoGauges. */
  public Map<MetricInfo, IMetric> getAllAutoGauge() {
    return autoGauges;
  }

  /** Clear all autoGauges. */
  public void clear() {
    autoGauges.clear();
  }

  /** Get the type of internal reporter. */
  public abstract InternalReporterType getType();
}
