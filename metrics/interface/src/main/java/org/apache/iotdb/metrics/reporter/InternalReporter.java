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

package org.apache.iotdb.metrics.reporter;

import org.apache.iotdb.metrics.type.AutoGauge;
import org.apache.iotdb.metrics.type.HistogramSnapshot;
import org.apache.iotdb.metrics.utils.InternalReporterType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.utils.Pair;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public abstract class InternalReporter {
  protected final Map<Pair<String, String[]>, AutoGauge> autoGauges = new ConcurrentHashMap<>();

  /**
   * Add autoGauge into internal reporter
   *
   * @param gauge autoGauge
   * @param name the name of autoGauge
   * @param tags the tags of autoGauge
   */
  public void addAutoGauge(AutoGauge autoGauge, String name, String... tags) {
    autoGauges.put(new Pair<>(name, tags), autoGauge);
  }

  /**
   * Add autoGauges into internal reporter
   *
   * @param gauges the map of autoGauge
   */
  public void addAutoGauge(Map<Pair<String, String[]>, AutoGauge> gauges) {
    autoGauges.putAll(gauges);
  }

  /** Get all autoGauges */
  public Map<Pair<String, String[]>, AutoGauge> getAllAutoGauge() {
    return autoGauges;
  }

  /** Clear all autoGauges */
  public void clear() {
    autoGauges.clear();
  }

  /**
   * Update value of metric without specific time
   *
   * @param name the name of metric
   * @param value the value of metric
   * @param type the type of value
   * @param tags the tags of metric
   */
  public abstract void updateValue(String name, Object value, TSDataType type, String... tags);

  /**
   * Update value of metric with specific time
   *
   * @param name the name of metric
   * @param value the value of metric
   * @param type the type of value
   * @param time the time of value
   * @param tags the tags of metric
   */
  public abstract void updateValue(
      String name, Object value, TSDataType type, Long time, String... tags);

  /**
   * Update the value of HistogramSnapshot
   *
   * @param name the name of metric
   * @param snapshot the snapshot of metric
   * @param tags the tags of metric
   */
  public abstract void writeSnapshotAndCount(
      String name, HistogramSnapshot snapshot, String... tags);

  /** Get the type of internal reporter */
  public abstract InternalReporterType getType();

  /** Start internal reporter */
  public abstract void start();

  /** Stop internal reporter */
  public abstract void stop();
}
