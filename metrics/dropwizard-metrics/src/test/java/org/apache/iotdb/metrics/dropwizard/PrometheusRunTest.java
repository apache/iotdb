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

import org.apache.iotdb.metrics.DoNothingMetricService;
import org.apache.iotdb.metrics.MetricManager;
import org.apache.iotdb.metrics.MetricService;
import org.apache.iotdb.metrics.config.MetricConfig;
import org.apache.iotdb.metrics.config.MetricConfigDescriptor;
import org.apache.iotdb.metrics.type.Counter;
import org.apache.iotdb.metrics.utils.MonitorType;

import java.util.ArrayList;
import java.util.concurrent.TimeUnit;

public class PrometheusRunTest {
  static MetricConfig metricConfig = MetricConfigDescriptor.getInstance().getMetricConfig();
  static MetricService metricService = new DoNothingMetricService();
  static MetricManager metricManager;

  public static void main(String[] args) throws InterruptedException {
    metricConfig.setMonitorType(MonitorType.dropwizard);
    metricConfig.setPredefinedMetrics(new ArrayList<>());
    metricService.startService();
    metricManager = metricService.getMetricManager();
    Counter counter = metricManager.getOrCreateCounter("counter");
    while (true) {
      counter.inc();
      TimeUnit.SECONDS.sleep(1);
    }
  }
}
