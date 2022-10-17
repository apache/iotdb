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

package org.apache.iotdb.db.service.metrics;

import org.apache.iotdb.commons.service.metric.MetricService;
import org.apache.iotdb.metrics.config.MetricConfigDescriptor;
import org.apache.iotdb.metrics.metricsets.jvm.JvmMetrics;
import org.apache.iotdb.metrics.metricsets.logback.LogbackMetrics;

public class DataNodeMetricsHelper {
  /** Bind predefined metric sets into DataNode */
  public static void bind() {
    if (MetricConfigDescriptor.getInstance().getMetricConfig().getEnableMetric()) {
      // bind predefined metric sets
      MetricService.getInstance().addMetricSet(new JvmMetrics());
      MetricService.getInstance().addMetricSet(new LogbackMetrics());
      MetricService.getInstance().addMetricSet(new FileMetrics());
      MetricService.getInstance().addMetricSet(new ProcessMetrics());
      MetricService.getInstance().addMetricSet(new SystemMetrics());
    }
  }
}
