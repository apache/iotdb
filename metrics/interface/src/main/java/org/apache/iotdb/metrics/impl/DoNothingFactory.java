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
package org.apache.iotdb.metrics.impl;

import org.apache.iotdb.metrics.KnownMetric;
import org.apache.iotdb.metrics.MetricFactory;
import org.apache.iotdb.metrics.MetricManager;

import java.util.Collections;
import java.util.Map;

public class DoNothingFactory implements MetricFactory {
  private DoNothingMetricManager metric = new DoNothingMetricManager();

  @Override
  public MetricManager getMetric(String namespace) {
    return metric;
  }

  @Override
  public void enableKnownMetric(KnownMetric metric) {}

  @Override
  public Map<String, MetricManager> getAllMetrics() {
    return Collections.emptyMap();
  }

  @Override
  public boolean isEnable() {
    return true;
  }
}
