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
package org.apache.iotdb.db.metrics.source;

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.jvm.BufferPoolMetricSet;
import com.codahale.metrics.jvm.GarbageCollectorMetricSet;
import com.codahale.metrics.jvm.MemoryUsageGaugeSet;

import java.lang.management.ManagementFactory;

public class JvmSource implements Source {

  public static final String SOURCE_NAME = "jvm";
  public MetricRegistry metricRegistry;

  public JvmSource(MetricRegistry metricRegistry) {
    this.metricRegistry = metricRegistry;
  }

  public void registerInfo() {
    metricRegistry.register(
        MetricRegistry.name(SOURCE_NAME, "gc"), new GarbageCollectorMetricSet());
    metricRegistry.register(MetricRegistry.name(SOURCE_NAME, "memory"), new MemoryUsageGaugeSet());
    metricRegistry.register(
        MetricRegistry.name(SOURCE_NAME, "buffer-pool"),
        new BufferPoolMetricSet(ManagementFactory.getPlatformMBeanServer()));
  }

  @Override
  public String sourceName() {
    return JvmSource.SOURCE_NAME;
  }
}
