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

package org.apache.iotdb.metrics.micrometer;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.binder.jvm.ClassLoaderMetrics;
import io.micrometer.core.instrument.binder.jvm.JvmCompilationMetrics;
import io.micrometer.core.instrument.binder.jvm.JvmGcMetrics;
import io.micrometer.core.instrument.binder.jvm.JvmHeapPressureMetrics;
import io.micrometer.core.instrument.binder.jvm.JvmMemoryMetrics;
import io.micrometer.core.instrument.binder.jvm.JvmThreadMetrics;
import org.apache.iotdb.metrics.KnownMetric;
import org.apache.iotdb.metrics.MetricFactory;
import org.apache.iotdb.metrics.MetricManager;

import java.util.Collections;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class MicrometerMetricFactory implements MetricFactory {
  boolean isEnable;
  Map<String, MetricManager> currentMetricManagers = new ConcurrentHashMap<String, MetricManager>();

  @Override
  public MetricManager getMetric(String namespace) {
    if (!isEnable) {
      return null;
    }
    currentMetricManagers.putIfAbsent(namespace, new MicrometerMetricManager());
    return currentMetricManagers.get(namespace);
  }

  @Override
  public void enableKnownMetric(KnownMetric metric) {
    if (!isEnable) {
      return;
    }
    switch (metric) {
      case JVM:
        enableJVMMetrics();
        break;
      case SYSTEM:
        break;
      case THREAD:
        break;
      default:
        // ignore;
    }
  }

  private void enableJVMMetrics() {
    MeterRegistry meterRegistry = (MeterRegistry) currentMetricManagers.get("iotdb");
    ClassLoaderMetrics classLoaderMetrics = new ClassLoaderMetrics();
    classLoaderMetrics.bindTo(meterRegistry);
    JvmCompilationMetrics jvmCompilationMetrics = new JvmCompilationMetrics();
    jvmCompilationMetrics.bindTo(meterRegistry);
    try (JvmGcMetrics jvmGcMetrics = new JvmGcMetrics();
         JvmHeapPressureMetrics jvmHeapPressureMetrics = new JvmHeapPressureMetrics()) {
      jvmGcMetrics.bindTo(meterRegistry);
      jvmHeapPressureMetrics.bindTo(meterRegistry);
    }
    JvmMemoryMetrics jvmMemoryMetrics = new JvmMemoryMetrics();
    jvmMemoryMetrics.bindTo(meterRegistry);
    JvmThreadMetrics jvmThreadMetrics = new JvmThreadMetrics();
    jvmThreadMetrics.bindTo(meterRegistry);
  }

  @Override
  public Map<String, MetricManager> getAllMetrics() {
    if (!isEnable) {
      return Collections.emptyMap();
    }
    return currentMetricManagers;
  }

  @Override
  public boolean isEnable() {
    return isEnable;
  }
}
