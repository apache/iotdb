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
package org.apache.iotdb.metrics;

import org.apache.iotdb.metrics.impl.DoNothingFactory;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.ServiceLoader;

/** MetricService is the entr */
public class MetricService {

  private static final Logger logger = LoggerFactory.getLogger(MetricService.class);

  private static final List<MetricReporter> reporters = new ArrayList<>();

  private static MetricFactory factory;

  static {
    init();
  }

  private static void init() {

    ServiceLoader<MetricFactory> metricFactories = ServiceLoader.load(MetricFactory.class);
    int size = 0;
    MetricFactory nothingFactory = null;

    for (MetricFactory mf : metricFactories) {
      if (mf instanceof DoNothingFactory) {
        nothingFactory = mf;
        continue;
      }
      size++;
      factory = mf;
    }

    // if no more implementation, we use nothingFactory.
    if (size == 0) {
      factory = nothingFactory;
    } else if (size > 1) {
      logger.warn("detect more than one MetricFactory, will use {}", factory.getClass().getName());
    }

    ServiceLoader<MetricReporter> reporter = ServiceLoader.load(MetricReporter.class);
    for (MetricReporter r : reporter) {
      reporters.add(r);
      r.setMetricFactory(factory);
      r.start();
      logger.info("detect MetricReporter {}", r.getClass().getName());
    }
  }

  public static void stop() {
    for (MetricReporter r : reporters) {
      r.stop();
    }
  }

  public static MetricManager getMetric(String namespace) {
    return factory.getMetric(namespace);
  }

  public static void enableKnownMetric(KnownMetric metric) {
    factory.enableKnownMetric(metric);
  }

  public static Map<String, MetricManager> getAllMetrics() {
    return factory.getAllMetrics();
  }

  public static boolean isEnable() {
    return factory.isEnable();
  }
}
