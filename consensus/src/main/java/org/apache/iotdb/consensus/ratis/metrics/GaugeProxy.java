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
package org.apache.iotdb.consensus.ratis.metrics;

import com.codahale.metrics.Gauge;
import com.codahale.metrics.MetricRegistry;

/** AutoGauge supplier holder class */
public class GaugeProxy implements Gauge {

  private final Gauge gauge;

  public GaugeProxy(MetricRegistry.MetricSupplier<Gauge> metricSupplier) {
    this.gauge = metricSupplier.newMetric();
  }

  @Override
  public Object getValue() {
    return gauge.getValue();
  }

  double getValueAsDouble() {
    Object value = getValue();
    if (value instanceof Number) {
      return ((Number) value).doubleValue();
    }
    return 0.0;
  }
}
