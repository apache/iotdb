/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.iotdb.db.metrics.source;

import org.apache.iotdb.db.metrics.server.ServerArgument;
import com.codahale.metrics.Gauge;
import com.codahale.metrics.MetricRegistry;

public class MetricsSource implements Source {

  private String sourceName = "iot-metrics";
  private MetricRegistry metricRegistry;
  private ServerArgument serverArgument;

  public MetricsSource(ServerArgument serverArgument, MetricRegistry metricRegistry) {
    this.serverArgument = serverArgument;
    this.metricRegistry = metricRegistry;
  }

  public void registerInfo() {

    metricRegistry.register(MetricRegistry.name(sourceName, "host"),
        (Gauge<String>) () -> serverArgument.getHost());

    metricRegistry.register(MetricRegistry.name(sourceName, "port"),
        (Gauge<Integer>) () -> (int) serverArgument.getPort());

    metricRegistry.register(MetricRegistry.name(sourceName, "cores"),
        (Gauge<Integer>) () -> (int) serverArgument.getCores());

    metricRegistry.register(MetricRegistry.name(sourceName, "cpu_ratio"),
        (Gauge<Integer>) () -> (int) serverArgument.getCpuRatio());

    metricRegistry.register(MetricRegistry.name(sourceName, "total_memory"),
        (Gauge<Integer>) () -> (int) serverArgument.getTotalMemory());

    metricRegistry.register(MetricRegistry.name(sourceName, "max_memory"),
        (Gauge<Integer>) () -> (int) serverArgument.getMaxMemory());

    metricRegistry.register(MetricRegistry.name(sourceName, "free_memory"),
        (Gauge<Integer>) () -> (int) serverArgument.getFreeMemory());

    metricRegistry.register(MetricRegistry.name(sourceName, "totalPhysical_memory"),
        (Gauge<Integer>) () -> (int) serverArgument.getTotalPhysicalMemory());

    metricRegistry.register(MetricRegistry.name(sourceName, "freePhysical_memory"),
        (Gauge<Integer>) () -> (int) serverArgument.getFreePhysicalMemory());

    metricRegistry.register(MetricRegistry.name(sourceName, "usedPhysical_memory"),
        (Gauge<Integer>) () -> (int) serverArgument.getUsedPhysicalMemory());
  }

  @Override
  public String sourceName() {
    return this.sourceName;
  }

}
