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

import org.apache.iotdb.db.metrics.server.ServerArgument;

import com.codahale.metrics.Gauge;
import com.codahale.metrics.MetricRegistry;

public class MetricsSource implements Source {

  private static final String SOURCE_NAME = "iot-metrics";
  public MetricRegistry metricRegistry;
  public ServerArgument serverArgument;

  public MetricsSource(ServerArgument serverArgument, MetricRegistry metricRegistry) {
    this.serverArgument = serverArgument;
    this.metricRegistry = metricRegistry;
  }

  public void registerInfo() {

    metricRegistry.register(
        MetricRegistry.name(SOURCE_NAME, "host"),
        new Gauge<String>() {
          @Override
          public String getValue() {
            return serverArgument.getHost();
          }
        });

    metricRegistry.register(
        MetricRegistry.name(SOURCE_NAME, "port"),
        new Gauge<Integer>() {
          @Override
          public Integer getValue() {
            return (int) serverArgument.getPort();
          }
        });

    metricRegistry.register(
        MetricRegistry.name(SOURCE_NAME, "cores"),
        new Gauge<Integer>() {
          @Override
          public Integer getValue() {
            return (int) serverArgument.getCores();
          }
        });

    metricRegistry.register(
        MetricRegistry.name(SOURCE_NAME, "cpu_ratio"),
        new Gauge<Integer>() {
          @Override
          public Integer getValue() {
            return (int) serverArgument.getCpuRatio();
          }
        });

    metricRegistry.register(
        MetricRegistry.name(SOURCE_NAME, "total_memory"),
        new Gauge<Integer>() {
          @Override
          public Integer getValue() {
            return (int) serverArgument.getTotalMemory();
          }
        });

    metricRegistry.register(
        MetricRegistry.name(SOURCE_NAME, "max_memory"),
        new Gauge<Integer>() {
          @Override
          public Integer getValue() {
            return (int) serverArgument.getMaxMemory();
          }
        });

    metricRegistry.register(
        MetricRegistry.name(SOURCE_NAME, "free_memory"),
        new Gauge<Integer>() {
          @Override
          public Integer getValue() {
            return (int) serverArgument.getFreeMemory();
          }
        });

    metricRegistry.register(
        MetricRegistry.name(SOURCE_NAME, "totalPhysical_memory"),
        new Gauge<Integer>() {
          @Override
          public Integer getValue() {
            return (int) serverArgument.getTotalPhysicalMemory();
          }
        });

    metricRegistry.register(
        MetricRegistry.name(SOURCE_NAME, "freePhysical_memory"),
        new Gauge<Integer>() {
          @Override
          public Integer getValue() {
            return (int) serverArgument.getFreePhysicalMemory();
          }
        });

    metricRegistry.register(
        MetricRegistry.name(SOURCE_NAME, "usedPhysical_memory"),
        new Gauge<Integer>() {
          @Override
          public Integer getValue() {
            return (int) serverArgument.getUsedPhysicalMemory();
          }
        });
  }

  @Override
  public String sourceName() {
    return MetricsSource.SOURCE_NAME;
  }
}
