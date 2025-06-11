/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * <p>http://www.apache.org/licenses/LICENSE-2.0
 *
 * <p>Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.iotdb.db.protocol.thrift.handler;

import org.apache.iotdb.commons.service.metric.enums.Metric;
import org.apache.iotdb.commons.service.metric.enums.Tag;
import org.apache.iotdb.metrics.AbstractMetricService;
import org.apache.iotdb.metrics.impl.DoNothingMetricManager;
import org.apache.iotdb.metrics.metricsets.IMetricSet;
import org.apache.iotdb.metrics.type.Gauge;
import org.apache.iotdb.metrics.type.Timer;
import org.apache.iotdb.metrics.utils.MetricLevel;
import org.apache.iotdb.metrics.utils.MetricType;

import java.util.Objects;
import java.util.concurrent.atomic.AtomicLong;

public class RPCServiceThriftHandlerMetrics implements IMetricSet {
  private static final RPCServiceThriftHandlerMetrics INSTANCE =
      new RPCServiceThriftHandlerMetrics(new AtomicLong(0));
  private AtomicLong thriftConnectionNumber;

  // region begin
  private Gauge unCompressionSizeTimer = DoNothingMetricManager.DO_NOTHING_GAUGE;
  private Gauge compressionSizeTimer = DoNothingMetricManager.DO_NOTHING_GAUGE;
  private Timer decodeLatencyTimer = DoNothingMetricManager.DO_NOTHING_TIMER;
  private Timer decompressLatencyTimer = DoNothingMetricManager.DO_NOTHING_TIMER;

  private Gauge memoryUsageGauge = DoNothingMetricManager.DO_NOTHING_GAUGE;

  // end
  public RPCServiceThriftHandlerMetrics(AtomicLong thriftConnectionNumber) {
    this.thriftConnectionNumber = thriftConnectionNumber;
  }

  public void recordUnCompressionSizeTimer(final long size) {
    unCompressionSizeTimer.set(size);
  }

  public void recordCompressionSizeTimer(final long size) {
    compressionSizeTimer.set(size);
  }

  public void recordDecodeLatencyTimer(final long costTimeInNanos) {
    decodeLatencyTimer.updateNanos(costTimeInNanos);
  }

  public void recordDecompressLatencyTimer(final long costTimeInNanos) {
    decompressLatencyTimer.updateNanos(costTimeInNanos);
  }

  public void recordMemoryUsage(final long memoryUsage) {
    memoryUsageGauge.set(memoryUsage);
  }

  @Override
  public void bindTo(AbstractMetricService metricService) {
    metricService.createAutoGauge(
        Metric.THRIFT_CONNECTIONS.toString(),
        MetricLevel.CORE,
        thriftConnectionNumber,
        AtomicLong::get,
        Tag.NAME.toString(),
        "ClientRPC");

    unCompressionSizeTimer =
        metricService.getOrCreateGauge(
            Metric.THRIFT_RPC_UNCOMPRESS_SIZE.toString(),
            MetricLevel.IMPORTANT,
            Tag.NAME.toString(),
            "unCompressionSize");

    compressionSizeTimer =
        metricService.getOrCreateGauge(
            Metric.THRIFT_RPC_COMPRESS_SIZE.toString(),
            MetricLevel.IMPORTANT,
            Tag.NAME.toString(),
            "compressionSize");

    decodeLatencyTimer =
        metricService.getOrCreateTimer(
            Metric.THRIFT_RPC_DECODE.toString(),
            MetricLevel.IMPORTANT,
            Tag.NAME.toString(),
            "decodeLatency");

    decompressLatencyTimer =
        metricService.getOrCreateTimer(
            Metric.THRIFT_RPC_UNCOMPRESS.toString(),
            MetricLevel.IMPORTANT,
            Tag.NAME.toString(),
            "decompressLatency");

    memoryUsageGauge =
        metricService.getOrCreateGauge(
            Metric.THRIFT_RPC_MEMORY_USAGE.toString(),
            MetricLevel.IMPORTANT,
            Tag.NAME.toString(),
            "memoryUsage");
  }

  @Override
  public void unbindFrom(AbstractMetricService metricService) {
    metricService.remove(
        MetricType.AUTO_GAUGE,
        Metric.THRIFT_CONNECTIONS.toString(),
        Tag.NAME.toString(),
        "ClientRPC");

    metricService.remove(
        MetricType.TIMER,
        Metric.THRIFT_RPC_UNCOMPRESS_SIZE.toString(),
        Tag.NAME.toString(),
        "unCompressionSize");

    metricService.remove(
        MetricType.TIMER,
        Metric.THRIFT_RPC_COMPRESS_SIZE.toString(),
        Tag.NAME.toString(),
        "compressionSize");

    metricService.remove(
        MetricType.TIMER,
        Metric.THRIFT_RPC_DECODE.toString(),
        Tag.NAME.toString(),
        "decodeLatency");

    metricService.remove(
        MetricType.TIMER,
        Metric.THRIFT_RPC_UNCOMPRESS.toString(),
        Tag.NAME.toString(),
        "decompressLatency");

    metricService.remove(
        MetricType.GAUGE,
        Metric.THRIFT_RPC_MEMORY_USAGE.toString(),
        Tag.NAME.toString(),
        "memoryUsage");
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    RPCServiceThriftHandlerMetrics that = (RPCServiceThriftHandlerMetrics) o;
    return Objects.equals(thriftConnectionNumber, that.thriftConnectionNumber);
  }

  @Override
  public int hashCode() {
    return Objects.hash(thriftConnectionNumber);
  }

  public static RPCServiceThriftHandlerMetrics getInstance() {
    return INSTANCE;
  }
}
