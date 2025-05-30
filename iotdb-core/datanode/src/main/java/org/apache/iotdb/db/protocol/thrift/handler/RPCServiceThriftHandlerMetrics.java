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
import org.apache.iotdb.metrics.type.Timer;
import org.apache.iotdb.metrics.utils.MetricLevel;
import org.apache.iotdb.metrics.utils.MetricType;

import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

public class RPCServiceThriftHandlerMetrics implements IMetricSet {
  private static final RPCServiceThriftHandlerMetrics INSTANCE =
      new RPCServiceThriftHandlerMetrics(new AtomicLong(0));
  private AtomicLong thriftConnectionNumber;

  // region begin
  private Timer compressionRatioTimer = DoNothingMetricManager.DO_NOTHING_TIMER;
  private Timer timeConsumedOfRPCTimer = DoNothingMetricManager.DO_NOTHING_TIMER;
  private Timer encodeLatencyTimer = DoNothingMetricManager.DO_NOTHING_TIMER;
  private Timer decodeLatencyTimer = DoNothingMetricManager.DO_NOTHING_TIMER;
  private Timer compressLatencyTimer = DoNothingMetricManager.DO_NOTHING_TIMER;
  private Timer decompressLatencyTimer = DoNothingMetricManager.DO_NOTHING_TIMER;

  // end

  public RPCServiceThriftHandlerMetrics(AtomicLong thriftConnectionNumber) {
    this.thriftConnectionNumber = thriftConnectionNumber;
  }

  public void recordCompressionRatioTimer(final long costTimeInNanos) {
    compressionRatioTimer.update(costTimeInNanos, TimeUnit.NANOSECONDS);
  }

  public void recordTimeConsumedOfRPC(final long costTimeInNanos) {
    timeConsumedOfRPCTimer.update(costTimeInNanos, TimeUnit.NANOSECONDS);
  }

  public void recordEncodeLatencyTimer(final long costTimeInNanos) {
    encodeLatencyTimer.update(costTimeInNanos, TimeUnit.NANOSECONDS);
  }

  public void recordDecodeLatencyTimer(final long costTimeInNanos) {
    decodeLatencyTimer.update(costTimeInNanos, TimeUnit.NANOSECONDS);
  }

  public void recordCompressLatencyTimer(final long costTimeInNanos) {
    compressLatencyTimer.update(costTimeInNanos, TimeUnit.NANOSECONDS);
  }

  public void recordDecompressLatencyTimer(final long costTimeInNanos) {
    decompressLatencyTimer.update(costTimeInNanos, TimeUnit.NANOSECONDS);
  }

  @Override
  public void bindTo(AbstractMetricService metricService) {
    bindToTimer(metricService);

    metricService.createAutoGauge(
        Metric.THRIFT_CONNECTIONS.toString(),
        MetricLevel.CORE,
        thriftConnectionNumber,
        AtomicLong::get,
        Tag.NAME.toString(),
        "ClientRPC");
  }

  private void bindToTimer(final AbstractMetricService metricService) {
    compressionRatioTimer =
        metricService.getOrCreateTimer(
            Metric.THRIFT_RPC_COMPRESS.toString(),
            MetricLevel.IMPORTANT,
            Tag.NAME.toString(),
            "compressionRatio");

    timeConsumedOfRPCTimer =
        metricService.getOrCreateTimer(
            Metric.THRIFT_RPC_COMPRESS.toString(),
            MetricLevel.IMPORTANT,
            Tag.NAME.toString(),
            "timeConsumedOfRPC");

    encodeLatencyTimer =
        metricService.getOrCreateTimer(
            Metric.THRIFT_RPC_COMPRESS.toString(),
            MetricLevel.IMPORTANT,
            Tag.NAME.toString(),
            "encodeLatency");

    decodeLatencyTimer =
        metricService.getOrCreateTimer(
            Metric.THRIFT_RPC_COMPRESS.toString(),
            MetricLevel.IMPORTANT,
            Tag.NAME.toString(),
            "decodeLatency");

    compressLatencyTimer =
        metricService.getOrCreateTimer(
            Metric.THRIFT_RPC_COMPRESS.toString(),
            MetricLevel.IMPORTANT,
            Tag.NAME.toString(),
            "compressLatency");

    decompressLatencyTimer =
        metricService.getOrCreateTimer(
            Metric.THRIFT_RPC_COMPRESS.toString(),
            MetricLevel.IMPORTANT,
            Tag.NAME.toString(),
            "decompressLatency");
  }

  @Override
  public void unbindFrom(AbstractMetricService metricService) {
    metricService.remove(
        MetricType.AUTO_GAUGE,
        Metric.THRIFT_CONNECTIONS.toString(),
        Tag.NAME.toString(),
        "ClientRPC");

    compressionRatioTimer = DoNothingMetricManager.DO_NOTHING_TIMER;
    timeConsumedOfRPCTimer = DoNothingMetricManager.DO_NOTHING_TIMER;
    encodeLatencyTimer = DoNothingMetricManager.DO_NOTHING_TIMER;
    decodeLatencyTimer = DoNothingMetricManager.DO_NOTHING_TIMER;
    compressLatencyTimer = DoNothingMetricManager.DO_NOTHING_TIMER;
    decompressLatencyTimer = DoNothingMetricManager.DO_NOTHING_TIMER;

    metricService.remove(
        MetricType.TIMER,
        Metric.THRIFT_RPC_COMPRESS.toString(),
        Tag.NAME.toString(),
        "compressionRatio");

    metricService.remove(
        MetricType.TIMER,
        Metric.THRIFT_RPC_COMPRESS.toString(),
        Tag.NAME.toString(),
        "timeConsumedOfRPC");

    metricService.remove(
        MetricType.TIMER,
        Metric.THRIFT_RPC_COMPRESS.toString(),
        Tag.NAME.toString(),
        "encodeLatency");

    metricService.remove(
        MetricType.TIMER,
        Metric.THRIFT_RPC_COMPRESS.toString(),
        Tag.NAME.toString(),
        "decodeLatency");

    metricService.remove(
        MetricType.TIMER,
        Metric.THRIFT_RPC_COMPRESS.toString(),
        Tag.NAME.toString(),
        "compressLatency");

    metricService.remove(
        MetricType.TIMER,
        Metric.THRIFT_RPC_COMPRESS.toString(),
        Tag.NAME.toString(),
        "decompressLatency");
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
