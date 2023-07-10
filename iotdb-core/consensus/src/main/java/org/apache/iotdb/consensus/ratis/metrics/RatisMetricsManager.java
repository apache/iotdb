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

import org.apache.iotdb.common.rpc.thrift.TConsensusGroupType;
import org.apache.iotdb.commons.service.metric.MetricService;
import org.apache.iotdb.commons.service.metric.enums.Metric;
import org.apache.iotdb.commons.service.metric.enums.Tag;
import org.apache.iotdb.metrics.utils.MetricLevel;

import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;

public class RatisMetricsManager {

  static class TimeKeeper implements AutoCloseable {
    private final long startMoment;
    private final TConsensusGroupType groupType;
    private final BiConsumer<Long, TConsensusGroupType> reporter;

    private TimeKeeper(
        BiConsumer<Long, TConsensusGroupType> reporter, TConsensusGroupType groupType) {
      this.reporter = reporter;
      this.groupType = groupType;
      startMoment = System.nanoTime();
    }

    void stopAndReport() {
      final long timeElapsed = System.nanoTime() - startMoment;
      reporter.accept(timeElapsed, groupType);
    }

    @Override
    public void close() throws Exception {
      stopAndReport();
    }
  }

  private final MetricService metricService = MetricService.getInstance();

  public TimeKeeper startWriteLocallyTimer(TConsensusGroupType consensusGroupType) {
    return new TimeKeeper(this::recordWriteLocallyCost, consensusGroupType);
  }

  public TimeKeeper startWriteRemotelyTimer(TConsensusGroupType consensusGroupType) {
    return new TimeKeeper(this::recordWriteRemotelyCost, consensusGroupType);
  }

  public TimeKeeper startReadTimer(TConsensusGroupType consensusGroupType) {
    return new TimeKeeper(this::recordReadRequestCost, consensusGroupType);
  }

  /** Record the time cost in write locally stage. */
  private void recordWriteLocallyCost(
      long costTimeInNanos, TConsensusGroupType consensusGroupType) {
    metricService.timer(
        costTimeInNanos,
        TimeUnit.NANOSECONDS,
        consensusGroupType.toString() + "_" + Metric.RATIS_CONSENSUS_WRITE,
        MetricLevel.IMPORTANT,
        Tag.STAGE.toString(),
        RatisMetricSet.WRITE_LOCALLY);
  }

  /** Record the time cost in write remotely stage. */
  private void recordWriteRemotelyCost(
      long costTimeInNanos, TConsensusGroupType consensusGroupType) {
    metricService.timer(
        costTimeInNanos,
        TimeUnit.NANOSECONDS,
        consensusGroupType.toString() + "_" + Metric.RATIS_CONSENSUS_WRITE,
        MetricLevel.IMPORTANT,
        Tag.STAGE.toString(),
        RatisMetricSet.WRITE_REMOTELY);
  }

  /** Record the time cost in submit read request stage. */
  private void recordReadRequestCost(long costTimeInNanos, TConsensusGroupType consensusGroupType) {
    metricService.timer(
        costTimeInNanos,
        TimeUnit.NANOSECONDS,
        consensusGroupType.toString() + "_" + Metric.RATIS_CONSENSUS_READ,
        MetricLevel.IMPORTANT,
        Tag.STAGE.toString(),
        RatisMetricSet.SUBMIT_READ_REQUEST);
  }

  /** Record the time cost in write state machine stage. */
  public void recordWriteStateMachineCost(
      long costTimeInNanos, TConsensusGroupType consensusGroupType) {
    metricService.timer(
        costTimeInNanos,
        TimeUnit.NANOSECONDS,
        consensusGroupType.toString() + "_" + Metric.RATIS_CONSENSUS_WRITE,
        MetricLevel.IMPORTANT,
        Tag.STAGE.toString(),
        RatisMetricSet.WRITE_STATE_MACHINE);
  }

  public static RatisMetricsManager getInstance() {
    return RatisMetricsManagerHolder.INSTANCE;
  }

  private static class RatisMetricsManagerHolder {
    private static final RatisMetricsManager INSTANCE = new RatisMetricsManager();

    private RatisMetricsManagerHolder() {
      // empty constructor
    }
  }
}
