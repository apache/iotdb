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
import org.apache.iotdb.commons.service.metric.enums.Metric;
import org.apache.iotdb.commons.service.metric.enums.Tag;
import org.apache.iotdb.metrics.AbstractMetricService;
import org.apache.iotdb.metrics.metricsets.IMetricSet;
import org.apache.iotdb.metrics.utils.MetricInfo;
import org.apache.iotdb.metrics.utils.MetricLevel;
import org.apache.iotdb.metrics.utils.MetricType;

import org.apache.ratis.metrics.MetricRegistries;

import java.util.HashMap;
import java.util.Map;

public class RatisMetricSet implements IMetricSet {
  private MetricRegistries manager;
  private final String consensusGroupType;
  private static final Map<String, MetricInfo> metricInfoMap = new HashMap<>();
  private static final String RATIS_CONSENSUS_WRITE = Metric.RATIS_CONSENSUS_WRITE.toString();
  private static final String RATIS_CONSENSUS_READ = Metric.RATIS_CONSENSUS_READ.toString();

  public static final String WRITE_CHECK = "checkWriteCondition";
  public static final String READ_CHECK = "checkReadCondition";
  public static final String WRITE_LOCALLY = "writeLocally";
  public static final String WRITE_REMOTELY = "writeRemotely";
  public static final String TOTAL_WRITE_TIME = "totalConsensusWrite";
  public static final String TOTAL_READ_TIME = "totalConsensusRead";
  public static final String SUBMIT_READ_REQUEST = "submitReadRequest";
  public static final String WRITE_STATE_MACHINE = "writeStateMachine";

  static {
    metricInfoMap.put(
        WRITE_CHECK,
        new MetricInfo(MetricType.TIMER, RATIS_CONSENSUS_WRITE, Tag.STAGE.toString(), WRITE_CHECK));
    metricInfoMap.put(
        READ_CHECK,
        new MetricInfo(MetricType.TIMER, RATIS_CONSENSUS_READ, Tag.STAGE.toString(), READ_CHECK));
    metricInfoMap.put(
        WRITE_LOCALLY,
        new MetricInfo(
            MetricType.TIMER, RATIS_CONSENSUS_WRITE, Tag.STAGE.toString(), WRITE_LOCALLY));
    metricInfoMap.put(
        WRITE_REMOTELY,
        new MetricInfo(
            MetricType.TIMER, RATIS_CONSENSUS_WRITE, Tag.STAGE.toString(), WRITE_REMOTELY));
    metricInfoMap.put(
        TOTAL_WRITE_TIME,
        new MetricInfo(
            MetricType.TIMER, RATIS_CONSENSUS_WRITE, Tag.STAGE.toString(), TOTAL_WRITE_TIME));
    metricInfoMap.put(
        TOTAL_READ_TIME,
        new MetricInfo(
            MetricType.TIMER, RATIS_CONSENSUS_READ, Tag.STAGE.toString(), TOTAL_READ_TIME));
    metricInfoMap.put(
        SUBMIT_READ_REQUEST,
        new MetricInfo(
            MetricType.TIMER, RATIS_CONSENSUS_READ, Tag.STAGE.toString(), SUBMIT_READ_REQUEST));
    metricInfoMap.put(
        WRITE_STATE_MACHINE,
        new MetricInfo(
            MetricType.TIMER, RATIS_CONSENSUS_WRITE, Tag.STAGE.toString(), WRITE_STATE_MACHINE));
  }

  public RatisMetricSet(TConsensusGroupType consensusGroupType) {
    super();
    this.consensusGroupType = consensusGroupType.toString();
  }

  @Override
  public void bindTo(AbstractMetricService metricService) {
    manager = MetricRegistries.global();
    if (manager instanceof MetricRegistryManager) {
      ((MetricRegistryManager) manager).setConsensusGroupType(consensusGroupType);
    }
    for (MetricInfo metricInfo : metricInfoMap.values()) {
      metricService.getOrCreateTimer(
          metricInfo.getName(), MetricLevel.CORE, metricInfo.getTagsInArray());
    }
  }

  @Override
  public void unbindFrom(AbstractMetricService metricService) {
    manager.clear();
    for (MetricInfo metricInfo : metricInfoMap.values()) {
      metricService.remove(MetricType.TIMER, metricInfo.getName(), metricInfo.getTagsInArray());
    }
  }
}
