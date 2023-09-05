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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class RatisMetricSet implements IMetricSet {
  private MetricRegistries manager;
  private static final Map<String, MetricInfo> metricInfoMap = new HashMap<>();
  private static final String RATIS_CONSENSUS_WRITE = Metric.RATIS_CONSENSUS_WRITE.toString();
  private static final String RATIS_CONSENSUS_READ = Metric.RATIS_CONSENSUS_READ.toString();

  private static final String DATA_REGION_RATIS_CONSENSUS_WRITE =
      TConsensusGroupType.DataRegion + "_" + RATIS_CONSENSUS_WRITE;
  private static final String DATA_REGION_RATIS_CONSENSUS_READ =
      TConsensusGroupType.DataRegion + "_" + RATIS_CONSENSUS_READ;
  private static final String SCHEMA_REGION_RATIS_CONSENSUS_WRITE =
      TConsensusGroupType.SchemaRegion + "_" + RATIS_CONSENSUS_WRITE;
  private static final String SCHEMA_REGION_RATIS_CONSENSUS_READ =
      TConsensusGroupType.SchemaRegion + "_" + RATIS_CONSENSUS_READ;
  private static final String CONFIG_REGION_RATIS_CONSENSUS_WRITE =
      TConsensusGroupType.ConfigRegion + "_" + RATIS_CONSENSUS_WRITE;
  private static final String CONFIG_REGION_RATIS_CONSENSUS_READ =
      TConsensusGroupType.ConfigRegion + "_" + RATIS_CONSENSUS_READ;

  public static final String WRITE_LOCALLY = "writeLocally";
  public static final String WRITE_REMOTELY = "writeRemotely";
  public static final String SUBMIT_READ_REQUEST = "submitReadRequest";
  public static final String WRITE_STATE_MACHINE = "writeStateMachine";
  private static final List<String> RATIS_WRITE_METRICS = new ArrayList<>();
  private static final List<String> RATIS_WRITE_STAGES = new ArrayList<>();
  private static final List<String> RATIS_READ_METRICS = new ArrayList<>();
  private static final List<String> RATIS_READ_STAGES = new ArrayList<>();

  static {
    RATIS_WRITE_METRICS.add(DATA_REGION_RATIS_CONSENSUS_WRITE);
    RATIS_WRITE_METRICS.add(SCHEMA_REGION_RATIS_CONSENSUS_WRITE);
    RATIS_WRITE_METRICS.add(CONFIG_REGION_RATIS_CONSENSUS_WRITE);

    RATIS_READ_METRICS.add(DATA_REGION_RATIS_CONSENSUS_READ);
    RATIS_READ_METRICS.add(SCHEMA_REGION_RATIS_CONSENSUS_READ);
    RATIS_READ_METRICS.add(CONFIG_REGION_RATIS_CONSENSUS_READ);

    RATIS_WRITE_STAGES.add(WRITE_LOCALLY);
    RATIS_WRITE_STAGES.add(WRITE_REMOTELY);
    RATIS_WRITE_STAGES.add(WRITE_STATE_MACHINE);

    RATIS_READ_STAGES.add(SUBMIT_READ_REQUEST);

    for (String ratisWriteMetric : RATIS_WRITE_METRICS) {
      for (String ratisWriteStage : RATIS_WRITE_STAGES) {
        metricInfoMap.put(
            ratisWriteStage,
            new MetricInfo(
                MetricType.TIMER, ratisWriteMetric, Tag.STAGE.toString(), ratisWriteStage));
      }
    }

    for (String ratisReadMetric : RATIS_READ_METRICS) {
      for (String ratisReadStage : RATIS_READ_STAGES) {
        metricInfoMap.put(
            ratisReadStage,
            new MetricInfo(
                MetricType.TIMER, ratisReadMetric, Tag.STAGE.toString(), ratisReadStage));
      }
    }
  }

  @Override
  public void bindTo(AbstractMetricService metricService) {
    manager = MetricRegistries.global();
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
