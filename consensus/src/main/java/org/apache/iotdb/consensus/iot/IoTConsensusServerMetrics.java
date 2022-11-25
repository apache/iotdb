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

package org.apache.iotdb.consensus.iot;

import org.apache.iotdb.commons.service.metric.MetricService;
import org.apache.iotdb.commons.service.metric.enums.Metric;
import org.apache.iotdb.commons.service.metric.enums.Tag;
import org.apache.iotdb.metrics.AbstractMetricService;
import org.apache.iotdb.metrics.metricsets.IMetricSet;
import org.apache.iotdb.metrics.utils.MetricLevel;
import org.apache.iotdb.metrics.utils.MetricType;

public class IoTConsensusServerMetrics implements IMetricSet {
  private final IoTConsensusServerImpl impl;

  public IoTConsensusServerMetrics(IoTConsensusServerImpl impl) {
    this.impl = impl;
  }

  @Override
  public void bindTo(AbstractMetricService metricService) {
    MetricService.getInstance()
        .createAutoGauge(
            Metric.MULTI_LEADER.toString(),
            MetricLevel.IMPORTANT,
            impl,
            IoTConsensusServerImpl::getIndex,
            Tag.NAME.toString(),
            "multiLeaderServerImpl",
            Tag.REGION.toString(),
            impl.getThisNode().getGroupId().toString(),
            Tag.TYPE.toString(),
            "searchIndex");
    MetricService.getInstance()
        .createAutoGauge(
            Metric.MULTI_LEADER.toString(),
            MetricLevel.IMPORTANT,
            impl,
            IoTConsensusServerImpl::getCurrentSafelyDeletedSearchIndex,
            Tag.NAME.toString(),
            "multiLeaderServerImpl",
            Tag.REGION.toString(),
            impl.getThisNode().getGroupId().toString(),
            Tag.TYPE.toString(),
            "safeIndex");
  }

  @Override
  public void unbindFrom(AbstractMetricService metricService) {
    MetricService.getInstance()
        .remove(
            MetricType.GAUGE,
            Metric.MULTI_LEADER.toString(),
            Tag.NAME.toString(),
            "multiLeaderServerImpl",
            Tag.REGION.toString(),
            impl.getThisNode().getGroupId().toString(),
            Tag.TYPE.toString(),
            "searchIndex");
    MetricService.getInstance()
        .remove(
            MetricType.GAUGE,
            Metric.MULTI_LEADER.toString(),
            Tag.NAME.toString(),
            "multiLeaderServerImpl",
            Tag.REGION.toString(),
            impl.getThisNode().getGroupId().toString(),
            Tag.TYPE.toString(),
            "safeIndex");
  }
}
