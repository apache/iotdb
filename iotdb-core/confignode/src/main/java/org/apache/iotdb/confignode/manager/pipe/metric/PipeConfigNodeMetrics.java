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

package org.apache.iotdb.confignode.manager.pipe.metric;

import org.apache.iotdb.confignode.manager.pipe.coordinator.PipeManager;
import org.apache.iotdb.confignode.manager.pipe.metric.overview.PipeConfigNodeRemainingTimeMetrics;
import org.apache.iotdb.confignode.manager.pipe.metric.overview.PipeConfigNodeResourceMetrics;
import org.apache.iotdb.confignode.manager.pipe.metric.overview.PipeProcedureMetrics;
import org.apache.iotdb.confignode.manager.pipe.metric.overview.PipeTaskInfoMetrics;
import org.apache.iotdb.confignode.manager.pipe.metric.overview.PipeTemporaryMetaInCoordinatorMetrics;
import org.apache.iotdb.confignode.manager.pipe.metric.receiver.PipeConfigNodeReceiverMetrics;
import org.apache.iotdb.confignode.manager.pipe.metric.sink.PipeConfigRegionSinkMetrics;
import org.apache.iotdb.confignode.manager.pipe.metric.source.PipeConfigNodeListenerMetrics;
import org.apache.iotdb.confignode.manager.pipe.metric.source.PipeConfigRegionSourceMetrics;
import org.apache.iotdb.metrics.AbstractMetricService;
import org.apache.iotdb.metrics.metricsets.IMetricSet;

public class PipeConfigNodeMetrics implements IMetricSet {

  private final PipeTaskInfoMetrics pipeTaskInfoMetrics;

  public PipeConfigNodeMetrics(final PipeManager pipeManager) {
    this.pipeTaskInfoMetrics = new PipeTaskInfoMetrics(pipeManager);
  }

  //////////////////////////// bindTo & unbindFrom (metric framework) ////////////////////////////

  @Override
  public void bindTo(final AbstractMetricService metricService) {
    PipeProcedureMetrics.getInstance().bindTo(metricService);
    pipeTaskInfoMetrics.bindTo(metricService);
    PipeConfigNodeListenerMetrics.getInstance().bindTo(metricService);
    PipeConfigRegionSourceMetrics.getInstance().bindTo(metricService);
    PipeConfigRegionSinkMetrics.getInstance().bindTo(metricService);
    PipeConfigNodeRemainingTimeMetrics.getInstance().bindTo(metricService);
    PipeTemporaryMetaInCoordinatorMetrics.getInstance().bindTo(metricService);
    PipeConfigNodeReceiverMetrics.getInstance().bindTo(metricService);
    PipeConfigNodeResourceMetrics.getInstance().bindTo(metricService);
  }

  @Override
  public void unbindFrom(final AbstractMetricService metricService) {
    PipeProcedureMetrics.getInstance().unbindFrom(metricService);
    pipeTaskInfoMetrics.unbindFrom(metricService);
    PipeConfigNodeListenerMetrics.getInstance().unbindFrom(metricService);
    PipeConfigRegionSourceMetrics.getInstance().unbindFrom(metricService);
    PipeConfigRegionSinkMetrics.getInstance().unbindFrom(metricService);
    PipeConfigNodeRemainingTimeMetrics.getInstance().unbindFrom(metricService);
    PipeTemporaryMetaInCoordinatorMetrics.getInstance().unbindFrom(metricService);
    PipeConfigNodeReceiverMetrics.getInstance().unbindFrom(metricService);
    PipeConfigNodeResourceMetrics.getInstance().unbindFrom(metricService);
  }
}
