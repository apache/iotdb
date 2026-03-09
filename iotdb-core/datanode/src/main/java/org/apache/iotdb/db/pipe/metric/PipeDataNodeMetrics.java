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

package org.apache.iotdb.db.pipe.metric;

import org.apache.iotdb.commons.pipe.metric.PipeEventCommitMetrics;
import org.apache.iotdb.db.pipe.metric.overview.PipeDataNodeSinglePipeMetrics;
import org.apache.iotdb.db.pipe.metric.overview.PipeHeartbeatEventMetrics;
import org.apache.iotdb.db.pipe.metric.overview.PipeResourceMetrics;
import org.apache.iotdb.db.pipe.metric.overview.PipeTsFileToTabletsMetrics;
import org.apache.iotdb.db.pipe.metric.processor.PipeProcessorMetrics;
import org.apache.iotdb.db.pipe.metric.receiver.PipeDataNodeReceiverMetrics;
import org.apache.iotdb.db.pipe.metric.schema.PipeSchemaRegionListenerMetrics;
import org.apache.iotdb.db.pipe.metric.schema.PipeSchemaRegionSinkMetrics;
import org.apache.iotdb.db.pipe.metric.schema.PipeSchemaRegionSourceMetrics;
import org.apache.iotdb.db.pipe.metric.sink.PipeDataRegionSinkMetrics;
import org.apache.iotdb.db.pipe.metric.source.PipeAssignerMetrics;
import org.apache.iotdb.db.pipe.metric.source.PipeDataRegionSourceMetrics;
import org.apache.iotdb.metrics.AbstractMetricService;
import org.apache.iotdb.metrics.metricsets.IMetricSet;

public class PipeDataNodeMetrics implements IMetricSet {

  //////////////////////////// bindTo & unbindFrom (metric framework) ////////////////////////////

  @Override
  public void bindTo(final AbstractMetricService metricService) {
    PipeAssignerMetrics.getInstance().bindTo(metricService);
    PipeDataRegionSourceMetrics.getInstance().bindTo(metricService);
    PipeProcessorMetrics.getInstance().bindTo(metricService);
    PipeDataRegionSinkMetrics.getInstance().bindTo(metricService);
    PipeHeartbeatEventMetrics.getInstance().bindTo(metricService);
    PipeResourceMetrics.getInstance().bindTo(metricService);
    PipeEventCommitMetrics.getInstance().bindTo(metricService);
    PipeSchemaRegionListenerMetrics.getInstance().bindTo(metricService);
    PipeSchemaRegionSourceMetrics.getInstance().bindTo(metricService);
    PipeSchemaRegionSinkMetrics.getInstance().bindTo(metricService);
    PipeDataNodeSinglePipeMetrics.getInstance().bindTo(metricService);
    PipeDataNodeReceiverMetrics.getInstance().bindTo(metricService);
    PipeTsFileToTabletsMetrics.getInstance().bindTo(metricService);
  }

  @Override
  public void unbindFrom(final AbstractMetricService metricService) {
    PipeAssignerMetrics.getInstance().unbindFrom(metricService);
    PipeDataRegionSourceMetrics.getInstance().unbindFrom(metricService);
    PipeProcessorMetrics.getInstance().unbindFrom(metricService);
    PipeDataRegionSinkMetrics.getInstance().unbindFrom(metricService);
    PipeHeartbeatEventMetrics.getInstance().unbindFrom(metricService);
    PipeResourceMetrics.getInstance().unbindFrom(metricService);
    PipeEventCommitMetrics.getInstance().unbindFrom(metricService);
    PipeSchemaRegionListenerMetrics.getInstance().unbindFrom(metricService);
    PipeSchemaRegionSourceMetrics.getInstance().unbindFrom(metricService);
    PipeSchemaRegionSinkMetrics.getInstance().unbindFrom(metricService);
    PipeDataNodeSinglePipeMetrics.getInstance().unbindFrom(metricService);
    PipeDataNodeReceiverMetrics.getInstance().unbindFrom(metricService);
    PipeTsFileToTabletsMetrics.getInstance().unbindFrom(metricService);
  }

  //////////////////////////// singleton ////////////////////////////

  private static class PipeDataNodeMetricsHolder {

    private static final PipeDataNodeMetrics INSTANCE = new PipeDataNodeMetrics();

    private PipeDataNodeMetricsHolder() {
      // empty constructor
    }
  }

  public static PipeDataNodeMetrics getInstance() {
    return PipeDataNodeMetricsHolder.INSTANCE;
  }

  private PipeDataNodeMetrics() {
    // empty constructor
  }
}
