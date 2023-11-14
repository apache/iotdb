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

import org.apache.iotdb.metrics.AbstractMetricService;
import org.apache.iotdb.metrics.metricsets.IMetricSet;

public class PipeMetrics implements IMetricSet {

  //////////////////////////// bindTo & unbindFrom (metric framework) ////////////////////////////

  @Override
  public void bindTo(AbstractMetricService metricService) {
    PipeAssignerMetrics.getInstance().bindTo(metricService);
    PipeExtractorMetrics.getInstance().bindTo(metricService);
    PipeProcessorMetrics.getInstance().bindTo(metricService);
    PipeConnectorMetrics.getInstance().bindTo(metricService);
    PipeHeartbeatEventMetrics.getInstance().bindTo(metricService);
    PipeWALInsertNodeCacheMetrics.getInstance().bindTo(metricService);
    PipeResourceMetrics.getInstance().bindTo(metricService);
  }

  @Override
  public void unbindFrom(AbstractMetricService metricService) {
    PipeAssignerMetrics.getInstance().unbindFrom(metricService);
    PipeExtractorMetrics.getInstance().unbindFrom(metricService);
    PipeProcessorMetrics.getInstance().unbindFrom(metricService);
    PipeConnectorMetrics.getInstance().unbindFrom(metricService);
    PipeHeartbeatEventMetrics.getInstance().unbindFrom(metricService);
    PipeWALInsertNodeCacheMetrics.getInstance().unbindFrom(metricService);
    PipeResourceMetrics.getInstance().unbindFrom(metricService);
  }

  //////////////////////////// singleton ////////////////////////////

  private static class PipeMetricsHolder {

    private static final PipeMetrics INSTANCE = new PipeMetrics();

    private PipeMetricsHolder() {
      // empty constructor
    }
  }

  public static PipeMetrics getInstance() {
    return PipeMetrics.PipeMetricsHolder.INSTANCE;
  }

  private PipeMetrics() {
    // empty constructor
  }
}
