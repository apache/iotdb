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

import org.apache.iotdb.commons.pipe.resource.ref.PipePhantomReferenceManager;
import org.apache.iotdb.commons.service.metric.enums.Metric;
import org.apache.iotdb.confignode.manager.pipe.resource.PipeConfigNodeResourceManager;
import org.apache.iotdb.metrics.AbstractMetricService;
import org.apache.iotdb.metrics.metricsets.IMetricSet;
import org.apache.iotdb.metrics.utils.MetricLevel;
import org.apache.iotdb.metrics.utils.MetricType;

public class PipeConfigNodeResourceMetrics implements IMetricSet {

  //////////////////////////// bindTo & unbindFrom (metric framework) ////////////////////////////

  @Override
  public void bindTo(final AbstractMetricService metricService) {
    // phantom reference count
    metricService.createAutoGauge(
        Metric.PIPE_PHANTOM_REFERENCE_COUNT.toString(),
        MetricLevel.IMPORTANT,
        PipeConfigNodeResourceManager.ref(),
        PipePhantomReferenceManager::getPhantomReferenceCount);
  }

  @Override
  public void unbindFrom(final AbstractMetricService metricService) {
    // phantom reference count
    metricService.remove(MetricType.AUTO_GAUGE, Metric.PIPE_PHANTOM_REFERENCE_COUNT.toString());
  }

  //////////////////////////// singleton ////////////////////////////

  private static class PipeConfigNodeResourceMetricsHolder {

    private static final PipeConfigNodeResourceMetrics INSTANCE =
        new PipeConfigNodeResourceMetrics();

    private PipeConfigNodeResourceMetricsHolder() {
      // empty constructor
    }
  }

  public static PipeConfigNodeResourceMetrics getInstance() {
    return PipeConfigNodeResourceMetrics.PipeConfigNodeResourceMetricsHolder.INSTANCE;
  }

  private PipeConfigNodeResourceMetrics() {
    // empty constructor
  }
}
