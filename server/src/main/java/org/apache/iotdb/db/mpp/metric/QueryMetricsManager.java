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

package org.apache.iotdb.db.mpp.metric;

import org.apache.iotdb.commons.service.metric.MetricService;
import org.apache.iotdb.commons.service.metric.enums.Metric;
import org.apache.iotdb.commons.service.metric.enums.Tag;
import org.apache.iotdb.metrics.type.Timer;
import org.apache.iotdb.metrics.utils.MetricLevel;

public class QueryMetricsManager {

  private final MetricService metricService = MetricService.getInstance();

  public void addPlanCost(String stage, long costTimeInNanos) {
    Timer timer =
        metricService.getOrCreateTimer(
            Metric.QUERY_PLAN_COST.toString(), MetricLevel.IMPORTANT, Tag.STAGE.toString(), stage);
    timer.updateNanos(costTimeInNanos);
  }

  public static QueryMetricsManager getInstance() {
    return QueryMetricsManager.QueryMetricsManagerHolder.INSTANCE;
  }

  private static class QueryMetricsManagerHolder {

    private static final QueryMetricsManager INSTANCE = new QueryMetricsManager();

    private QueryMetricsManagerHolder() {}
  }
}
