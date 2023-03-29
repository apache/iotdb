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

import org.apache.iotdb.commons.service.metric.enums.Metric;
import org.apache.iotdb.commons.service.metric.enums.Tag;
import org.apache.iotdb.metrics.AbstractMetricService;
import org.apache.iotdb.metrics.metricsets.IMetricSet;
import org.apache.iotdb.metrics.utils.MetricLevel;
import org.apache.iotdb.metrics.utils.MetricType;

import java.util.Arrays;
import java.util.List;

public class QueryPlanCostMetricSet implements IMetricSet {
  public static final String ANALYZER = "analyzer";
  public static final String LOGICAL_PLANNER = "logical_planner";
  public static final String DISTRIBUTION_PLANNER = "distribution_planner";

  public static final String PARTITION_FETCHER = "partition_fetcher";
  public static final String SCHEMA_FETCHER = "schema_fetcher";

  private final String metric = Metric.QUERY_PLAN_COST.toString();
  private final String tagKey = Tag.STAGE.toString();

  private static final List<String> stages =
      Arrays.asList(
          ANALYZER, LOGICAL_PLANNER, DISTRIBUTION_PLANNER, PARTITION_FETCHER, SCHEMA_FETCHER);

  @Override
  public void bindTo(AbstractMetricService metricService) {
    for (String stage : stages) {
      metricService.getOrCreateTimer(metric, MetricLevel.IMPORTANT, tagKey, stage);
    }
  }

  @Override
  public void unbindFrom(AbstractMetricService metricService) {
    for (String stage : stages) {
      metricService.remove(MetricType.TIMER, metric, tagKey, stage);
    }
  }
}
