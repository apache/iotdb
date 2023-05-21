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
import org.apache.iotdb.metrics.impl.DoNothingMetricManager;
import org.apache.iotdb.metrics.metricsets.IMetricSet;
import org.apache.iotdb.metrics.type.Timer;
import org.apache.iotdb.metrics.utils.MetricLevel;
import org.apache.iotdb.metrics.utils.MetricType;

import java.util.Arrays;

public class QueryPlanCostMetricSet implements IMetricSet {
  private static final QueryPlanCostMetricSet INSTANCE = new QueryPlanCostMetricSet();
  public static final String ANALYZER = "analyzer";
  public static final String LOGICAL_PLANNER = "logical_planner";
  public static final String DISTRIBUTION_PLANNER = "distribution_planner";

  public static final String PARTITION_FETCHER = "partition_fetcher";
  public static final String SCHEMA_FETCHER = "schema_fetcher";

  private QueryPlanCostMetricSet() {
    // empty constructor
  }

  private Timer analyzerTimer = DoNothingMetricManager.DO_NOTHING_TIMER;
  private Timer logicalPlannerTimer = DoNothingMetricManager.DO_NOTHING_TIMER;
  private Timer distributionPlannerTimer = DoNothingMetricManager.DO_NOTHING_TIMER;
  private Timer partitionFetcherTimer = DoNothingMetricManager.DO_NOTHING_TIMER;
  private Timer schemaFetcherTimer = DoNothingMetricManager.DO_NOTHING_TIMER;

  public void recordPlanCost(String stage, long costTimeInNanos) {
    switch (stage) {
      case ANALYZER:
        analyzerTimer.updateNanos(costTimeInNanos);
        break;
      case LOGICAL_PLANNER:
        logicalPlannerTimer.updateNanos(costTimeInNanos);
        break;
      case DISTRIBUTION_PLANNER:
        distributionPlannerTimer.updateNanos(costTimeInNanos);
        break;
      case PARTITION_FETCHER:
        partitionFetcherTimer.updateNanos(costTimeInNanos);
        break;
      case SCHEMA_FETCHER:
        schemaFetcherTimer.updateNanos(costTimeInNanos);
        break;
      default:
        throw new UnsupportedOperationException("Unsupported stage: " + stage);
    }
  }

  @Override
  public void bindTo(AbstractMetricService metricService) {
    analyzerTimer =
        metricService.getOrCreateTimer(
            Metric.QUERY_PLAN_COST.toString(),
            MetricLevel.IMPORTANT,
            Tag.STAGE.toString(),
            ANALYZER);
    logicalPlannerTimer =
        metricService.getOrCreateTimer(
            Metric.QUERY_PLAN_COST.toString(),
            MetricLevel.IMPORTANT,
            Tag.STAGE.toString(),
            LOGICAL_PLANNER);
    distributionPlannerTimer =
        metricService.getOrCreateTimer(
            Metric.QUERY_PLAN_COST.toString(),
            MetricLevel.IMPORTANT,
            Tag.STAGE.toString(),
            DISTRIBUTION_PLANNER);
    partitionFetcherTimer =
        metricService.getOrCreateTimer(
            Metric.QUERY_PLAN_COST.toString(),
            MetricLevel.IMPORTANT,
            Tag.STAGE.toString(),
            PARTITION_FETCHER);
    schemaFetcherTimer =
        metricService.getOrCreateTimer(
            Metric.QUERY_PLAN_COST.toString(),
            MetricLevel.IMPORTANT,
            Tag.STAGE.toString(),
            SCHEMA_FETCHER);
  }

  @Override
  public void unbindFrom(AbstractMetricService metricService) {
    Arrays.asList(
            ANALYZER, LOGICAL_PLANNER, DISTRIBUTION_PLANNER, PARTITION_FETCHER, SCHEMA_FETCHER)
        .forEach(
            stage -> {
              metricService.remove(
                  MetricType.TIMER, Metric.QUERY_PLAN_COST.toString(), Tag.STAGE.toString(), stage);
            });
  }

  public static QueryPlanCostMetricSet getInstance() {
    return INSTANCE;
  }
}
