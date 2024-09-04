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

package org.apache.iotdb.db.queryengine.metric;

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

  public static final String TABLE_TYPE = "table";
  public static final String TREE_TYPE = "tree";

  public static final String ANALYZER = "analyzer";
  public static final String LOGICAL_PLANNER = "logical_planner";
  public static final String LOGICAL_PLAN_OPTIMIZE = "logical_plan_optimize";
  public static final String DISTRIBUTION_PLANNER = "distribution_planner";
  public static final String PARTITION_FETCHER = "partition_fetcher";
  public static final String SCHEMA_FETCHER = "schema_fetcher";

  private QueryPlanCostMetricSet() {
    // empty constructor
  }

  private Timer treeAnalyzerTimer = DoNothingMetricManager.DO_NOTHING_TIMER;
  private Timer treeLogicalPlannerTimer = DoNothingMetricManager.DO_NOTHING_TIMER;
  private Timer treeDistributionPlannerTimer = DoNothingMetricManager.DO_NOTHING_TIMER;
  private Timer treePartitionFetcherTimer = DoNothingMetricManager.DO_NOTHING_TIMER;
  private Timer treeSchemaFetcherTimer = DoNothingMetricManager.DO_NOTHING_TIMER;

  private Timer tableAnalyzerTimer = DoNothingMetricManager.DO_NOTHING_TIMER;
  private Timer tableLogicalPlannerTimer = DoNothingMetricManager.DO_NOTHING_TIMER;
  private Timer tableLogicalPlanOptimizerTimer = DoNothingMetricManager.DO_NOTHING_TIMER;
  private Timer tableDistributionPlannerTimer = DoNothingMetricManager.DO_NOTHING_TIMER;
  private Timer tablePartitionFetcherTimer = DoNothingMetricManager.DO_NOTHING_TIMER;
  private Timer tableSchemaFetcherTimer = DoNothingMetricManager.DO_NOTHING_TIMER;

  public void recordPlanCost(String type, String stage, long costTimeInNanos) {
    switch (stage) {
      case ANALYZER:
        if (TREE_TYPE.equals(type)) {
          treeAnalyzerTimer.updateNanos(costTimeInNanos);
        } else {
          tableAnalyzerTimer.updateNanos(costTimeInNanos);
        }
        break;
      case LOGICAL_PLANNER:
        if (TREE_TYPE.equals(type)) {
          treeLogicalPlannerTimer.updateNanos(costTimeInNanos);
        } else {
          tableLogicalPlannerTimer.updateNanos(costTimeInNanos);
        }
        break;
      case LOGICAL_PLAN_OPTIMIZE:
        tableLogicalPlanOptimizerTimer.updateNanos(costTimeInNanos);
        break;
      case DISTRIBUTION_PLANNER:
        if (TREE_TYPE.equals(type)) {
          treeDistributionPlannerTimer.updateNanos(costTimeInNanos);
        } else {
          tableDistributionPlannerTimer.updateNanos(costTimeInNanos);
        }
        break;
      case PARTITION_FETCHER:
        if (TREE_TYPE.equals(type)) {
          treePartitionFetcherTimer.updateNanos(costTimeInNanos);
        } else {
          tablePartitionFetcherTimer.updateNanos(costTimeInNanos);
        }
        break;
      case SCHEMA_FETCHER:
        if (TREE_TYPE.equals(type)) {
          treeSchemaFetcherTimer.updateNanos(costTimeInNanos);
        } else {
          tableSchemaFetcherTimer.updateNanos(costTimeInNanos);
        }
        break;
      default:
        throw new UnsupportedOperationException("Unsupported stage: " + stage);
    }
  }

  @Override
  public void bindTo(AbstractMetricService metricService) {
    treeAnalyzerTimer =
        metricService.getOrCreateTimer(
            Metric.QUERY_PLAN_COST.toString(),
            MetricLevel.IMPORTANT,
            Tag.TYPE.toString(),
            TREE_TYPE,
            Tag.STAGE.toString(),
            ANALYZER);
    treeLogicalPlannerTimer =
        metricService.getOrCreateTimer(
            Metric.QUERY_PLAN_COST.toString(),
            MetricLevel.IMPORTANT,
            Tag.TYPE.toString(),
            TREE_TYPE,
            Tag.STAGE.toString(),
            LOGICAL_PLANNER);
    treeDistributionPlannerTimer =
        metricService.getOrCreateTimer(
            Metric.QUERY_PLAN_COST.toString(),
            MetricLevel.IMPORTANT,
            Tag.TYPE.toString(),
            TREE_TYPE,
            Tag.STAGE.toString(),
            DISTRIBUTION_PLANNER);
    treePartitionFetcherTimer =
        metricService.getOrCreateTimer(
            Metric.QUERY_PLAN_COST.toString(),
            MetricLevel.IMPORTANT,
            Tag.TYPE.toString(),
            TREE_TYPE,
            Tag.STAGE.toString(),
            PARTITION_FETCHER);
    treeSchemaFetcherTimer =
        metricService.getOrCreateTimer(
            Metric.QUERY_PLAN_COST.toString(),
            MetricLevel.IMPORTANT,
            Tag.TYPE.toString(),
            TREE_TYPE,
            Tag.STAGE.toString(),
            SCHEMA_FETCHER);

    tableAnalyzerTimer =
        metricService.getOrCreateTimer(
            Metric.QUERY_PLAN_COST.toString(),
            MetricLevel.IMPORTANT,
            Tag.TYPE.toString(),
            TABLE_TYPE,
            Tag.STAGE.toString(),
            ANALYZER);
    tableLogicalPlannerTimer =
        metricService.getOrCreateTimer(
            Metric.QUERY_PLAN_COST.toString(),
            MetricLevel.IMPORTANT,
            Tag.TYPE.toString(),
            TABLE_TYPE,
            Tag.STAGE.toString(),
            LOGICAL_PLANNER);
    tableLogicalPlanOptimizerTimer =
        metricService.getOrCreateTimer(
            Metric.QUERY_PLAN_COST.toString(),
            MetricLevel.IMPORTANT,
            Tag.TYPE.toString(),
            TABLE_TYPE,
            Tag.STAGE.toString(),
            LOGICAL_PLAN_OPTIMIZE);
    tableDistributionPlannerTimer =
        metricService.getOrCreateTimer(
            Metric.QUERY_PLAN_COST.toString(),
            MetricLevel.IMPORTANT,
            Tag.TYPE.toString(),
            TABLE_TYPE,
            Tag.STAGE.toString(),
            DISTRIBUTION_PLANNER);
    tablePartitionFetcherTimer =
        metricService.getOrCreateTimer(
            Metric.QUERY_PLAN_COST.toString(),
            MetricLevel.IMPORTANT,
            Tag.TYPE.toString(),
            TABLE_TYPE,
            Tag.STAGE.toString(),
            PARTITION_FETCHER);
    tableSchemaFetcherTimer =
        metricService.getOrCreateTimer(
            Metric.QUERY_PLAN_COST.toString(),
            MetricLevel.IMPORTANT,
            Tag.TYPE.toString(),
            TABLE_TYPE,
            Tag.STAGE.toString(),
            SCHEMA_FETCHER);
  }

  @Override
  public void unbindFrom(AbstractMetricService metricService) {
    Arrays.asList(
            ANALYZER,
            LOGICAL_PLANNER,
            LOGICAL_PLAN_OPTIMIZE,
            DISTRIBUTION_PLANNER,
            PARTITION_FETCHER,
            SCHEMA_FETCHER)
        .forEach(
            stage ->
                Arrays.asList(TREE_TYPE, TABLE_TYPE)
                    .forEach(
                        type ->
                            metricService.remove(
                                MetricType.TIMER,
                                Metric.QUERY_PLAN_COST.toString(),
                                Tag.TYPE.toString(),
                                type,
                                Tag.STAGE.toString(),
                                stage)));
  }

  public static QueryPlanCostMetricSet getInstance() {
    return INSTANCE;
  }
}
