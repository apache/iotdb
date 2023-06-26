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

package org.apache.iotdb.db.queryengine.execution.operator.source;

import org.apache.iotdb.commons.path.AlignedPath;
import org.apache.iotdb.db.queryengine.execution.aggregation.Aggregator;
import org.apache.iotdb.db.queryengine.execution.aggregation.timerangeiterator.ITimeRangeIterator;
import org.apache.iotdb.db.queryengine.execution.operator.OperatorContext;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNodeId;
import org.apache.iotdb.db.queryengine.plan.planner.plan.parameter.GroupByTimeParameter;
import org.apache.iotdb.db.queryengine.plan.planner.plan.parameter.SeriesScanOptions;
import org.apache.iotdb.db.queryengine.plan.statement.component.Ordering;

import java.util.List;

/** This operator is responsible to do the aggregation calculation especially for aligned series. */
public class AlignedSeriesAggregationScanOperator extends AbstractSeriesAggregationScanOperator {

  @SuppressWarnings("squid:S107")
  public AlignedSeriesAggregationScanOperator(
      PlanNodeId sourceId,
      AlignedPath seriesPath,
      Ordering scanOrder,
      SeriesScanOptions scanOptions,
      OperatorContext context,
      List<Aggregator> aggregators,
      ITimeRangeIterator timeRangeIterator,
      GroupByTimeParameter groupByTimeParameter,
      long maxReturnSize) {
    super(
        sourceId,
        context,
        new AlignedSeriesScanUtil(seriesPath, scanOrder, scanOptions, context.getInstanceContext()),
        seriesPath.getMeasurementList().size(),
        aggregators,
        timeRangeIterator,
        scanOrder.isAscending(),
        groupByTimeParameter,
        maxReturnSize);
  }
}
