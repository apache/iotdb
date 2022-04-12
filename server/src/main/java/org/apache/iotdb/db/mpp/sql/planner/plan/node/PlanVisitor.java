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
package org.apache.iotdb.db.mpp.sql.planner.plan.node;

import org.apache.iotdb.db.mpp.sql.planner.plan.node.metedata.read.DevicesSchemaScanNode;
import org.apache.iotdb.db.mpp.sql.planner.plan.node.metedata.read.SchemaMergeNode;
import org.apache.iotdb.db.mpp.sql.planner.plan.node.metedata.read.SchemaScanNode;
import org.apache.iotdb.db.mpp.sql.planner.plan.node.metedata.read.TimeSeriesSchemaScanNode;
import org.apache.iotdb.db.mpp.sql.planner.plan.node.process.AggregateNode;
import org.apache.iotdb.db.mpp.sql.planner.plan.node.process.DeviceMergeNode;
import org.apache.iotdb.db.mpp.sql.planner.plan.node.process.ExchangeNode;
import org.apache.iotdb.db.mpp.sql.planner.plan.node.process.FillNode;
import org.apache.iotdb.db.mpp.sql.planner.plan.node.process.FilterNode;
import org.apache.iotdb.db.mpp.sql.planner.plan.node.process.FilterNullNode;
import org.apache.iotdb.db.mpp.sql.planner.plan.node.process.GroupByLevelNode;
import org.apache.iotdb.db.mpp.sql.planner.plan.node.process.LimitNode;
import org.apache.iotdb.db.mpp.sql.planner.plan.node.process.OffsetNode;
import org.apache.iotdb.db.mpp.sql.planner.plan.node.process.SortNode;
import org.apache.iotdb.db.mpp.sql.planner.plan.node.process.TimeJoinNode;
import org.apache.iotdb.db.mpp.sql.planner.plan.node.sink.FragmentSinkNode;
import org.apache.iotdb.db.mpp.sql.planner.plan.node.source.SeriesAggregateScanNode;
import org.apache.iotdb.db.mpp.sql.planner.plan.node.source.SeriesScanNode;

public abstract class PlanVisitor<R, C> {

  public R process(PlanNode node, C context) {
    return node.accept(this, context);
  }

  public abstract R visitPlan(PlanNode node, C context);

  public R visitSeriesScan(SeriesScanNode node, C context) {
    return visitPlan(node, context);
  }

  public R visitSeriesAggregate(SeriesAggregateScanNode node, C context) {
    return visitPlan(node, context);
  }

  public R visitDeviceMerge(DeviceMergeNode node, C context) {
    return visitPlan(node, context);
  }

  public R visitFill(FillNode node, C context) {
    return visitPlan(node, context);
  }

  public R visitFilter(FilterNode node, C context) {
    return visitPlan(node, context);
  }

  public R visitFilterNull(FilterNullNode node, C context) {
    return visitPlan(node, context);
  }

  public R visitGroupByLevel(GroupByLevelNode node, C context) {
    return visitPlan(node, context);
  }

  public R visitLimit(LimitNode node, C context) {
    return visitPlan(node, context);
  }

  public R visitOffset(OffsetNode node, C context) {
    return visitPlan(node, context);
  }

  public R visitRowBasedSeriesAggregate(AggregateNode node, C context) {
    return visitPlan(node, context);
  }

  public R visitSort(SortNode node, C context) {
    return visitPlan(node, context);
  }

  public R visitTimeJoin(TimeJoinNode node, C context) {
    return visitPlan(node, context);
  }

  public R visitExchange(ExchangeNode node, C context) {
    return visitPlan(node, context);
  }

  public R visitMetaMerge(SchemaMergeNode node, C context) {
    return visitPlan(node, context);
  };

  public R visitMetaScan(SchemaScanNode node, C context) {
    return visitPlan(node, context);
  }

  public R visitTimeSeriesMetaScan(TimeSeriesSchemaScanNode node, C context) {
    return visitMetaScan(node, context);
  }

  public R visitDevicesMetaScan(DevicesSchemaScanNode node, C context) {
    return visitMetaScan(node, context);
  }

  public R visitFragmentSink(FragmentSinkNode node, C context) {
    return visitPlan(node, context);
  }
}
