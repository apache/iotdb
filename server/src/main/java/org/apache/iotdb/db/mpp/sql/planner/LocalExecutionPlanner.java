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
package org.apache.iotdb.db.mpp.sql.planner;

import org.apache.iotdb.db.mpp.execution.InstanceContext;
import org.apache.iotdb.db.mpp.operator.Operator;
import org.apache.iotdb.db.mpp.sql.planner.plan.node.PlanNode;
import org.apache.iotdb.db.mpp.sql.planner.plan.node.PlanVisitor;
import org.apache.iotdb.db.mpp.sql.planner.plan.node.process.*;
import org.apache.iotdb.db.mpp.sql.planner.plan.node.source.SeriesAggregateScanNode;
import org.apache.iotdb.db.mpp.sql.planner.plan.node.source.SeriesScanNode;
import org.apache.iotdb.db.qp.logical.crud.FilterOperator;

import java.util.List;

/**
 * used to plan a fragment instance. Currently, we simply change it from PlanNode to executable Operator tree,
 * but in the future, we may split one fragment instance into multiple pipeline to run a fragment instance parallel and take full advantage of multi-cores
 */
public class LocalExecutionPlanner {


    /**
     * This Visitor is responsible for transferring PlanNode Tree to Operator Tree
     */
    private class Visitor extends PlanVisitor<Operator, LocalExecutionPlanContext> {

        @Override
        public Operator visitPlan(PlanNode node, LocalExecutionPlanContext context) {
            throw new UnsupportedOperationException("should call the concrete visitXX() method");
        }

        @Override
        public Operator visitSeriesScan(SeriesScanNode node, LocalExecutionPlanContext context) {
            return super.visitSeriesScan(node, context);
        }

        @Override
        public Operator visitSeriesAggregate(SeriesAggregateScanNode node, LocalExecutionPlanContext context) {
            return super.visitSeriesAggregate(node, context);
        }

        @Override
        public Operator visitDeviceMerge(DeviceMergeNode node, LocalExecutionPlanContext context) {
            return super.visitDeviceMerge(node, context);
        }

        @Override
        public Operator visitFill(FillNode node, LocalExecutionPlanContext context) {
            return super.visitFill(node, context);
        }

        @Override
        public Operator visitFilter(FilterNode node, LocalExecutionPlanContext context) {
            PlanNode child = node.getChild();

            FilterOperator filterExpression = node.getPredicate();
            List<String> outputSymbols = node.getOutputColumnNames();
            return super.visitFilter(node, context);
        }

        @Override
        public Operator visitFilterNull(FilterNullNode node, LocalExecutionPlanContext context) {
            return super.visitFilterNull(node, context);
        }

        @Override
        public Operator visitGroupByLevel(GroupByLevelNode node, LocalExecutionPlanContext context) {
            return super.visitGroupByLevel(node, context);
        }

        @Override
        public Operator visitLimit(LimitNode node, LocalExecutionPlanContext context) {
            return super.visitLimit(node, context);
        }

        @Override
        public Operator visitOffset(OffsetNode node, LocalExecutionPlanContext context) {
            return super.visitOffset(node, context);
        }

        @Override
        public Operator visitRowBasedSeriesAggregate(AggregateNode node, LocalExecutionPlanContext context) {
            return super.visitRowBasedSeriesAggregate(node, context);
        }

        @Override
        public Operator visitSort(SortNode node, LocalExecutionPlanContext context) {
            return super.visitSort(node, context);
        }

        @Override
        public Operator visitTimeJoin(TimeJoinNode node, LocalExecutionPlanContext context) {
            return super.visitTimeJoin(node, context);
        }
    }

    private static class LocalExecutionPlanContext {
        private final InstanceContext taskContext;
        private int nextOperatorId = 0;

        public LocalExecutionPlanContext(InstanceContext taskContext) {
            this.taskContext = taskContext;
        }

        private int getNextOperatorId() {
            return nextOperatorId++;
        }
    }
}
