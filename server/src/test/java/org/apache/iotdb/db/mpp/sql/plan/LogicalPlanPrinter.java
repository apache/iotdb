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

package org.apache.iotdb.db.mpp.sql.plan;

import org.apache.iotdb.db.mpp.sql.planner.plan.node.PlanNode;
import org.apache.iotdb.db.mpp.sql.planner.plan.node.PlanVisitor;
import org.apache.iotdb.db.mpp.sql.planner.plan.node.process.*;
import org.apache.iotdb.db.mpp.sql.planner.plan.node.source.SeriesAggregateScanNode;
import org.apache.iotdb.db.mpp.sql.planner.plan.node.source.SeriesScanNode;

public class LogicalPlanPrinter {

  private static final String INDENT = "   ";

  public void print(PlanNode root) {
    LogicalPlanPrintVisitor printer = new LogicalPlanPrintVisitor();
    printer.process(root, 0);
  }

  private static class LogicalPlanPrintVisitor extends PlanVisitor<Void, Integer> {

    @Override
    public Void visitPlan(PlanNode node, Integer indentLevel) {
      throw new UnsupportedOperationException("not yet implemented: " + node);
    }

    public Void visitSeriesScan(SeriesScanNode node, Integer indentLevel) {
      print(indentLevel, node.toString());
      return null;
    }

    public Void visitSeriesAggregate(SeriesAggregateScanNode node, Integer indentLevel) {
      print(indentLevel, node.toString());
      return null;
    }

    public Void visitDeviceMerge(DeviceMergeNode node, Integer indentLevel) {
      print(indentLevel, node.toString());
      for (PlanNode subNode : node.getChildren()) {
        process(subNode, indentLevel + 1);
      }
      return null;
    }

    public Void visitFill(FillNode node, Integer indentLevel) {
      return null;
    }

    public Void visitFilter(FilterNode node, Integer indentLevel) {
      return null;
    }

    public Void visitFilterNull(FilterNullNode node, Integer indentLevel) {
      return null;
    }

    public Void visitGroupByLevel(GroupByLevelNode node, Integer indentLevel) {
      return null;
    }

    public Void visitLimit(LimitNode node, Integer indentLevel) {
      return null;
    }

    public Void visitOffset(OffsetNode node, Integer indentLevel) {
      return null;
    }

    public Void visitRowBasedSeriesAggregate(AggregateNode node, Integer indentLevel) {
      return null;
    }

    public Void visitSort(SortNode node, Integer indentLevel) {
      return null;
    }

    public Void visitTimeJoin(TimeJoinNode node, Integer indentLevel) {
      print(indentLevel, node.toString());
      for (PlanNode subNode : node.getChildren()) {
        process(subNode, indentLevel + 1);
      }
      return null;
    }

    private void print(Integer indentLevel, String value) {
      System.out.println(repeatIndent(indentLevel) + value);
    }

    private String repeatIndent(int indentLevel) {
      StringBuilder res = new StringBuilder();
      for (int i = 0; i < indentLevel; i++) {
        res.append(INDENT);
      }
      return res.toString();
    }
  }
}
