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

package org.apache.iotdb.db.queryengine.plan.relational.planner.optimizations;

import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanVisitor;
import org.apache.iotdb.db.queryengine.plan.relational.planner.OrderingScheme;
import org.apache.iotdb.db.queryengine.plan.relational.planner.Symbol;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.DeviceTableScanNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.FillNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.GapFillNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.PatternRecognitionNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.SortNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.StreamSortNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.ValueFillNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.WindowNode;

import java.util.Collections;

/**
 * <b>Optimization phase:</b> Distributed plan planning.
 *
 * <p>This optimize rule implement the rules below.
 * <li>When order by time and there is only one device entry in DeviceTableScanNode below, the
 *     SortNode can be eliminated.
 * <li>When order by all IDColumns and time, the SortNode can be eliminated.
 * <li>When StreamSortIndex==OrderBy size()-1, remove this StreamSortNode
 */
public class SortElimination implements PlanOptimizer {

  @Override
  public PlanNode optimize(PlanNode plan, PlanOptimizer.Context context) {
    if (!context.getAnalysis().hasSortNode()) {
      return plan;
    }

    return plan.accept(new Rewriter(), new Context());
  }

  private static class Rewriter extends PlanVisitor<PlanNode, Context> {
    @Override
    public PlanNode visitPlan(PlanNode node, Context context) {
      PlanNode newNode = node.clone();
      for (PlanNode child : node.getChildren()) {
        newNode.addChild(child.accept(this, context));
      }
      return newNode;
    }

    @Override
    public PlanNode visitSort(SortNode node, Context context) {
      Context newContext = new Context();
      PlanNode child = node.getChild().accept(this, newContext);
      context.setCannotEliminateSort(newContext.cannotEliminateSort);
      OrderingScheme orderingScheme = node.getOrderingScheme();
      if (context.canEliminateSort()
          && newContext.getTotalDeviceEntrySize() == 1
          && orderingScheme.getOrderBy().get(0).getName().equals(context.getTimeColumnName())) {
        return child;
      }
      return context.canEliminateSort() && node.isOrderByAllIdsAndTime()
          ? child
          : node.replaceChildren(Collections.singletonList(child));
    }

    @Override
    public PlanNode visitStreamSort(StreamSortNode node, Context context) {
      Context newContext = new Context();
      PlanNode child = node.getChild().accept(this, newContext);
      context.setCannotEliminateSort(newContext.cannotEliminateSort);
      return context.canEliminateSort()
              && (node.isOrderByAllIdsAndTime()
                  || node.getStreamCompareKeyEndIndex()
                      == node.getOrderingScheme().getOrderBy().size() - 1)
          ? child
          : node.replaceChildren(Collections.singletonList(child));
    }

    @Override
    public PlanNode visitDeviceTableScan(DeviceTableScanNode node, Context context) {
      context.addDeviceEntrySize(node.getDeviceEntries().size());
      context.setTimeColumnName(node.getTimeColumn().map(Symbol::getName).orElse(null));
      return node;
    }

    @Override
    public PlanNode visitFill(FillNode node, Context context) {
      PlanNode newNode = node.clone();
      for (PlanNode child : node.getChildren()) {
        newNode.addChild(child.accept(this, context));
      }
      context.setCannotEliminateSort(!(node instanceof ValueFillNode));
      return newNode;
    }

    @Override
    public PlanNode visitGapFill(GapFillNode node, Context context) {
      PlanNode newNode = node.clone();
      for (PlanNode child : node.getChildren()) {
        newNode.addChild(child.accept(this, context));
      }
      context.setCannotEliminateSort(true);
      return newNode;
    }

    @Override
    public PlanNode visitWindowFunction(WindowNode node, Context context) {
      PlanNode newNode = node.clone();
      for (PlanNode child : node.getChildren()) {
        newNode.addChild(child.accept(this, context));
      }
      context.setCannotEliminateSort(true);
      return newNode;
    }

    @Override
    public PlanNode visitPatternRecognition(PatternRecognitionNode node, Context context) {
      PlanNode newNode = node.clone();
      for (PlanNode child : node.getChildren()) {
        newNode.addChild(child.accept(this, context));
      }
      context.setCannotEliminateSort(true);
      return newNode;
    }
  }

  private static class Context {
    private int totalDeviceEntrySize = 0;

    // There are 3 situations where sort cannot be eliminated
    // 1. Query plan has linear fill, previous fill or gapfill
    // 2. Query plan has window function and it has ordering scheme
    // 3. Query plan has pattern recognition and it has ordering scheme
    private boolean cannotEliminateSort = false;

    private String timeColumnName = null;

    Context() {}

    public void addDeviceEntrySize(int deviceEntrySize) {
      this.totalDeviceEntrySize += deviceEntrySize;
    }

    public int getTotalDeviceEntrySize() {
      return totalDeviceEntrySize;
    }

    public boolean canEliminateSort() {
      return !cannotEliminateSort;
    }

    public void setCannotEliminateSort(boolean cannotEliminateSort) {
      this.cannotEliminateSort = cannotEliminateSort;
    }

    public String getTimeColumnName() {
      return timeColumnName;
    }

    public void setTimeColumnName(String timeColumnName) {
      this.timeColumnName = timeColumnName;
    }
  }
}
