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
import org.apache.iotdb.tsfile.utils.Pair;

import java.util.List;

public class LogicalPlanPrinter {

  private static final String INDENT = "   ";
  private static final String CORNER = " └─";
  private static final String LINE = " │";

  public String print(PlanNode root) {
    LogicalPlanPrintVisitor printer = new LogicalPlanPrintVisitor();
    printer.process(root, new PrinterContext(0, false));
    return printer.getOutput();
  }

  private static class LogicalPlanPrintVisitor extends PlanVisitor<Void, PrinterContext> {

    private final StringBuilder stringBuilder = new StringBuilder();

    @Override
    public Void visitPlan(PlanNode node, PrinterContext context) {
      throw new UnsupportedOperationException("not yet implemented: " + node);
    }

    @Override
    public Void visitSeriesScan(SeriesScanNode node, PrinterContext context) {
      print(context.getIndentLevel(), context.isShowCorner(), node.print());
      return null;
    }

    @Override
    public Void visitSeriesAggregate(SeriesAggregateScanNode node, PrinterContext context) {
      print(context.getIndentLevel(), context.isShowCorner(), node.print());
      return null;
    }

    @Override
    public Void visitDeviceMerge(DeviceMergeNode node, PrinterContext context) {
      print(context.getIndentLevel(), context.isShowCorner(), node.print());
      context.incIndentLevel();
      context.setShowCorner(true);
      for (int i = 0; i < node.getChildren().size(); i++) {
        if (i > 0) {
          context.setShowCorner(false);
        }
        process(node.getChildren().get(i), context);
      }
      return null;
    }

    @Override
    public Void visitFill(FillNode node, PrinterContext context) {
      print(context.getIndentLevel(), context.isShowCorner(), node.print());
      context.incIndentLevel();
      context.setShowCorner(true);
      process(node.getChildren().get(0), context);
      return null;
    }

    @Override
    public Void visitFilter(FilterNode node, PrinterContext context) {
      print(context.getIndentLevel(), context.isShowCorner(), node.print());
      context.incIndentLevel();
      context.setShowCorner(true);
      process(node.getChildren().get(0), context);
      return null;
    }

    @Override
    public Void visitFilterNull(FilterNullNode node, PrinterContext context) {
      print(context.getIndentLevel(), context.isShowCorner(), node.print());
      context.incIndentLevel();
      context.setShowCorner(true);
      process(node.getChildren().get(0), context);
      return null;
    }

    @Override
    public Void visitGroupByLevel(GroupByLevelNode node, PrinterContext context) {
      print(context.getIndentLevel(), context.isShowCorner(), node.print());
      context.incIndentLevel();
      context.setShowCorner(true);
      for (int i = 0; i < node.getChildren().size(); i++) {
        if (i > 0) {
          context.setShowCorner(false);
        }
        process(node.getChildren().get(i), context);
      }
      return null;
    }

    @Override
    public Void visitLimit(LimitNode node, PrinterContext context) {
      print(context.getIndentLevel(), context.isShowCorner(), node.print());
      context.incIndentLevel();
      context.setShowCorner(true);
      process(node.getChildren().get(0), context);
      return null;
    }

    @Override
    public Void visitOffset(OffsetNode node, PrinterContext context) {
      print(context.getIndentLevel(), context.isShowCorner(), node.print());
      context.incIndentLevel();
      context.setShowCorner(true);
      process(node.getChild(), context);
      return null;
    }

    @Override
    public Void visitRowBasedSeriesAggregate(AggregateNode node, PrinterContext context) {
      return visitPlan(node, context);
    }

    @Override
    public Void visitSort(SortNode node, PrinterContext context) {
      print(context.getIndentLevel(), context.isShowCorner(), node.print());
      context.incIndentLevel();
      context.setShowCorner(true);
      process(node.getChildren().get(0), context);
      return null;
    }

    @Override
    public Void visitTimeJoin(TimeJoinNode node, PrinterContext context) {
      print(context.getIndentLevel(), context.isShowCorner(), node.print());
      context.incIndentLevel();
      context.setShowCorner(true);
      for (int i = 0; i < node.getChildren().size(); i++) {
        if (i > 0) {
          context.setShowCorner(false);
        }
        process(node.getChildren().get(i), context);
      }
      return null;
    }

    private void print(Integer indentLevel, boolean showCorner, Pair<String, List<String>> value) {
      stringBuilder.append(repeatIndent(indentLevel));
      if (indentLevel > 0) {
        stringBuilder.append(showCorner ? CORNER : INDENT);
      }
      stringBuilder.append(value.left).append('\n');
      for (String attribute : value.right) {
        stringBuilder.append(repeatIndent(indentLevel + 1));
        stringBuilder.append(LINE).append(INDENT);
        stringBuilder.append(attribute).append('\n');
      }
    }

    private String repeatIndent(int indentLevel) {
      if (indentLevel < 2) {
        return "";
      }
      StringBuilder res = new StringBuilder();
      for (int i = 0; i < indentLevel - 1; i++) {
        res.append(INDENT);
      }
      return res.toString();
    }

    public String getOutput() {
      return stringBuilder.toString();
    }
  }

  private static class PrinterContext {
    private int indentLevel;
    private boolean showCorner;

    public PrinterContext(int indentLevel, boolean showCorner) {
      this.indentLevel = indentLevel;
      this.showCorner = showCorner;
    }

    public int getIndentLevel() {
      return indentLevel;
    }

    public void incIndentLevel() {
      indentLevel++;
    }

    public boolean isShowCorner() {
      return showCorner;
    }

    public void setShowCorner(boolean showCorner) {
      this.showCorner = showCorner;
    }
  }
}
