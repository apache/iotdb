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

package org.apache.iotdb.db.queryengine.plan.relational.planner;

import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanVisitor;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.DeviceTableScanNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.FilterNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.JoinNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.OutputNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.ProjectNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.SortNode;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.ArithmeticBinaryExpression;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.ArithmeticUnaryExpression;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.AstVisitor;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.BetweenPredicate;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.ComparisonExpression;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.Expression;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.FunctionCall;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.InPredicate;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.Literal;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.LogicalExpression;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.Node;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.NotExpression;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.SymbolReference;

import org.apache.tsfile.utils.RamUsageEstimator;

import java.util.IdentityHashMap;

import static java.util.Objects.requireNonNull;

public final class PlanMemoryEstimator {
  /** estimate the memory usage (in bytes) of the entire plan tree */
  public static long estimatePlan(final PlanNode root) {
    requireNonNull(root, "root is null");
    Seen seen = new Seen();
    return new PlanSizer(seen).process(root, null);
  }

  public static long estimateExpression(Expression e) {
    if (e == null) return 0L;
    Seen seen = new Seen();
    ExpressionSizer sz = new ExpressionSizer(seen);
    return sz.process(e, null);
  }

  private static final class Seen {
    private final IdentityHashMap<Object, Boolean> visited = new IdentityHashMap<>();

    boolean mark(Object o) {
      if (o == null) return false;
      // TRUE indicates the first occurrence and should be counted
      return visited.put(o, Boolean.TRUE) == null;
    }
  }

  /* ========================== Expression Sizer =========================== */

  private static final class ExpressionSizer extends AstVisitor<Long, Void> {
    private final Seen seen;

    ExpressionSizer(Seen seen) {
      this.seen = seen;
    }

    private long sizeOfExpr(Expression e) {
      if (e == null || !seen.mark(e)) return 0L;
      long size = RamUsageEstimator.shallowSizeOfInstance(e.getClass());
      return size + process(e, null);
    }

    @Override
    protected Long visitNode(Node node, Void ctx) {
      return node == null || !seen.mark(node) ? 0L : RamUsageEstimator.sizeOfObject(node);
    }

    @Override
    protected Long visitExpression(Expression node, Void ctx) {
      return 0L;
    }

    @Override
    protected Long visitLiteral(Literal node, Void ctx) {
      return node == null || !seen.mark(node) ? 0L : RamUsageEstimator.sizeOfObject(node);
    }

    @Override
    protected Long visitSymbolReference(SymbolReference node, Void ctx) {
      if (node == null || !seen.mark(node)) return 0L;
      long size = RamUsageEstimator.shallowSizeOfInstance(node.getClass());
      size += RamUsageEstimator.sizeOf(node.getName());
      return size;
    }

    @Override
    protected Long visitLogicalExpression(LogicalExpression node, Void ctx) {
      if (node == null || !seen.mark(node)) return 0L;
      long size = RamUsageEstimator.shallowSizeOfInstance(node.getClass());
      if (node.getTerms() != null) {
        for (Expression e : node.getTerms()) size += sizeOfExpr(e);
      }
      return size;
    }

    @Override
    protected Long visitComparisonExpression(ComparisonExpression node, Void ctx) {
      if (node == null || !seen.mark(node)) return 0L;
      long size = RamUsageEstimator.shallowSizeOfInstance(node.getClass());
      size += sizeOfExpr(node.getLeft());
      size += sizeOfExpr(node.getRight());
      return size;
    }

    @Override
    protected Long visitFunctionCall(FunctionCall node, Void ctx) {
      if (node == null || !seen.mark(node)) return 0L;
      long size = RamUsageEstimator.shallowSizeOfInstance(node.getClass());
      size += RamUsageEstimator.sizeOfObject(node.getName());
      if (node.getArguments() != null) {
        for (Expression e : node.getArguments()) size += sizeOfExpr(e);
      }
      return size;
    }

    @Override
    protected Long visitBetweenPredicate(BetweenPredicate node, Void ctx) {
      if (node == null || !seen.mark(node)) return 0L;
      long size = RamUsageEstimator.shallowSizeOfInstance(node.getClass());
      size += sizeOfExpr(node.getValue());
      size += sizeOfExpr(node.getMin());
      size += sizeOfExpr(node.getMax());
      return size;
    }

    @Override
    protected Long visitInPredicate(InPredicate node, Void ctx) {
      if (node == null || !seen.mark(node)) return 0L;
      long size = RamUsageEstimator.shallowSizeOfInstance(node.getClass());
      size += sizeOfExpr(node.getValue());
      if (node.getValueList() != null) size += sizeOfExpr(node.getValueList());
      return size;
    }

    @Override
    protected Long visitNotExpression(NotExpression node, Void ctx) {
      return node == null || !seen.mark(node) ? 0L : sizeOfExpr(node.getValue());
    }

    @Override
    protected Long visitArithmeticBinary(ArithmeticBinaryExpression node, Void ctx) {
      if (node == null || !seen.mark(node)) return 0L;
      long size = RamUsageEstimator.shallowSizeOfInstance(node.getClass());
      size += sizeOfExpr(node.getLeft());
      size += sizeOfExpr(node.getRight());
      return size;
    }

    @Override
    protected Long visitArithmeticUnary(ArithmeticUnaryExpression node, Void ctx) {
      return node == null || !seen.mark(node) ? 0L : sizeOfExpr(node.getValue());
    }
  }

  /* ============================ Plan Sizer ============================== */

  private static final class PlanSizer extends PlanVisitor<Long, Void> {
    private final Seen seen;
    private final ExpressionSizer exprSizer;

    PlanSizer(Seen seen) {
      this.seen = seen;
      this.exprSizer = new ExpressionSizer(seen);
    }

    private long sizeOfPlan(PlanNode n) {
      if (n == null || !seen.mark(n)) return 0L;
      long size = RamUsageEstimator.shallowSizeOfInstance(n.getClass());
      if (n.getChildren() != null) {
        for (PlanNode c : n.getChildren()) size += sizeOfPlan(c);
      }
      size += RamUsageEstimator.sizeOfObject(n.getPlanNodeId());
      return size;
    }

    private long sizeOfExpr(Expression e) {
      return exprSizer.process(e);
    }

    @Override
    public Long visitPlan(PlanNode node, Void ctx) {
      return sizeOfPlan(node);
    }

    @Override
    public Long visitOutput(OutputNode node, Void ctx) {
      if (node == null || !seen.mark(node)) return 0L;
      long size = sizeOfPlan(node);
      if (node.getOutputSymbols() != null)
        for (Object sym : node.getOutputSymbols()) size += RamUsageEstimator.sizeOfObject(sym);
      return size;
    }

    @Override
    public Long visitSort(SortNode node, Void ctx) {
      if (node == null || !seen.mark(node)) return 0L;
      long size = sizeOfPlan(node);
      if (node.getOrderingScheme() != null && seen.mark(node.getOrderingScheme())) {
        size += RamUsageEstimator.shallowSizeOfInstance(node.getOrderingScheme().getClass());
        if (node.getOrderingScheme().getOrderBy() != null)
          for (Object sym : node.getOrderingScheme().getOrderBy())
            size += RamUsageEstimator.sizeOfObject(sym);
        if (node.getOrderingScheme().getOrderings() != null)
          size += RamUsageEstimator.sizeOfMap(node.getOrderingScheme().getOrderings());
      }
      return size;
    }

    @Override
    public Long visitFilter(FilterNode node, Void ctx) {
      if (node == null || !seen.mark(node)) return 0L;
      long size = sizeOfPlan(node);
      size += sizeOfExpr(node.getPredicate());
      return size;
    }

    @Override
    public Long visitProject(ProjectNode node, Void ctx) {
      if (node == null || !seen.mark(node)) return 0L;
      long size = sizeOfPlan(node);
      Assignments assignments = node.getAssignments();
      if (assignments != null && seen.mark(assignments)) {
        size += RamUsageEstimator.shallowSizeOfInstance(assignments.getClass());
        size += RamUsageEstimator.sizeOfMap(assignments.getMap());
      }
      return size;
    }

    @Override
    public Long visitJoin(JoinNode node, Void ctx) {
      if (node == null || !seen.mark(node)) return 0L;
      long size = sizeOfPlan(node);

      if (node.getCriteria() != null) {
        for (JoinNode.EquiJoinClause c : node.getCriteria()) {
          if (c != null && seen.mark(c)) {
            size += RamUsageEstimator.shallowSizeOfInstance(c.getClass());
            size += RamUsageEstimator.sizeOfObject(c.getLeft());
            size += RamUsageEstimator.sizeOfObject(c.getRight());
          }
        }
      }

      if (node.getAsofCriteria().isPresent()) {
        JoinNode.AsofJoinClause ac = node.getAsofCriteria().get();
        if (seen.mark(ac)) {
          size += RamUsageEstimator.shallowSizeOfInstance(ac.getClass());
          size += RamUsageEstimator.sizeOfObject(ac.getLeft());
          size += RamUsageEstimator.sizeOfObject(ac.getRight());
        }
      }

      if (node.getLeftOutputSymbols() != null)
        for (Object s : node.getLeftOutputSymbols()) size += RamUsageEstimator.sizeOfObject(s);
      if (node.getRightOutputSymbols() != null)
        for (Object s : node.getRightOutputSymbols()) size += RamUsageEstimator.sizeOfObject(s);

      if (node.getFilter().isPresent()) {
        Expression f = node.getFilter().get();
        size += sizeOfExpr(f);
      }

      if (node.isSpillable().isPresent()) {
        Boolean b = node.isSpillable().get();
        if (seen.mark(b)) {
          size += RamUsageEstimator.sizeOfObject(b);
        }
      }

      return size;
    }

    @Override
    public Long visitDeviceTableScan(DeviceTableScanNode node, Void ctx) {
      if (node == null || !seen.mark(node)) return 0L;
      long size = sizeOfPlan(node);

      size += RamUsageEstimator.sizeOfObject(node.getQualifiedObjectName());
      if (node.getOutputSymbols() != null)
        for (Object s : node.getOutputSymbols()) size += RamUsageEstimator.sizeOfObject(s);
      if (node.getAssignments() != null) size += RamUsageEstimator.sizeOfMap(node.getAssignments());
      size += sizeOfExpr(node.getPushDownPredicate());
      size += RamUsageEstimator.sizeOfObject(node.getRegionReplicaSet());

      if (node.getDeviceEntries() != null)
        for (Object d : node.getDeviceEntries()) size += RamUsageEstimator.sizeOfObject(d);
      if (node.getTagAndAttributeIndexMap() != null)
        size += RamUsageEstimator.sizeOfMap(node.getTagAndAttributeIndexMap());
      if (node.getTimePredicate().isPresent()) {
        Expression tp = node.getTimePredicate().get();
        size += sizeOfExpr(tp);
      }
      size += RamUsageEstimator.sizeOfObject(node.getTimeFilter());

      return size;
    }
  }
}
