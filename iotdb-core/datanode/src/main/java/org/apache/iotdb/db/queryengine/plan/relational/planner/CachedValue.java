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

import org.apache.iotdb.db.queryengine.common.QueryId;
import org.apache.iotdb.db.queryengine.common.header.DatasetHeader;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanVisitor;
import org.apache.iotdb.db.queryengine.plan.relational.metadata.ColumnSchema;
import org.apache.iotdb.db.queryengine.plan.relational.planner.iterative.GroupReference;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.AggregationNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.ApplyNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.CorrelatedJoinNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.DeviceTableScanNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.FilterNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.GroupNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.JoinNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.LimitNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.LinearFillNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.OffsetNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.OutputNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.PatternRecognitionNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.PreviousFillNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.ProjectNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.SemiJoinNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.SortNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.TableFunctionNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.TableFunctionProcessorNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.TopKNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.TreeDeviceViewScanNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.ValueFillNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.WindowNode;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.AstVisitor;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.ComparisonExpression;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.Expression;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.Literal;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.LogicalExpression;

import com.google.common.collect.ImmutableList;
import org.apache.tsfile.read.common.type.Type;
import org.apache.tsfile.utils.RamUsageEstimator;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

public class CachedValue {
  private static final int INSTANCE_SIZE =
      (int) RamUsageEstimator.shallowSizeOfInstance(CachedValue.class);

  PlanNode planNode;
  List<DeviceTableScanNode> scanNodes;

  DatasetHeader respHeader;
  HashMap<Symbol, Type> symbolMap;
  int symbolNextId;

  // Used for indexScan to fetch device
  List<List<Expression>> metadataExpressionLists;
  List<List<String>> attributeColumnsLists;
  List<Map<Symbol, ColumnSchema>> assignmentsLists;

  List<Literal> literalReference;

  long estimatedMemoryUsage;

  public CachedValue(
      PlanNode planNode,
      List<DeviceTableScanNode> scanNodes,
      List<Literal> literalReference,
      DatasetHeader header,
      HashMap<Symbol, Type> symbolMap,
      int symbolNextId,
      List<List<Expression>> metadataExpressionLists,
      List<List<String>> attributeColumnsLists,
      List<Map<Symbol, ColumnSchema>> assignmentsLists) {
    this.planNode = planNode;
    this.scanNodes = scanNodes;
    this.respHeader = header;
    this.symbolMap = symbolMap;
    this.symbolNextId = symbolNextId;
    this.metadataExpressionLists = metadataExpressionLists;
    this.attributeColumnsLists = attributeColumnsLists;
    this.assignmentsLists = assignmentsLists;
    this.literalReference = literalReference;
    this.estimatedMemoryUsage = estimateMemoryUsage();
  }

  public DatasetHeader getRespHeader() {
    return respHeader;
  }

  public PlanNode getPlanNode() {
    return planNode;
  }

  public List<DeviceTableScanNode> getScanNodes() {
    return scanNodes;
  }

  public HashMap<Symbol, Type> getSymbolMap() {
    return symbolMap;
  }

  public int getSymbolNextId() {
    return symbolNextId;
  }

  public List<List<Expression>> getMetadataExpressionLists() {
    return metadataExpressionLists;
  }

  public List<List<String>> getAttributeColumnsLists() {
    return attributeColumnsLists;
  }

  public List<Map<Symbol, ColumnSchema>> getAssignmentsLists() {
    return assignmentsLists;
  }

  public List<Literal> getLiteralReference() {
    return literalReference;
  }

  /** Clone a new planNode using the new literal list */
  public static PlanNode clonePlanWithNewLiterals(PlanNode node, ClonerContext context) {
    return node.accept(new PlanNodeCloner(), context);
  }

  /** Clone new metadataExpressions using the new literal list */
  public static List<Expression> cloneMetadataExpressions(
      List<Expression> metadataExpressionList, List<Literal> newLiterals) {
    if (metadataExpressionList == null) {
      return null;
    }
    List<Expression> clonedList = new ArrayList<>(metadataExpressionList.size());
    ExpressionCloner exprCloner = new ExpressionCloner();
    for (Expression expr : metadataExpressionList) {
      clonedList.add(expr.accept(exprCloner, newLiterals));
    }
    return clonedList;
  }

  /**
   * ExpressionCloner is responsible for deep cloning SQL Expression trees. It replaces Literal
   * nodes.
   */
  private static class ExpressionCloner extends AstVisitor<Expression, List<Literal>> {
    @Override
    protected Expression visitExpression(Expression node, List<Literal> context) {
      // Default case, just return the node itself
      return node;
    }

    @Override
    protected Expression visitLiteral(Literal node, List<Literal> context) {
      int idx = node.getLiteralIndex();
      if (idx >= 0 && idx < context.size()) {
        return context.get(idx);
      }
      return node;
    }

    @Override
    protected Expression visitComparisonExpression(
        ComparisonExpression node, List<Literal> context) {
      return new ComparisonExpression(
          node.getOperator(),
          node.getLeft().accept(this, context),
          node.getRight().accept(this, context));
    }

    @Override
    protected Expression visitLogicalExpression(LogicalExpression node, List<Literal> context) {
      List<Expression> newTerms = new ArrayList<>();
      for (Expression term : node.getTerms()) {
        newTerms.add(term.accept(this, context));
      }
      return new LogicalExpression(node.getOperator(), newTerms);
    }

    // FunctionCall, Between, InPredicate, etc
  }

  private static class PlanNodeCloner extends PlanVisitor<PlanNode, ClonerContext> {
    private final ExpressionCloner exprCloner = new ExpressionCloner();

    @Override
    public PlanNode visitPlan(PlanNode node, ClonerContext context) {
      // Default case, just return the node itself
      // return node;
      throw new UnsupportedOperationException(
          "visitNode of Node {" + node.getClass() + "} is not supported in PlanNodeCloner");
    }

    @Override
    public PlanNode visitOutput(OutputNode node, ClonerContext context) {
      PlanNode newChild = node.getChild().accept(this, context);
      OutputNode newNode = (OutputNode) node.clone();
      newNode.setChild(newChild);
      return newNode;
    }

    @Override
    public PlanNode visitSort(SortNode node, ClonerContext context) {
      PlanNode newChild = node.getChild().accept(this, context);

      return new SortNode(
          context.getQueryId().genPlanNodeId(),
          newChild,
          node.getOrderingScheme(),
          node.isPartial(),
          node.isOrderByAllIdsAndTime());
    }

    @Override
    public PlanNode visitFilter(FilterNode node, ClonerContext context) {
      Expression newPredicate = node.getPredicate().accept(exprCloner, context.getNewLiterals());
      PlanNode newChild = node.getChild().accept(this, context);
      return new FilterNode(context.getQueryId().genPlanNodeId(), newChild, newPredicate);
    }

    @Override
    public PlanNode visitProject(ProjectNode node, ClonerContext context) {
      PlanNode newChild = node.getChild().accept(this, context);

      Map<Symbol, Expression> newAssignmentsMap = new HashMap<>();
      for (Map.Entry<Symbol, Expression> entry : node.getAssignments().entrySet()) {
        Expression clonedExpr =
            entry.getValue().accept(new ExpressionCloner(), context.getNewLiterals());
        newAssignmentsMap.put(entry.getKey(), clonedExpr);
      }
      Assignments newAssignments = new Assignments(newAssignmentsMap);

      return new ProjectNode(context.getQueryId().genPlanNodeId(), newChild, newAssignments);
    }

    @Override
    public PlanNode visitJoin(JoinNode node, ClonerContext context) {
      PlanNode newLeft = node.getLeftChild().accept(this, context);
      PlanNode newRight = node.getRightChild().accept(this, context);

      Optional<Expression> newFilter =
          node.getFilter().map(expr -> expr.accept(exprCloner, context.getNewLiterals()));

      return new JoinNode(
          context.getQueryId().genPlanNodeId(),
          node.getJoinType(),
          newLeft,
          newRight,
          ImmutableList.copyOf(node.getCriteria()),
          node.getAsofCriteria(),
          ImmutableList.copyOf(node.getLeftOutputSymbols()),
          ImmutableList.copyOf(node.getRightOutputSymbols()),
          newFilter,
          node.isSpillable());
    }

    @Override
    public PlanNode visitDeviceTableScan(DeviceTableScanNode node, ClonerContext context) {
      // deep copy pushDownPredicate
      Expression newPredicate =
          node.getPushDownPredicate() == null
              ? null
              : node.getPushDownPredicate()
                  .accept(new ExpressionCloner(), context.getNewLiterals());

      // deep copy timePredicate
      Expression newTimePredicate =
          node.getTimePredicate()
              .map(tp -> tp.accept(new ExpressionCloner(), context.getNewLiterals()))
              .orElse(null);

      DeviceTableScanNode newNode =
          new DeviceTableScanNode(
              context.getQueryId().genPlanNodeId(),
              node.getQualifiedObjectName(),
              node.getOutputSymbols(),
              node.getAssignments(),
              node.getDeviceEntries(),
              node.getTagAndAttributeIndexMap(),
              node.getScanOrder(),
              newTimePredicate,
              newPredicate,
              node.getPushDownLimit(),
              node.getPushDownOffset(),
              node.isPushLimitToEachDevice(),
              node.containsNonAlignedDevice());

      return newNode;
    }

    @Override
    public PlanNode visitPatternRecognition(PatternRecognitionNode node, ClonerContext context) {
      PlanNode newChild = node.getChild().accept(this, context);
      PatternRecognitionNode newNode = (PatternRecognitionNode) node.clone();
      newNode.setChild(newChild);
      return newNode;
    }

    @Override
    public PlanNode visitAggregation(AggregationNode node, ClonerContext context) {
      PlanNode newChild = node.getChild().accept(this, context);
      AggregationNode newNode = (AggregationNode) node.clone();
      newNode.setChild(newChild);
      return newNode;
    }

    @Override
    public PlanNode visitWindowFunction(WindowNode node, ClonerContext context) {
      PlanNode newChild = node.getChild().accept(this, context);
      WindowNode newNode = (WindowNode) node.clone();
      newNode.setChild(newChild);
      return newNode;
    }

    @Override
    public PlanNode visitTableFunction(TableFunctionNode node, ClonerContext context) {
      List<PlanNode> newChildren = new ArrayList<>();
      for (PlanNode child : node.getChildren()) {
        newChildren.add(child.accept(this, context));
      }
      TableFunctionNode newNode = (TableFunctionNode) node.clone();
      newNode.setChildren(newChildren);
      return newNode;
    }

    @Override
    public PlanNode visitTableFunctionProcessor(
        TableFunctionProcessorNode node, ClonerContext context) {
      PlanNode newChild = node.getChild().accept(this, context);
      TableFunctionProcessorNode newNode = (TableFunctionProcessorNode) node.clone();
      newNode.setChild(newChild);
      return newNode;
    }

    @Override
    public PlanNode visitLimit(LimitNode node, ClonerContext context) {
      PlanNode newChild = node.getChild().accept(this, context);
      LimitNode newNode = (LimitNode) node.clone();
      newNode.setChild(newChild);
      return newNode;
    }

    @Override
    public PlanNode visitOffset(OffsetNode node, ClonerContext context) {
      PlanNode newChild = node.getChild().accept(this, context);
      OffsetNode newNode = (OffsetNode) node.clone();
      newNode.setChild(newChild);
      return newNode;
    }

    @Override
    public PlanNode visitApply(ApplyNode node, ClonerContext context) {
      PlanNode newLeft = node.getLeftChild().accept(this, context);
      PlanNode newRight = node.getRightChild().accept(this, context);
      ApplyNode newNode = (ApplyNode) node.clone();
      newNode.setLeftChild(newLeft);
      newNode.setRightChild(newRight);
      return newNode;
    }

    @Override
    public PlanNode visitCorrelatedJoin(CorrelatedJoinNode node, ClonerContext context) {
      PlanNode newLeft = node.getLeftChild().accept(this, context);
      PlanNode newRight = node.getRightChild().accept(this, context);
      CorrelatedJoinNode newNode = (CorrelatedJoinNode) node.clone();
      newNode.setLeftChild(newLeft);
      newNode.setRightChild(newRight);
      return newNode;
    }

    @Override
    public PlanNode visitLinearFill(LinearFillNode node, ClonerContext context) {
      PlanNode newChild = node.getChild().accept(this, context);
      LinearFillNode newNode = (LinearFillNode) node.clone();
      newNode.setChild(newChild);
      return newNode;
    }

    @Override
    public PlanNode visitPreviousFill(PreviousFillNode node, ClonerContext context) {
      PlanNode newChild = node.getChild().accept(this, context);
      PreviousFillNode newNode = (PreviousFillNode) node.clone();
      newNode.setChild(newChild);
      return newNode;
    }

    @Override
    public PlanNode visitValueFill(ValueFillNode node, ClonerContext context) {
      PlanNode newChild = node.getChild().accept(this, context);
      ValueFillNode newNode = (ValueFillNode) node.clone();
      newNode.setChild(newChild);
      return newNode;
    }

    @Override
    public PlanNode visitGroup(GroupNode node, ClonerContext context) {
      PlanNode newChild = node.getChild().accept(this, context);
      GroupNode newNode = (GroupNode) node.clone();
      newNode.setChild(newChild);
      return newNode;
    }

    @Override
    public PlanNode visitTopK(TopKNode node, ClonerContext context) {
      List<PlanNode> newChildren = new ArrayList<>();
      for (PlanNode child : node.getChildren()) {
        newChildren.add(child.accept(this, context));
      }
      TopKNode newNode = (TopKNode) node.clone();
      newNode.setChildren(newChildren);
      return newNode;
    }

    @Override
    public PlanNode visitSemiJoin(SemiJoinNode node, ClonerContext context) {
      PlanNode newLeft = node.getLeftChild().accept(this, context);
      PlanNode newRight = node.getRightChild().accept(this, context);
      SemiJoinNode newNode = (SemiJoinNode) node.clone();
      newNode.setLeftChild(newLeft);
      newNode.setRightChild(newRight);
      return newNode;
    }

    @Override
    public PlanNode visitGroupReference(GroupReference node, ClonerContext context) {
      GroupReference newNode = (GroupReference) node.clone();
      return newNode;
    }

    @Override
    public PlanNode visitTreeDeviceViewScan(TreeDeviceViewScanNode node, ClonerContext context) {
      TreeDeviceViewScanNode newNode = (TreeDeviceViewScanNode) node.clone();
      return newNode;
    }
  }

  public static List<DeviceTableScanNode> collectDeviceTableScanNodes(PlanNode root) {
    List<DeviceTableScanNode> list = new ArrayList<>();
    traverse(root, list);
    return list;
  }

  private static void traverse(PlanNode node, List<DeviceTableScanNode> list) {
    if (node instanceof DeviceTableScanNode) {
      list.add((DeviceTableScanNode) node);
    }
    for (PlanNode child : node.getChildren()) {
      traverse(child, list);
    }
  }

  public long estimateMemoryUsage() {
    long size = INSTANCE_SIZE;

    if (planNode != null) {
      size += PlanMemoryEstimator.estimatePlan(planNode); // 已内部处理 Seen
    }

    if (respHeader != null) {
      size += RamUsageEstimator.sizeOfObject(respHeader);
    }

    if (symbolMap != null) {
      size += RamUsageEstimator.sizeOfMap(symbolMap);
    }

    if (metadataExpressionLists != null) {
      for (List<Expression> list : metadataExpressionLists) {
        if (list != null) {
          for (Expression e : list) {
            if (e != null) {
              size += PlanMemoryEstimator.estimateExpression(e);
            }
          }
        }
      }
    }

    if (attributeColumnsLists != null) {
      for (List<String> list : attributeColumnsLists) {
        if (list != null) {
          for (String s : list) {
            if (s != null) {
              size += RamUsageEstimator.sizeOf(s);
            }
          }
        }
      }
    }

    // scanNodes, assignmentsLists, and literalreferences all store references to a small part of
    // the planNode, so there is no need to repeat the calculation

    return size;
  }

  public static class ClonerContext {
    private final QueryId queryId;
    private final List<Literal> newLiterals;

    public ClonerContext(QueryId queryId, List<Literal> newLiterals) {
      this.queryId = queryId;
      this.newLiterals = newLiterals;
    }

    public QueryId getQueryId() {
      return queryId;
    }

    public List<Literal> getNewLiterals() {
      return newLiterals;
    }
  }
}
