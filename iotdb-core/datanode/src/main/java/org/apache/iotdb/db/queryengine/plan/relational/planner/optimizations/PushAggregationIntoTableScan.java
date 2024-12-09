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

package org.apache.iotdb.db.queryengine.plan.relational.planner.optimizations;

import org.apache.iotdb.commons.udf.builtin.relational.TableBuiltinScalarFunction;
import org.apache.iotdb.db.queryengine.common.QueryId;
import org.apache.iotdb.db.queryengine.common.SessionInfo;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanVisitor;
import org.apache.iotdb.db.queryengine.plan.relational.metadata.Metadata;
import org.apache.iotdb.db.queryengine.plan.relational.planner.Symbol;
import org.apache.iotdb.db.queryengine.plan.relational.planner.SymbolAllocator;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.AggregationNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.AggregationTableScanNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.DeviceTableScanNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.ProjectNode;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.Expression;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.FunctionCall;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.SymbolReference;

import com.google.common.collect.ImmutableSet;
import org.apache.tsfile.utils.Pair;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import static org.apache.iotdb.db.queryengine.plan.relational.planner.node.AggregationTableScanNode.combineAggregationAndTableScan;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.optimizations.Util.split;

/**
 * <b>Optimization phase:</b> Logical plan planning.
 *
 * <p>The Aggregation may be pushed down to the DeviceTableScanNode, so that we can make use of
 * statistics.
 *
 * <p>Attention: This optimizer depends on {@link UnaliasSymbolReferences}.
 */
public class PushAggregationIntoTableScan implements PlanOptimizer {

  @Override
  public PlanNode optimize(PlanNode plan, PlanOptimizer.Context context) {
    if (!(context.getAnalysis().isQuery()) || !context.getAnalysis().containsAggregationQuery()) {
      return plan;
    }

    return plan.accept(
        new Rewriter(),
        new Context(
            context.getQueryContext().getQueryId(),
            context.getMetadata(),
            context.sessionInfo(),
            context.getSymbolAllocator()));
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
    public PlanNode visitAggregation(AggregationNode node, Context context) {
      PlanNode child = node.getChild().accept(this, context);
      node = (AggregationNode) node.clone();
      node.setChild(child);
      DeviceTableScanNode tableScanNode = null;
      ProjectNode projectNode = null;
      if (child instanceof DeviceTableScanNode) {
        tableScanNode = (DeviceTableScanNode) child;
      }
      if (child instanceof ProjectNode) {
        projectNode = (ProjectNode) child;
        if (projectNode.getChild() instanceof DeviceTableScanNode) {
          tableScanNode = (DeviceTableScanNode) projectNode.getChild();
        }
      }

      // only optimize AggregationNode with raw DeviceTableScanNode
      if (tableScanNode == null
          || tableScanNode instanceof AggregationTableScanNode) { // no need to optimize
        return node;
      }

      PushDownLevel pushDownLevel =
          calculatePushDownLevel(
              node.getAggregations().values(),
              node.getGroupingKeys(),
              projectNode,
              tableScanNode,
              context.session,
              context.metadata);
      if (pushDownLevel == PushDownLevel.NOOP) { // no push-down
        return node;
      } else if (pushDownLevel == PushDownLevel.PARTIAL) { // partial push-down
        Pair<AggregationNode, AggregationNode> result =
            split(node, context.symbolAllocator, context.queryId);
        AggregationTableScanNode aggregationTableScanNode =
            combineAggregationAndTableScan(
                context.queryId.genPlanNodeId(), result.right, projectNode, tableScanNode);
        result.left.setChild(aggregationTableScanNode);
        return result.left;
      } else { // complete push-down
        return combineAggregationAndTableScan(
            context.queryId.genPlanNodeId(), node, projectNode, tableScanNode);
      }
    }

    /** Calculate the level of push-down, and extract the projection of date_bin(time). */
    private PushDownLevel calculatePushDownLevel(
        Collection<AggregationNode.Aggregation> values,
        List<Symbol> groupingKeys,
        ProjectNode projectNode,
        DeviceTableScanNode tableScanNode,
        SessionInfo session,
        Metadata metadata) {
      boolean hasProject = projectNode != null;
      Map<Symbol, Expression> assignments =
          hasProject ? projectNode.getAssignments().getMap() : null;
      // calculate Function part
      for (AggregationNode.Aggregation aggregation : values) {
        // all the functions can be pre-agg in AggTableScanNode

        // if expr appears in arguments of Aggregation, we don't push down
        if (hasProject
            && aggregation.getArguments().stream()
                .anyMatch(
                    argument ->
                        !(assignments.get(Symbol.from(argument)) instanceof SymbolReference))) {
          return PushDownLevel.NOOP;
        }
      }

      // calculate DataSet part
      boolean singleDeviceEntry = tableScanNode.getDeviceEntries().size() < 2;
      if (groupingKeys.isEmpty()) {
        // GlobalAggregation
        if (singleDeviceEntry) {
          return PushDownLevel.COMPLETE;
        } else {
          // We need to two-stage Aggregation to combine Aggregation result of different DeviceEntry
          return PushDownLevel.PARTIAL;
        }
      }

      List<FunctionCall> dateBinFunctionsOfTime = new ArrayList<>();
      if (groupingKeys.stream()
              .anyMatch(
                  groupingKey ->
                      hasProject
                              && !(assignments.get(groupingKey) instanceof SymbolReference
                                  || isDateBinFunctionOfTime(
                                      assignments.get(groupingKey),
                                      dateBinFunctionsOfTime,
                                      tableScanNode))
                          || tableScanNode.isMeasurementOrTimeColumn(groupingKey))
          || dateBinFunctionsOfTime.size() > 1) {
        // If expr except date_bin(time), Measurement column, or Time column appears in
        // groupingKeys, we don't push down;
        // Attention: Now we also don't push down if there are more than one date_bin function
        // appear in groupingKeys.

        return PushDownLevel.NOOP;
      } else if (singleDeviceEntry
          || ImmutableSet.copyOf(groupingKeys)
              .containsAll(tableScanNode.getIdColumnsInTableStore(metadata, session))) {
        // If all ID columns appear in groupingKeys and no Measurement column appears, we can push
        // down completely.
        return PushDownLevel.COMPLETE;
      } else {
        return PushDownLevel.PARTIAL;
      }
    }

    private boolean isDateBinFunctionOfTime(
        Expression expression,
        List<FunctionCall> dateBinFunctionsOfTime,
        DeviceTableScanNode tableScanNode) {
      if (expression instanceof FunctionCall) {
        FunctionCall function = (FunctionCall) expression;
        if (TableBuiltinScalarFunction.DATE_BIN
                .getFunctionName()
                .equals(function.getName().toString())
            && function.getArguments().get(2) instanceof SymbolReference
            && tableScanNode.isTimeColumn(Symbol.from(function.getArguments().get(2)))) {
          dateBinFunctionsOfTime.add(function);
          return true;
        }
      }
      return false;
    }
  }

  private enum PushDownLevel {
    NOOP,
    PARTIAL,
    COMPLETE
  }

  private static class Context {
    private final QueryId queryId;
    private final Metadata metadata;
    private final SessionInfo session;
    private final SymbolAllocator symbolAllocator;

    public Context(
        QueryId queryId, Metadata metadata, SessionInfo session, SymbolAllocator symbolAllocator) {
      this.queryId = queryId;
      this.metadata = metadata;
      this.session = session;
      this.symbolAllocator = symbolAllocator;
    }
  }
}
