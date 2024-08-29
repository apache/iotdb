/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.iotdb.db.queryengine.plan.relational.planner.optimizations;

import org.apache.iotdb.db.queryengine.common.QueryId;
import org.apache.iotdb.db.queryengine.common.SessionInfo;
import org.apache.iotdb.db.queryengine.plan.expression.leaf.TimestampOperand;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanVisitor;
import org.apache.iotdb.db.queryengine.plan.relational.metadata.Metadata;
import org.apache.iotdb.db.queryengine.plan.relational.metadata.ResolvedFunction;
import org.apache.iotdb.db.queryengine.plan.relational.metadata.TableBuiltinAggregationFunction;
import org.apache.iotdb.db.queryengine.plan.relational.planner.Symbol;
import org.apache.iotdb.db.queryengine.plan.relational.planner.SymbolAllocator;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.AggregationNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.AggregationTableScanNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.ProjectNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.TableScanNode;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.Expression;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.FunctionCall;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.Query;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.SymbolReference;
import org.apache.iotdb.db.queryengine.transformation.dag.column.unary.scalar.TableBuiltinScalarFunction;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import org.apache.tsfile.read.common.type.RowType;
import org.apache.tsfile.read.common.type.Type;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkState;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.node.AggregationNode.Step.FINAL;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.node.AggregationNode.Step.PARTIAL;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.node.AggregationTableScanNode.combineAggregationAndTableScan;

/**
 * <b>Optimization phase:</b> Logical plan planning.
 *
 * <p>The Aggregation may be pushed down to the TableScanNode, so that we can make use of
 * statistics.
 *
 * <p>Attention: This optimizer depends on {@link UnaliasSymbolReferences}.
 */
public class PushAggregationIntoTableScan implements PlanOptimizer {

  @Override
  public PlanNode optimize(PlanNode plan, PlanOptimizer.Context context) {
    if (!(context.getAnalysis().getStatement() instanceof Query)
        || !context.getAnalysis().hasAggregates()) {
      return plan;
    }

    return plan.accept(
        new Rewriter(),
        new Context(
            context.getQueryContext().getQueryId(),
            context.getMetadata(),
            context.sessionInfo(),
            context.symbolAllocator()));
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
      TableScanNode tableScanNode = null;
      ProjectNode projectNode = null;
      if (node.getChild() instanceof TableScanNode) {
        tableScanNode = (TableScanNode) node.getChild();
      }
      if (node.getChild() instanceof ProjectNode) {
        projectNode = (ProjectNode) node.getChild();
        if (projectNode.getChild() instanceof TableScanNode) {
          tableScanNode = (TableScanNode) projectNode.getChild();
        }
      }
      if (tableScanNode == null
          || tableScanNode.getPushDownPredicate() != null) { // no need to optimize
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
        node = split(node, context);
        AggregationTableScanNode aggregationTableScanNode =
            combineAggregationAndTableScan(
                context.queryId.genPlanNodeId(),
                (AggregationNode) node.getChild(),
                projectNode,
                tableScanNode);
        node.setChild(aggregationTableScanNode);
        return node;
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
        TableScanNode tableScanNode,
        SessionInfo session,
        Metadata metadata) {
      boolean hasProject = projectNode != null;
      Map<Symbol, Expression> assignments =
          hasProject ? projectNode.getAssignments().getMap() : null;
      // calculate Function part
      for (AggregationNode.Aggregation aggregation : values) {
        // if the function cannot make use of Statistics, we don't push down
        if (!metadata.canUseStatistics(
            aggregation.getResolvedFunction().getSignature().getName())) {
          return PushDownLevel.NOOP;
        }

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
      if (groupingKeys.isEmpty()) {
        // GlobalAggregation
        return PushDownLevel.COMPLETE;
      }

      List<FunctionCall> dateBinFunctionsOfTime = new ArrayList<>();
      if (groupingKeys.stream()
              .anyMatch(
                  groupingKey ->
                      hasProject
                              && !(assignments.get(groupingKey) instanceof SymbolReference
                                  || isDateBinFunctionOfTime(
                                      assignments.get(groupingKey), dateBinFunctionsOfTime))
                          || tableScanNode.isMeasurementColumn(groupingKey))
          || dateBinFunctionsOfTime.size() > 1) {
        // If expr except date_bin(time) or Measurement column appears in groupingKeys, we don't
        // push down;
        // Attention: Now we also don't push down if there are more than one date_bin function
        // appear in groupingKeys.

        return PushDownLevel.NOOP;
      } else if (ImmutableSet.copyOf(groupingKeys)
          .containsAll(tableScanNode.getIdColumnsInTableStore(metadata, session))) {
        // If all ID columns appear in groupingKeys and no Measurement column appears, we can push
        // down completely.
        return PushDownLevel.COMPLETE;
      } else {
        return PushDownLevel.PARTIAL;
      }
    }

    private boolean isDateBinFunctionOfTime(
        Expression expression, List<FunctionCall> dateBinFunctionsOfTime) {
      if (expression instanceof FunctionCall) {
        FunctionCall function = (FunctionCall) expression;
        if (TableBuiltinScalarFunction.DATE_BIN
                .getFunctionName()
                .equals(function.getName().toString())
            && function.getArguments().get(2) instanceof SymbolReference
            && TimestampOperand.TIMESTAMP_EXPRESSION_STRING.equalsIgnoreCase(
                ((SymbolReference) function.getArguments().get(2)).getName())) {
          dateBinFunctionsOfTime.add(function);
          return true;
        }
      }
      return false;
    }
  }

  private static AggregationNode split(AggregationNode node, Context context) {
    // otherwise, add a partial and final with an exchange in between
    Map<Symbol, AggregationNode.Aggregation> intermediateAggregation = new HashMap<>();
    Map<Symbol, AggregationNode.Aggregation> finalAggregation = new HashMap<>();
    for (Map.Entry<Symbol, AggregationNode.Aggregation> entry : node.getAggregations().entrySet()) {
      AggregationNode.Aggregation originalAggregation = entry.getValue();
      ResolvedFunction resolvedFunction = originalAggregation.getResolvedFunction();
      List<Type> intermediateTypes =
          TableBuiltinAggregationFunction.getIntermediateTypes(
              resolvedFunction.getSignature().getName(),
              resolvedFunction.getSignature().getReturnType());
      Type intermediateType =
          intermediateTypes.size() == 1
              ? intermediateTypes.get(0)
              : RowType.anonymous(intermediateTypes);
      Symbol intermediateSymbol =
          context.symbolAllocator.newSymbol(
              resolvedFunction.getSignature().getName(), intermediateType);

      checkState(
          !originalAggregation.getOrderingScheme().isPresent(),
          "Aggregate with ORDER BY does not support partial aggregation");
      intermediateAggregation.put(
          intermediateSymbol,
          new AggregationNode.Aggregation(
              resolvedFunction,
              originalAggregation.getArguments(),
              originalAggregation.isDistinct(),
              originalAggregation.getFilter(),
              originalAggregation.getOrderingScheme(),
              originalAggregation.getMask()));

      // rewrite final aggregation in terms of intermediate function
      finalAggregation.put(
          entry.getKey(),
          new AggregationNode.Aggregation(
              resolvedFunction,
              ImmutableList.of(intermediateSymbol.toSymbolReference()),
              false,
              Optional.empty(),
              Optional.empty(),
              Optional.empty()));
    }

    AggregationNode partial =
        new AggregationNode(
            context.queryId.genPlanNodeId(),
            node.getChild(),
            intermediateAggregation,
            node.getGroupingSets(),
            // preGroupedSymbols reflect properties of the input. Splitting the aggregation and
            // pushing partial aggregation
            // through the exchange may or may not preserve these properties. Hence, it is safest to
            // drop preGroupedSymbols here.
            node.getPreGroupedSymbols(),
            PARTIAL,
            node.getHashSymbol(),
            node.getGroupIdSymbol());

    return new AggregationNode(
        node.getPlanNodeId(),
        partial,
        finalAggregation,
        node.getGroupingSets(),
        // preGroupedSymbols reflect properties of the input. Splitting the aggregation and pushing
        // partial aggregation
        // through the exchange may or may not preserve these properties. Hence, it is safest to
        // drop preGroupedSymbols here.
        node.getPreGroupedSymbols(),
        FINAL,
        node.getHashSymbol(),
        node.getGroupIdSymbol());
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
