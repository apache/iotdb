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

import org.apache.iotdb.common.rpc.thrift.TRegionReplicaSet;
import org.apache.iotdb.common.rpc.thrift.TSeriesPartitionSlot;
import org.apache.iotdb.common.rpc.thrift.TTimePartitionSlot;
import org.apache.iotdb.commons.partition.DataPartition;
import org.apache.iotdb.commons.partition.DataPartitionQueryParam;
import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.queryengine.common.MPPQueryContext;
import org.apache.iotdb.db.queryengine.common.SessionInfo;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanVisitor;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.process.MultiChildProcessNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.process.SingleChildProcessNode;
import org.apache.iotdb.db.queryengine.plan.relational.analyzer.Analysis;
import org.apache.iotdb.db.queryengine.plan.relational.analyzer.predicate.PredicateCombineIntoTableScanChecker;
import org.apache.iotdb.db.queryengine.plan.relational.analyzer.predicate.PredicatePushIntoMetadataChecker;
import org.apache.iotdb.db.queryengine.plan.relational.metadata.DeviceEntry;
import org.apache.iotdb.db.queryengine.plan.relational.metadata.Metadata;
import org.apache.iotdb.db.queryengine.plan.relational.planner.Symbol;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.FilterNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.TableScanNode;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.Expression;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.FunctionCall;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.LogicalExpression;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.Node;

import org.apache.tsfile.read.filter.basic.Filter;
import org.apache.tsfile.utils.Pair;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static org.apache.iotdb.commons.schema.table.column.TsTableColumnCategory.ATTRIBUTE;
import static org.apache.iotdb.commons.schema.table.column.TsTableColumnCategory.MEASUREMENT;
import static org.apache.iotdb.db.queryengine.plan.analyze.AnalyzeVisitor.getTimePartitionSlotList;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.ir.GlobalTimePredicateExtractVisitor.extractGlobalTimeFilter;

/**
 * After the optimized rule {@link SimplifyExpressions} finished, predicate expression in FilterNode
 * has been transformed to conjunctive normal forms(CNF).
 *
 * <p>1. In this class, we examine each expression in CNFs, determine how to use it, in metadata
 * query, or pushed down into ScanOperators, or it can only be used in FilterNode above with
 * TableScanNode.
 * <li>For metadata query expressions, it will be used in {@code tableIndexScan} method to generate
 *     the deviceEntries and DataPartition used for TableScanNode.
 * <li>For expressions which can be pushed into TableScanNode, we will execute {@code
 *     extractGlobalTimeFilter}, to extract the timePredicate and pushDownValuePredicate.
 * <li>Expression which can not be pushed down into TableScanNode, will be used in the FilterNode
 *     above of TableScanNode.
 *
 *     <p>Notice that, when aggregation, multi-table, join are introduced, this optimization rule
 *     need to be adapted.
 */
public class FilterScanCombine implements RelationalPlanOptimizer {

  private static final IoTDBConfig CONFIG = IoTDBDescriptor.getInstance().getConfig();

  @Override
  public PlanNode optimize(
      PlanNode planNode,
      Analysis analysis,
      Metadata metadata,
      SessionInfo sessionInfo,
      MPPQueryContext queryContext) {
    return planNode.accept(new Rewriter(queryContext, analysis, metadata), new RewriterContext());
  }

  private static class Rewriter extends PlanVisitor<PlanNode, RewriterContext> {
    private final MPPQueryContext queryContext;
    private final Analysis analysis;
    private final Metadata metadata;
    private Expression predicate;
    private boolean hasDiffInFilter = false;

    // used for metadata index scan
    private final List<Expression> metadataExpressions = new ArrayList<>();

    Rewriter(MPPQueryContext queryContext, Analysis analysis, Metadata metadata) {
      this.queryContext = queryContext;
      this.analysis = analysis;
      this.metadata = metadata;
    }

    @Override
    public PlanNode visitPlan(PlanNode node, RewriterContext context) {
      throw new IllegalArgumentException(
          String.format("Unexpected plan node: %s in FilterScanCombineRule", node));
    }

    @Override
    public PlanNode visitSingleChildProcess(SingleChildProcessNode node, RewriterContext context) {
      PlanNode rewrittenChild = node.getChild().accept(this, context);
      node.setChild(rewrittenChild);
      return node;
    }

    @Override
    public PlanNode visitMultiChildProcess(MultiChildProcessNode node, RewriterContext context) {
      List<PlanNode> rewrittenChildren = new ArrayList<>();
      for (PlanNode child : node.getChildren()) {
        rewrittenChildren.add(child.accept(this, context));
      }
      node.setChildren(rewrittenChildren);
      return node;
    }

    @Override
    public PlanNode visitFilter(FilterNode node, RewriterContext context) {

      if (node.getPredicate() != null) {

        // when exist diff function, predicate can not be pushed down into TableScanNode
        if (containsDiffFunction(node.getPredicate())) {
          hasDiffInFilter = true;
          node.getChild().accept(this, context);
          return node;
        }

        predicate = node.getPredicate();

        if (node.getChild() instanceof TableScanNode) {
          // child of FilterNode is TableScanNode, means FilterNode must get from where clause
          return combineFilterAndScan((TableScanNode) node.getChild());
        } else {
          // FilterNode may get from having or subquery
          return node.getChild().accept(this, context);
        }

      } else {
        throw new IllegalStateException(
            "Filter node has no predicate, node: " + node.getPlanNodeId());
      }
    }

    public PlanNode combineFilterAndScan(TableScanNode tableScanNode) {
      List<List<Expression>> splitPredicates = splitPredicate(tableScanNode);

      // exist expressions can push down to scan operator
      if (!splitPredicates.get(1).isEmpty()) {
        List<Expression> expressions = splitPredicates.get(1);
        Expression pushDownPredicate =
            expressions.size() == 1
                ? expressions.get(0)
                : new LogicalExpression(LogicalExpression.Operator.AND, expressions);

        // extract global time filter and set it to TableScanNode
        Pair<Expression, Boolean> resultPair = extractGlobalTimeFilter(pushDownPredicate);
        if (resultPair.left != null) {
          tableScanNode.setTimePredicate(resultPair.left);
        }
        if (resultPair.right) {
          tableScanNode.setPushDownPredicate(pushDownPredicate);
        }
      } else {
        tableScanNode.setPushDownPredicate(null);
      }

      tableMetadataIndexScan(tableScanNode);

      // exist expressions can not push down to scan operator
      if (!splitPredicates.get(2).isEmpty()) {
        List<Expression> expressions = splitPredicates.get(2);
        return new FilterNode(
            queryContext.getQueryId().genPlanNodeId(),
            tableScanNode,
            expressions.size() == 1
                ? expressions.get(0)
                : new LogicalExpression(LogicalExpression.Operator.AND, expressions));
      }

      return tableScanNode;
    }

    /**
     * splits predicate expression in table model into three parts, index 0 represents
     * metadataExpressions, index 1 represents expressionsCanPushDownToOperator, index 2 represents
     * expressionsCannotPushDownToOperator
     */
    private List<List<Expression>> splitPredicate(TableScanNode node) {

      Set<String> idOrAttributeColumnNames =
          node.getIdAndAttributeIndexMap().keySet().stream()
              .map(Symbol::getName)
              .collect(Collectors.toSet());

      Set<String> measurementColumnNames =
          node.getAssignments().entrySet().stream()
              .filter(e -> MEASUREMENT.equals(e.getValue().getColumnCategory()))
              .map(e -> e.getKey().getName())
              .collect(Collectors.toSet());
      measurementColumnNames.add("time");

      List<Expression> expressionsCanPushDown = new ArrayList<>();
      List<Expression> expressionsCannotPushDown = new ArrayList<>();

      if (predicate instanceof LogicalExpression
          && ((LogicalExpression) predicate).getOperator() == LogicalExpression.Operator.AND) {

        for (Expression expression : ((LogicalExpression) predicate).getTerms()) {
          if (PredicatePushIntoMetadataChecker.check(idOrAttributeColumnNames, expression)) {
            metadataExpressions.add(expression);
          } else if (PredicateCombineIntoTableScanChecker.check(
              measurementColumnNames, expression)) {
            expressionsCanPushDown.add(expression);
          } else {
            expressionsCannotPushDown.add(expression);
          }
        }

        return Arrays.asList(
            metadataExpressions, expressionsCanPushDown, expressionsCannotPushDown);
      }

      if (PredicatePushIntoMetadataChecker.check(idOrAttributeColumnNames, predicate)) {
        metadataExpressions.add(predicate);
      } else if (PredicateCombineIntoTableScanChecker.check(measurementColumnNames, predicate)) {
        expressionsCanPushDown.add(predicate);
      } else {
        expressionsCannotPushDown.add(predicate);
      }

      return Arrays.asList(metadataExpressions, expressionsCanPushDown, expressionsCannotPushDown);
    }

    @Override
    public PlanNode visitTableScan(TableScanNode node, RewriterContext context) {
      tableMetadataIndexScan(node);
      return node;
    }

    /** Get deviceEntries and DataPartition used in TableScan. */
    private void tableMetadataIndexScan(TableScanNode node) {
      List<String> attributeColumns =
          node.getOutputSymbols().stream()
              .filter(
                  symbol -> ATTRIBUTE.equals(node.getAssignments().get(symbol).getColumnCategory()))
              .map(Symbol::getName)
              .collect(Collectors.toList());
      List<DeviceEntry> deviceEntries =
          metadata.indexScan(node.getQualifiedObjectName(), metadataExpressions, attributeColumns);
      node.setDeviceEntries(deviceEntries);

      if (deviceEntries.isEmpty()) {
        analysis.setFinishQueryAfterAnalyze();
        analysis.setEmptyDataSource(true);
      } else {
        // TODO(beyyes) use table model data partition fetch methods
        String treeModelDatabase = "root." + node.getQualifiedObjectName().getDatabaseName();
        DataPartition dataPartition =
            fetchDataPartitionByDevices(
                treeModelDatabase, deviceEntries, queryContext.getGlobalTimeFilter());

        if (dataPartition.getDataPartitionMap().size() > 1) {
          throw new IllegalStateException(
              "Table model can only process data only in one database yet!");
        }

        if (dataPartition.getDataPartitionMap().isEmpty()) {
          analysis.setFinishQueryAfterAnalyze();
          analysis.setEmptyDataSource(true);
        } else {
          Set<TRegionReplicaSet> regionReplicaSet = new HashSet<>();
          for (Map.Entry<
                  String,
                  Map<TSeriesPartitionSlot, Map<TTimePartitionSlot, List<TRegionReplicaSet>>>>
              e1 : dataPartition.getDataPartitionMap().entrySet()) {
            for (Map.Entry<TSeriesPartitionSlot, Map<TTimePartitionSlot, List<TRegionReplicaSet>>>
                e2 : e1.getValue().entrySet()) {
              for (Map.Entry<TTimePartitionSlot, List<TRegionReplicaSet>> e3 :
                  e2.getValue().entrySet()) {
                regionReplicaSet.addAll(e3.getValue());
              }
            }
          }
          node.setRegionReplicaSetList(new ArrayList<>(regionReplicaSet));
        }
      }
    }

    private DataPartition fetchDataPartitionByDevices(
        String database, List<DeviceEntry> deviceEntries, Filter globalTimeFilter) {
      Pair<List<TTimePartitionSlot>, Pair<Boolean, Boolean>> res =
          getTimePartitionSlotList(globalTimeFilter, queryContext);

      // there is no satisfied time range
      if (res.left.isEmpty() && Boolean.FALSE.equals(res.right.left)) {
        return new DataPartition(
            Collections.emptyMap(),
            CONFIG.getSeriesPartitionExecutorClass(),
            CONFIG.getSeriesPartitionSlotNum());
      }

      List<DataPartitionQueryParam> dataPartitionQueryParams =
          deviceEntries.stream()
              .map(
                  deviceEntry ->
                      new DataPartitionQueryParam(
                          deviceEntry.getDeviceID(), res.left, res.right.left, res.right.right))
              .collect(Collectors.toList());
      Map<String, List<DataPartitionQueryParam>> sgNameToQueryParamsMap =
          Collections.singletonMap(database, dataPartitionQueryParams);

      if (res.right.left || res.right.right) {
        return metadata.getDataPartitionWithUnclosedTimeRange(sgNameToQueryParamsMap);
      } else {
        return metadata.getDataPartition(sgNameToQueryParamsMap);
      }
    }
  }

  static boolean containsDiffFunction(Expression expression) {
    if (expression instanceof FunctionCall
        && "diff".equalsIgnoreCase(((FunctionCall) expression).getName().toString())) {
      return true;
    }

    if (!expression.getChildren().isEmpty()) {
      for (Node node : expression.getChildren()) {
        if (containsDiffFunction((Expression) node)) {
          return true;
        }
      }
    }

    return false;
  }

  private static class RewriterContext {}
}
