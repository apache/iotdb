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

import org.apache.iotdb.common.rpc.thrift.TTimePartitionSlot;
import org.apache.iotdb.commons.partition.DataPartition;
import org.apache.iotdb.commons.partition.DataPartitionQueryParam;
import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.queryengine.common.MPPQueryContext;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanVisitor;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.process.MultiChildProcessNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.process.SingleChildProcessNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.write.InsertTabletNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.write.RelationalInsertTabletNode;
import org.apache.iotdb.db.queryengine.plan.relational.analyzer.Analysis;
import org.apache.iotdb.db.queryengine.plan.relational.analyzer.predicate.ConvertPredicateToTimeFilterVisitor;
import org.apache.iotdb.db.queryengine.plan.relational.analyzer.predicate.PredicateCombineIntoTableScanChecker;
import org.apache.iotdb.db.queryengine.plan.relational.analyzer.predicate.PredicatePushIntoMetadataChecker;
import org.apache.iotdb.db.queryengine.plan.relational.metadata.ColumnSchema;
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
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static java.util.Objects.requireNonNull;
import static org.apache.iotdb.commons.schema.table.column.TsTableColumnCategory.ATTRIBUTE;
import static org.apache.iotdb.commons.schema.table.column.TsTableColumnCategory.MEASUREMENT;
import static org.apache.iotdb.commons.schema.table.column.TsTableColumnCategory.TIME;
import static org.apache.iotdb.db.queryengine.plan.analyze.AnalyzeVisitor.getTimePartitionSlotList;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.ir.GlobalTimePredicateExtractVisitor.extractGlobalTimeFilter;

/**
 * <b>Optimization phase:</b> Logical plan planning.
 *
 * <p>After the optimized rule {@link SimplifyExpressions} finished, predicate expression in
 * FilterNode has been transformed to conjunctive normal forms(CNF).
 *
 * <p>In this class, we examine each expression in CNFs, determine how to use it, in metadata query,
 * or pushed down into ScanOperators, or it can only be used in FilterNode above with TableScanNode.
 *
 * <ul>
 *   <li>For metadata query expressions, it will be used in {@code tableIndexScan} method to
 *       generate the deviceEntries and DataPartition used for TableScanNode.
 *   <li>For expressions which can be pushed into TableScanNode, we will execute {@code
 *       extractGlobalTimeFilter}, to extract the timePredicate and pushDownValuePredicate.
 *   <li>Expression which can not be pushed down into TableScanNode, will be used in the FilterNode
 *       above of TableScanNode.
 * </ul>
 *
 * <p>Notice that, when aggregation, multi-table, join are introduced, this optimization rule need
 * to be adapted.
 */
public class PushPredicateIntoTableScan implements PlanOptimizer {

  private static final IoTDBConfig CONFIG = IoTDBDescriptor.getInstance().getConfig();

  @Override
  public PlanNode optimize(PlanNode plan, Context context) {
    return plan.accept(
        new Rewriter(context.getQueryContext(), context.getAnalysis(), context.getMetadata()),
        null);
  }

  private static class Rewriter extends PlanVisitor<PlanNode, Void> {
    private final MPPQueryContext queryContext;
    private final Analysis analysis;
    private final Metadata metadata;
    private Expression predicate;

    Rewriter(MPPQueryContext queryContext, Analysis analysis, Metadata metadata) {
      this.queryContext = queryContext;
      this.analysis = analysis;
      this.metadata = metadata;
    }

    @Override
    public PlanNode visitPlan(PlanNode node, Void context) {
      throw new IllegalArgumentException(
          String.format("Unexpected plan node: %s in rule PushPredicateIntoTableScan", node));
    }

    @Override
    public PlanNode visitSingleChildProcess(SingleChildProcessNode node, Void context) {
      PlanNode rewrittenChild = node.getChild().accept(this, context);
      node.setChild(rewrittenChild);
      return node;
    }

    @Override
    public PlanNode visitMultiChildProcess(MultiChildProcessNode node, Void context) {
      List<PlanNode> rewrittenChildren = new ArrayList<>();
      for (PlanNode child : node.getChildren()) {
        rewrittenChildren.add(child.accept(this, context));
      }
      node.setChildren(rewrittenChildren);
      return node;
    }

    @Override
    public PlanNode visitFilter(FilterNode node, Void context) {

      if (node.getPredicate() != null) {

        predicate = node.getPredicate();

        // when exist diff function, predicate can not be pushed down into TableScanNode
        if (containsDiffFunction(predicate)) {
          node.setChild(node.getChild().accept(this, context));
          return node;
        }

        if (node.getChild() instanceof TableScanNode) {
          // child of FilterNode is TableScanNode, means FilterNode must get from where clause
          return combineFilterAndScan((TableScanNode) node.getChild());
        } else {
          // FilterNode may get from having or subquery
          node.setChild(node.getChild().accept(this, context));
          return node;
        }

      } else {
        throw new IllegalStateException(
            "Filter node has no predicate, node: " + node.getPlanNodeId());
      }
    }

    public PlanNode combineFilterAndScan(TableScanNode tableScanNode) {
      SplitExpression splitExpression = splitPredicate(tableScanNode);

      // exist expressions can push down to scan operator
      if (!splitExpression.getExpressionsCanPushDown().isEmpty()) {
        List<Expression> expressions = splitExpression.getExpressionsCanPushDown();
        Expression pushDownPredicate =
            expressions.size() == 1
                ? expressions.get(0)
                : new LogicalExpression(LogicalExpression.Operator.AND, expressions);

        // extract global time filter and set it to TableScanNode
        Pair<Expression, Boolean> resultPair = extractGlobalTimeFilter(pushDownPredicate);
        if (resultPair.left != null) {
          tableScanNode.setTimePredicate(resultPair.left);
        }
        if (Boolean.TRUE.equals(resultPair.right)) {
          if (pushDownPredicate instanceof LogicalExpression
              && ((LogicalExpression) pushDownPredicate).getTerms().size() == 1) {
            tableScanNode.setPushDownPredicate(
                ((LogicalExpression) pushDownPredicate).getTerms().get(0));
          } else {
            tableScanNode.setPushDownPredicate(pushDownPredicate);
          }
        }
      } else {
        tableScanNode.setPushDownPredicate(null);
      }

      // do index scan after expressionCanPushDown is processed
      tableMetadataIndexScan(tableScanNode, splitExpression.getMetadataExpressions());

      // exist expressions can not push down to scan operator
      if (!splitExpression.getExpressionsCannotPushDown().isEmpty()) {
        List<Expression> expressions = splitExpression.getExpressionsCannotPushDown();
        return new FilterNode(
            queryContext.getQueryId().genPlanNodeId(),
            tableScanNode,
            expressions.size() == 1
                ? expressions.get(0)
                : new LogicalExpression(LogicalExpression.Operator.AND, expressions));
      }

      return tableScanNode;
    }

    private SplitExpression splitPredicate(TableScanNode node) {
      Set<String> idOrAttributeColumnNames = new HashSet<>(node.getAssignments().size());
      Set<String> measurementColumnNames = new HashSet<>(node.getAssignments().size());
      for (Map.Entry<Symbol, ColumnSchema> entry : node.getAssignments().entrySet()) {
        Symbol columnSymbol = entry.getKey();
        ColumnSchema columnSchema = entry.getValue();
        if (MEASUREMENT.equals(columnSchema.getColumnCategory())
            || TIME.equals(columnSchema.getColumnCategory())) {
          measurementColumnNames.add(columnSymbol.getName());
        } else {
          idOrAttributeColumnNames.add(columnSymbol.getName());
        }
      }

      List<Expression> metadataExpressions = new ArrayList<>();
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

        return new SplitExpression(
            metadataExpressions, expressionsCanPushDown, expressionsCannotPushDown);
      }

      if (PredicatePushIntoMetadataChecker.check(idOrAttributeColumnNames, predicate)) {
        metadataExpressions.add(predicate);
      } else if (PredicateCombineIntoTableScanChecker.check(measurementColumnNames, predicate)) {
        expressionsCanPushDown.add(predicate);
      } else {
        expressionsCannotPushDown.add(predicate);
      }

      return new SplitExpression(
          metadataExpressions, expressionsCanPushDown, expressionsCannotPushDown);
    }

    @Override
    public PlanNode visitTableScan(TableScanNode node, Void context) {
      tableMetadataIndexScan(node, Collections.emptyList());
      return node;
    }

    @Override
    public PlanNode visitInsertTablet(InsertTabletNode node, Void context) {
      return node;
    }

    @Override
    public PlanNode visitRelationalInsertTablet(RelationalInsertTabletNode node, Void context) {
      return node;
    }

    /** Get deviceEntries and DataPartition used in TableScan. */
    private void tableMetadataIndexScan(TableScanNode node, List<Expression> metadataExpressions) {
      List<String> attributeColumns = new ArrayList<>();
      int attributeIndex = 0;
      for (Symbol columnName : node.getAssignments().keySet()) {
        if (ATTRIBUTE.equals(node.getAssignments().get(columnName).getColumnCategory())) {
          attributeColumns.add(columnName.getName());
          node.getIdAndAttributeIndexMap().put(columnName, attributeIndex++);
        }
      }
      List<DeviceEntry> deviceEntries =
          metadata.indexScan(
              node.getQualifiedObjectName(), metadataExpressions, attributeColumns, queryContext);
      node.setDeviceEntries(deviceEntries);

      if (deviceEntries.isEmpty()) {
        analysis.setFinishQueryAfterAnalyze();
        analysis.setEmptyDataSource(true);
      } else {
        Filter timeFilter =
            node.getTimePredicate()
                .map(value -> value.accept(new ConvertPredicateToTimeFilterVisitor(), null))
                .orElse(null);
        node.setTimeFilter(timeFilter);
        String treeModelDatabase = "root." + node.getQualifiedObjectName().getDatabaseName();
        DataPartition dataPartition =
            fetchDataPartitionByDevices(treeModelDatabase, deviceEntries, timeFilter);

        if (dataPartition.getDataPartitionMap().size() > 1) {
          throw new IllegalStateException(
              "Table model can only process data only in one database yet!");
        }

        if (dataPartition.getDataPartitionMap().isEmpty()) {
          analysis.setFinishQueryAfterAnalyze();
          analysis.setEmptyDataSource(true);
        } else {
          analysis.upsertDataPartition(dataPartition);
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

      if (res.right.left || res.right.right) {
        return metadata.getDataPartitionWithUnclosedTimeRange(database, dataPartitionQueryParams);
      } else {
        return metadata.getDataPartition(database, dataPartitionQueryParams);
      }
    }
  }

  public static boolean containsDiffFunction(Expression expression) {
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

  private static class SplitExpression {
    // indexed tag expressions, such as `tag1 = 'A'`
    List<Expression> metadataExpressions;
    // expressions can push down into TableScan, such as `time > 1 and s_1 = 1`
    List<Expression> expressionsCanPushDown;
    // expressions can not push down into TableScan, such as `s_1 is null`
    List<Expression> expressionsCannotPushDown;

    public SplitExpression(
        List<Expression> metadataExpressions,
        List<Expression> expressionsCanPushDown,
        List<Expression> expressionsCannotPushDown) {
      this.metadataExpressions = requireNonNull(metadataExpressions, "metadataExpressions is null");
      this.expressionsCanPushDown =
          requireNonNull(expressionsCanPushDown, "expressionsCanPushDown is null");
      this.expressionsCannotPushDown =
          requireNonNull(expressionsCannotPushDown, "expressionsCannotPushDown is null");
    }

    public List<Expression> getMetadataExpressions() {
      return this.metadataExpressions;
    }

    public List<Expression> getExpressionsCanPushDown() {
      return this.expressionsCanPushDown;
    }

    public List<Expression> getExpressionsCannotPushDown() {
      return this.expressionsCannotPushDown;
    }
  }
}
