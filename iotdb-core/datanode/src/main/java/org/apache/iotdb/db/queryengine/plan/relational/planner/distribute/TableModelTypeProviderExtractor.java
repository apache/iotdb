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

package org.apache.iotdb.db.queryengine.plan.relational.planner.distribute;

import org.apache.iotdb.db.queryengine.plan.analyze.TypeProvider;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.SimplePlanVisitor;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.metadata.read.AbstractTableDeviceQueryNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.metadata.read.CountSchemaMergeNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.metadata.read.TableDeviceQueryCountNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.metadata.read.TableDeviceQueryScanNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.process.ExchangeNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.sink.IdentitySinkNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.Symbol;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.AggregationNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.CollectNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.FilterNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.JoinNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.LimitNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.MergeSortNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.OffsetNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.OutputNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.ProjectNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.SortNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.StreamSortNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.TableScanNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.TopKNode;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.Expression;

import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.read.common.type.BooleanType;
import org.apache.tsfile.read.common.type.TypeFactory;

import java.util.HashMap;
import java.util.Map;

import static org.apache.iotdb.db.queryengine.plan.relational.sql.ast.CountDevice.COUNT_DEVICE_HEADER_STRING;

public class TableModelTypeProviderExtractor {

  private TableModelTypeProviderExtractor() {}

  public static TypeProvider extractor(PlanNode root, TypeProvider analyzedTypeProvider) {
    TypeProvider operatorTypeProvider = new TypeProvider(new HashMap<>());
    root.accept(new Visitor(operatorTypeProvider, analyzedTypeProvider), null);
    return operatorTypeProvider;
  }

  private static class Visitor extends SimplePlanVisitor<Void> {

    // typeProvider used for be operator execution
    private final TypeProvider beTypeProvider;

    // typeProvider analyzed in fe stage
    private final TypeProvider feTypeProvider;

    public Visitor(TypeProvider beTypeProvider, TypeProvider feTypeProvider) {
      this.beTypeProvider = beTypeProvider;
      this.feTypeProvider = feTypeProvider;
    }

    @Override
    public Void visitPlan(PlanNode node, Void context) {
      for (Symbol symbol : node.getOutputSymbols()) {
        if (!feTypeProvider.isSymbolExist(symbol)) {
          throw new IllegalStateException(
              String.format(
                  "Symbol: %s is not exist in feTypeProvider with %s",
                  symbol, node.getClass().getSimpleName()));
        }
        beTypeProvider.putTableModelType(symbol, feTypeProvider.getTableModelType(symbol));
      }
      node.getChildren().forEach(child -> child.accept(this, context));
      return null;
    }

    @Override
    public Void visitAggregation(AggregationNode node, Void context) {
      node.getChild().accept(this, context);
      node.getAggregations()
          .forEach(
              (k, v) ->
                  beTypeProvider.putTableModelType(
                      k, v.getResolvedFunction().getSignature().getReturnType()));
      return null;
    }

    @Override
    public Void visitTableScan(TableScanNode node, Void context) {
      node.getAssignments().forEach((k, v) -> beTypeProvider.putTableModelType(k, v.getType()));
      return null;
    }

    @Override
    public Void visitCountMerge(final CountSchemaMergeNode node, final Void context) {
      beTypeProvider.putTableModelType(
          new Symbol(COUNT_DEVICE_HEADER_STRING), TypeFactory.getType(TSDataType.INT64));
      node.getChildren().forEach(schemaCount -> schemaCount.accept(this, context));
      return null;
    }

    @Override
    public Void visitTableDeviceQueryScan(final TableDeviceQueryScanNode node, final Void context) {
      return visitAbstractTableDeviceQueryNode(node);
    }

    @Override
    public Void visitTableDeviceQueryCount(
        final TableDeviceQueryCountNode node, final Void context) {
      return visitAbstractTableDeviceQueryNode(node);
    }

    private Void visitAbstractTableDeviceQueryNode(final AbstractTableDeviceQueryNode node) {
      node.getColumnHeaderList()
          .forEach(
              columnHeader ->
                  beTypeProvider.putTableModelType(
                      new Symbol(columnHeader.getColumnName()),
                      TypeFactory.getType(columnHeader.getColumnType())));
      return null;
    }

    @Override
    public Void visitProject(ProjectNode node, Void context) {
      node.getChild().accept(this, context);
      for (Map.Entry<Symbol, Expression> entry : node.getAssignments().getMap().entrySet()) {
        Symbol symbol = entry.getKey();
        if (!beTypeProvider.isSymbolExist(symbol)) {
          beTypeProvider.putTableModelType(symbol, feTypeProvider.getTableModelType(symbol));
        }
      }
      return null;
    }

    @Override
    public Void visitFilter(FilterNode node, Void context) {
      node.getChild().accept(this, context);
      // TODO consider complex filter expression
      beTypeProvider.putTableModelType(
          new Symbol(node.getPredicate().toString()), BooleanType.BOOLEAN);
      return null;
    }

    @Override
    public Void visitOutput(OutputNode node, Void context) {
      node.getChild().accept(this, context);
      return null;
    }

    @Override
    public Void visitCollect(CollectNode node, Void context) {
      node.getChildren().forEach(c -> c.accept(this, context));
      return null;
    }

    @Override
    public Void visitSort(SortNode node, Void context) {
      node.getChild().accept(this, context);
      return null;
    }

    @Override
    public Void visitTopK(TopKNode node, Void context) {
      node.getChildren().forEach(c -> c.accept(this, context));
      return null;
    }

    @Override
    public Void visitStreamSort(StreamSortNode node, Void context) {
      node.getChild().accept(this, context);
      return null;
    }

    @Override
    public Void visitMergeSort(MergeSortNode node, Void context) {
      node.getChildren().forEach(c -> c.accept(this, context));
      return null;
    }

    @Override
    public Void visitLimit(LimitNode node, Void context) {
      node.getChild().accept(this, context);
      return null;
    }

    @Override
    public Void visitOffset(OffsetNode node, Void context) {
      node.getChild().accept(this, context);
      return null;
    }

    @Override
    public Void visitExchange(ExchangeNode node, Void context) {
      node.getChildren().forEach(c -> c.accept(this, context));
      return null;
    }

    @Override
    public Void visitJoin(JoinNode node, Void context) {
      node.getLeftChild().accept(this, context);
      node.getRightChild().accept(this, context);
      return null;
    }

    @Override
    public Void visitIdentitySink(IdentitySinkNode node, Void context) {
      node.getChildren().forEach(c -> c.accept(this, context));
      return null;
    }
  }
}
