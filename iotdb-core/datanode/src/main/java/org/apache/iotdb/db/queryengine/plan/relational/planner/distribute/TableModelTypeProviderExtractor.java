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
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.process.ExchangeNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.sink.IdentitySinkNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.Symbol;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.FilterNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.MergeSortNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.OutputNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.ProjectNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.TableScanNode;
import org.apache.iotdb.db.relational.sql.tree.Expression;

import org.apache.tsfile.read.common.type.BooleanType;

import java.util.HashMap;
import java.util.Map;

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
      node.getOutputSymbols()
          .forEach(
              symbol ->
                  beTypeProvider.putTableModelType(
                      symbol, feTypeProvider.getTableModelType(symbol)));
      for (PlanNode source : node.getChildren()) {
        source.accept(this, context);
      }
      return null;
    }

    @Override
    public Void visitTableScan(TableScanNode node, Void context) {
      node.getAssignments().forEach((k, v) -> beTypeProvider.putTableModelType(k, v.getType()));
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
    public Void visitMergeSort(MergeSortNode node, Void context) {
      node.getChildren().forEach(c -> c.accept(this, context));
      return null;
    }

    @Override
    public Void visitExchange(ExchangeNode node, Void context) {
      node.getChildren().forEach(c -> c.accept(this, context));
      return null;
    }

    @Override
    public Void visitIdentitySink(IdentitySinkNode node, Void context) {
      node.getChildren().forEach(c -> c.accept(this, context));
      return null;
    }
  }
}
