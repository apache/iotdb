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

import org.apache.iotdb.commons.schema.table.column.TsTableColumnCategory;
import org.apache.iotdb.db.queryengine.common.MPPQueryContext;
import org.apache.iotdb.db.queryengine.common.SessionInfo;
import org.apache.iotdb.db.queryengine.plan.analyze.IPartitionFetcher;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanVisitor;
import org.apache.iotdb.db.queryengine.plan.relational.analyzer.Analysis;
import org.apache.iotdb.db.queryengine.plan.relational.metadata.ColumnSchema;
import org.apache.iotdb.db.queryengine.plan.relational.metadata.Metadata;
import org.apache.iotdb.db.queryengine.plan.relational.planner.Symbol;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.FilterNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.OutputNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.ProjectNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.TableScanNode;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.DefaultTraversalVisitor;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.Expression;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.SymbolReference;

import com.google.common.collect.ImmutableList;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.apache.iotdb.commons.conf.IoTDBConstant.TIME;

/**
 * Remove unused columns in TableScanNode.
 *
 * <p>For example, The output columns of TableScanNode in `select * from table1` query are `tag1,
 * attr1, s1`, but the output columns of TableScanNode in `select s1 from table1` query can only be
 * `s1`.
 */
public class PruneUnUsedColumns implements RelationalPlanOptimizer {

  @Override
  public PlanNode optimize(
      PlanNode planNode,
      Analysis analysis,
      Metadata metadata,
      IPartitionFetcher partitionFetcher,
      SessionInfo sessionInfo,
      MPPQueryContext context) {
    return planNode.accept(new Rewriter(), new RewriterContext());
  }

  private static class Rewriter extends PlanVisitor<PlanNode, RewriterContext> {

    @Override
    public PlanNode visitPlan(PlanNode node, RewriterContext context) {
      for (PlanNode child : node.getChildren()) {
        child.accept(this, context);
      }
      return node;
    }

    @Override
    public PlanNode visitOutput(OutputNode node, RewriterContext context) {
      context.allUsedSymbolSet.addAll(node.getOutputSymbols());
      node.getChild().accept(this, context);
      return node;
    }

    @Override
    public PlanNode visitProject(ProjectNode node, RewriterContext context) {
      // There must exist OutputNode above ProjectNode
      node.getAssignments()
          .getMap()
          .entrySet()
          .removeIf(entry -> !context.allUsedSymbolSet.contains(entry.getKey()));
      Set<Symbol> usedSymbolSet = new HashSet<>();
      for (Map.Entry<Symbol, Expression> entry : node.getAssignments().getMap().entrySet()) {
        ImmutableList.Builder<Symbol> symbolBuilder = ImmutableList.builder();
        new SymbolBuilderVisitor().process(entry.getValue(), symbolBuilder);
        usedSymbolSet.addAll(symbolBuilder.build());
      }

      context.allUsedSymbolSet.addAll(usedSymbolSet);
      node.getChild().accept(this, context);
      return node;
    }

    @Override
    public PlanNode visitFilter(FilterNode node, RewriterContext context) {
      ImmutableList.Builder<Symbol> symbolBuilder = ImmutableList.builder();
      new SymbolBuilderVisitor().process(node.getPredicate(), symbolBuilder);
      context.allUsedSymbolSet.addAll(symbolBuilder.build());
      node.getChild().accept(this, context);
      return node;
    }

    @Override
    public PlanNode visitTableScan(TableScanNode node, RewriterContext context) {
      List<Symbol> newOutputSymbols = new ArrayList<>();
      Map<Symbol, ColumnSchema> newAssignments = new HashMap<>();
      for (Symbol symbol : node.getOutputSymbols()) {
        if (TIME.equalsIgnoreCase(symbol.getName()) || context.allUsedSymbolSet.contains(symbol)) {
          newOutputSymbols.add(symbol);
          newAssignments.put(symbol, node.getAssignments().get(symbol));
        }
      }
      node.setOutputSymbols(newOutputSymbols);
      node.setAssignments(newAssignments);

      int IDIdx = 0, attributeIdx = 0;
      Map<Symbol, Integer> idAndAttributeIndexMap = new HashMap<>(node.getAssignments().size());
      for (Symbol symbol : node.getOutputSymbols()) {
        ColumnSchema columnSchema = node.getAssignments().get(symbol);
        if (TsTableColumnCategory.ID.equals(columnSchema.getColumnCategory())) {
          idAndAttributeIndexMap.put(symbol, IDIdx++);
        } else if (TsTableColumnCategory.ATTRIBUTE.equals(columnSchema.getColumnCategory())) {
          idAndAttributeIndexMap.put(symbol, attributeIdx++);
        }
      }
      node.setIdAndAttributeIndexMap(idAndAttributeIndexMap);

      return node;
    }
  }

  private static class SymbolBuilderVisitor
      extends DefaultTraversalVisitor<ImmutableList.Builder<Symbol>> {
    @Override
    protected Void visitSymbolReference(
        SymbolReference node, ImmutableList.Builder<Symbol> builder) {
      builder.add(Symbol.from(node));
      return null;
    }
  }

  private static class RewriterContext {
    Set<Symbol> allUsedSymbolSet = new HashSet<>();
  }
}
