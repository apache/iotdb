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
package org.apache.iotdb.db.queryengine.plan.relational.planner;

import org.apache.iotdb.commons.schema.table.column.TsTableColumnCategory;
import org.apache.iotdb.db.queryengine.common.QueryId;
import org.apache.iotdb.db.queryengine.common.SessionInfo;
import org.apache.iotdb.db.queryengine.plan.relational.analyzer.Analysis;
import org.apache.iotdb.db.queryengine.plan.relational.analyzer.Field;
import org.apache.iotdb.db.queryengine.plan.relational.analyzer.NodeRef;
import org.apache.iotdb.db.queryengine.plan.relational.analyzer.Scope;
import org.apache.iotdb.db.queryengine.plan.relational.metadata.ColumnSchema;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.TableScanNode;
import org.apache.iotdb.db.relational.sql.tree.AliasedRelation;
import org.apache.iotdb.db.relational.sql.tree.AstVisitor;
import org.apache.iotdb.db.relational.sql.tree.Except;
import org.apache.iotdb.db.relational.sql.tree.Intersect;
import org.apache.iotdb.db.relational.sql.tree.Join;
import org.apache.iotdb.db.relational.sql.tree.Node;
import org.apache.iotdb.db.relational.sql.tree.Query;
import org.apache.iotdb.db.relational.sql.tree.QuerySpecification;
import org.apache.iotdb.db.relational.sql.tree.SubqueryExpression;
import org.apache.iotdb.db.relational.sql.tree.Table;
import org.apache.iotdb.db.relational.sql.tree.TableSubquery;
import org.apache.iotdb.db.relational.sql.tree.Union;
import org.apache.iotdb.db.relational.sql.tree.Values;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static java.util.Objects.requireNonNull;

public class RelationPlanner extends AstVisitor<RelationPlan, Void> {
  private final Analysis analysis;
  private final SymbolAllocator symbolAllocator;
  private final QueryId idAllocator;
  private final SessionInfo sessionInfo;
  private final Map<NodeRef<Node>, RelationPlan> recursiveSubqueries;

  public RelationPlanner(
      Analysis analysis,
      SymbolAllocator symbolAllocator,
      QueryId idAllocator,
      SessionInfo sessionInfo,
      Map<NodeRef<Node>, RelationPlan> recursiveSubqueries) {
    requireNonNull(analysis, "analysis is null");
    requireNonNull(symbolAllocator, "symbolAllocator is null");
    requireNonNull(idAllocator, "idAllocator is null");
    requireNonNull(sessionInfo, "session is null");
    requireNonNull(recursiveSubqueries, "recursiveSubqueries is null");

    this.analysis = analysis;
    this.symbolAllocator = symbolAllocator;
    this.idAllocator = idAllocator;
    this.sessionInfo = sessionInfo;
    this.recursiveSubqueries = recursiveSubqueries;
  }

  @Override
  protected RelationPlan visitQuery(Query node, Void context) {
    return new QueryPlanner(
            analysis, symbolAllocator, idAllocator, sessionInfo, recursiveSubqueries)
        .plan(node);
  }

  @Override
  protected RelationPlan visitTable(Table table, Void context) {
    // is this a recursive reference in expandable named query? If so, there's base relation already
    // planned.
    RelationPlan expansion = recursiveSubqueries.get(NodeRef.of(table));
    if (expansion != null) {
      // put the pre-planned recursive subquery in the actual outer context to enable resolving
      // correlation
      return new RelationPlan(
          expansion.getRoot(), expansion.getScope(), expansion.getFieldMappings());
    }

    Map<Symbol, Integer> idAndAttributeIndexMap = new HashMap<>();
    Scope scope = analysis.getScope(table);
    ImmutableList.Builder<Symbol> outputSymbolsBuilder = ImmutableList.builder();
    ImmutableMap.Builder<Symbol, ColumnSchema> symbolToColumnSchema = ImmutableMap.builder();
    Collection<Field> fields = scope.getRelationType().getAllFields();
    int IDIdx = 0, attributeIdx = 0;
    for (Field field : fields) {
      if ("time".equalsIgnoreCase(field.getName().get())) {
        // TODO consider time ColumnCategory
        continue;
      }
      Symbol symbol = symbolAllocator.newSymbol(field);
      outputSymbolsBuilder.add(symbol);
      symbolToColumnSchema.put(
          symbol,
          new ColumnSchema(
              field.getName().get(), field.getType(), field.isHidden(), field.getColumnCategory()));

      if (TsTableColumnCategory.ID.equals(field.getColumnCategory())) {
        idAndAttributeIndexMap.put(symbol, IDIdx++);
      } else if (TsTableColumnCategory.ATTRIBUTE.equals(field.getColumnCategory())) {
        idAndAttributeIndexMap.put(symbol, attributeIdx++);
      }
    }

    List<Symbol> outputSymbols = outputSymbolsBuilder.build();
    TableScanNode tableScanNode =
        new TableScanNode(
            idAllocator.genPlanNodeId(),
            table.getName().toString(),
            outputSymbols,
            symbolToColumnSchema.build());

    tableScanNode.setIdAndAttributeIndexMap(idAndAttributeIndexMap);
    return new RelationPlan(tableScanNode, scope, outputSymbols);

    // Collection<Field> fields = analysis.getMaterializedViewStorageTableFields(node);
    // Query namedQuery = analysis.getNamedQuery(node);
    // Collection<Field> fields = analysis.getMaterializedViewStorageTableFields(node);
    // plan = addRowFilters(node, plan);
    // plan = addColumnMasks(node, plan);
  }

  @Override
  protected RelationPlan visitQuerySpecification(QuerySpecification node, Void context) {
    return new QueryPlanner(
            analysis, symbolAllocator, idAllocator, sessionInfo, recursiveSubqueries)
        .plan(node);
  }

  @Override
  protected RelationPlan visitNode(Node node, Void context) {
    throw new IllegalStateException("Unsupported node type: " + node.getClass().getName());
  }

  // ================================ Implemented later =====================================
  @Override
  protected RelationPlan visitTableSubquery(TableSubquery node, Void context) {
    throw new IllegalStateException("TableSubquery is not supported in current version.");
  }

  @Override
  protected RelationPlan visitValues(Values node, Void context) {
    throw new IllegalStateException("Values is not supported in current version.");
  }

  @Override
  protected RelationPlan visitSubqueryExpression(SubqueryExpression node, Void context) {
    throw new IllegalStateException("SubqueryExpression is not supported in current version.");
  }

  @Override
  protected RelationPlan visitJoin(Join node, Void context) {
    throw new IllegalStateException("Join is not supported in current version.");
  }

  @Override
  protected RelationPlan visitAliasedRelation(AliasedRelation node, Void context) {
    throw new IllegalStateException("AliasedRelation is not supported in current version.");
  }

  @Override
  protected RelationPlan visitIntersect(Intersect node, Void context) {
    throw new IllegalStateException("Intersect is not supported in current version.");
  }

  @Override
  protected RelationPlan visitUnion(Union node, Void context) {
    throw new IllegalStateException("Union is not supported in current version.");
  }

  @Override
  protected RelationPlan visitExcept(Except node, Void context) {
    throw new IllegalStateException("Except is not supported in current version.");
  }
}
