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

import org.apache.iotdb.db.queryengine.common.QueryId;
import org.apache.iotdb.db.queryengine.common.SessionInfo;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNode;
import org.apache.iotdb.db.queryengine.plan.relational.analyzer.Analysis;
import org.apache.iotdb.db.queryengine.plan.relational.analyzer.Field;
import org.apache.iotdb.db.queryengine.plan.relational.analyzer.NodeRef;
import org.apache.iotdb.db.queryengine.plan.relational.analyzer.Scope;
import org.apache.iotdb.db.queryengine.plan.relational.metadata.ColumnHandle;
import org.apache.iotdb.db.queryengine.plan.relational.metadata.TableHandle;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.FilterNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.TableScanNode;
import org.apache.iotdb.db.relational.sql.tree.AliasedRelation;
import org.apache.iotdb.db.relational.sql.tree.AstVisitor;
import org.apache.iotdb.db.relational.sql.tree.Expression;
import org.apache.iotdb.db.relational.sql.tree.Node;
import org.apache.iotdb.db.relational.sql.tree.Query;
import org.apache.iotdb.db.relational.sql.tree.QuerySpecification;
import org.apache.iotdb.db.relational.sql.tree.SubqueryExpression;
import org.apache.iotdb.db.relational.sql.tree.Table;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

import static java.util.Objects.requireNonNull;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.PlanBuilder.newPlanBuilder;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.QueryPlanner.coerceIfNecessary;

class RelationPlanner extends AstVisitor<RelationPlan, Void> {
  private final Analysis analysis;
  private final SymbolAllocator symbolAllocator;
  private final QueryId idAllocator;
  private final SessionInfo session;
  private final Map<NodeRef<Node>, RelationPlan> recursiveSubqueries;

  RelationPlanner(
      Analysis analysis,
      SymbolAllocator symbolAllocator,
      QueryId idAllocator,
      SessionInfo session,
      Map<NodeRef<Node>, RelationPlan> recursiveSubqueries) {
    requireNonNull(analysis, "analysis is null");
    requireNonNull(symbolAllocator, "symbolAllocator is null");
    requireNonNull(idAllocator, "idAllocator is null");
    requireNonNull(session, "session is null");
    requireNonNull(recursiveSubqueries, "recursiveSubqueries is null");

    this.analysis = analysis;
    this.symbolAllocator = symbolAllocator;
    this.idAllocator = idAllocator;
    this.session = session;
    this.recursiveSubqueries = recursiveSubqueries;
  }

  @Override
  protected RelationPlan visitNode(Node node, Void context) {
    throw new IllegalStateException("Unsupported node type: " + node.getClass().getName());
  }

  @Override
  protected RelationPlan visitTable(Table node, Void context) {
    // is this a recursive reference in expandable named query? If so, there's base relation already
    // planned.
    RelationPlan expansion = recursiveSubqueries.get(NodeRef.of(node));
    if (expansion != null) {
      // put the pre-planned recursive subquery in the actual outer context to enable resolving
      // correlation
      return new RelationPlan(
          expansion.getRoot(), expansion.getScope(), expansion.getFieldMappings());
    }

    Query namedQuery = analysis.getNamedQuery(node);
    Scope scope = analysis.getScope(node);

    RelationPlan plan;
    if (namedQuery != null) {
      throw new RuntimeException("NamedQuery is not supported");
    } else {
      TableHandle handle = analysis.getTableHandle(node);

      ImmutableList.Builder<Symbol> outputSymbolsBuilder = ImmutableList.builder();
      ImmutableMap.Builder<Symbol, ColumnHandle> columns = ImmutableMap.builder();

      // Collection<Field> fields = analysis.getMaterializedViewStorageTableFields(node);
      Collection<Field> fields = scope.getRelationType().getAllFields();
      for (Field field : fields) {
        Symbol symbol = symbolAllocator.newSymbol(field);

        outputSymbolsBuilder.add(symbol);
        columns.put(symbol, analysis.getColumn(field));
      }

      List<Symbol> outputSymbols = outputSymbolsBuilder.build();
      PlanNode root =
          new TableScanNode(
              idAllocator.genPlanNodeId(), handle, outputSymbols, columns.buildOrThrow());

      plan = new RelationPlan(root, scope, outputSymbols);
    }

    // TODO what's the meaning of RowFilters addColumnMasks?
    // plan = addRowFilters(node, plan);
    // plan = addColumnMasks(node, plan);

    return plan;
  }

  private RelationPlan addRowFilters(Table node, RelationPlan plan) {
    return addRowFilters(node, plan, Function.identity());
  }

  public RelationPlan addRowFilters(
      Table node, RelationPlan plan, Function<Expression, Expression> predicateTransformation) {
    List<Expression> filters = null;
    // analysis.getRowFilters(node);

    if (filters.isEmpty()) {
      return plan;
    }

    // The fields in the access control scope has the same layout as those for the table scope
    PlanBuilder planBuilder = newPlanBuilder(plan, analysis, session);
    // .withScope(accessControlScope.apply(node), plan.getFieldMappings());

    for (Expression filter : filters) {
      // planBuilder = subqueryPlanner.handleSubqueries(planBuilder, filter,
      // analysis.getSubqueries(filter));

      Expression predicate = coerceIfNecessary(analysis, filter, filter);
      predicate = predicateTransformation.apply(predicate);
      planBuilder =
          planBuilder.withNewRoot(
              new FilterNode(idAllocator.genPlanNodeId(), planBuilder.getRoot(), predicate));
    }

    return new RelationPlan(planBuilder.getRoot(), plan.getScope(), plan.getFieldMappings());
  }

  //    private RelationPlan addColumnMasks(Table table, RelationPlan plan) {
  //        Map<String, Expression> columnMasks = analysis.getColumnMasks(table);
  //
  //        // A Table can represent a WITH query, which can have anonymous fields. On the other
  // hand,
  //        // it can't have masks. The loop below expects fields to have proper names, so bail out
  //        // if the masks are missing
  //        if (columnMasks.isEmpty()) {
  //            return plan;
  //        }
  //
  //        // The fields in the access control scope has the same layout as those for the table
  // scope
  //        PlanBuilder planBuilder = newPlanBuilder(plan, analysis, session)
  //                .withScope(analysis.getAccessControlScope(table), plan.getFieldMappings());
  //
  //        Assignments.Builder assignments = Assignments.builder();
  //        assignments.putIdentities(planBuilder.getRoot().getOutputSymbols());
  //
  //        List<Symbol> fieldMappings = new ArrayList<>();
  //        for (int i = 0; i < plan.getDescriptor().getAllFieldCount(); i++) {
  //            Field field = plan.getDescriptor().getFieldByIndex(i);
  //
  //            Expression mask = columnMasks.get(field.getName().orElseThrow());
  //            Symbol symbol = plan.getFieldMappings().get(i);
  //            Expression projection = symbol.toSymbolReference();
  //            if (mask != null) {
  //                symbol = symbolAllocator.newSymbol(symbol);
  //                projection = coerceIfNecessary(analysis, mask, planBuilder.rewrite(mask));
  //            }
  //
  //            assignments.put(symbol, projection);
  //            fieldMappings.add(symbol);
  //        }
  //
  //        planBuilder = planBuilder
  //                .withNewRoot(new ProjectNode(
  //                        idAllocator.genPlanNodeId(),
  //                        planBuilder.getRoot(),
  //                        assignments.build()));
  //
  //        return new RelationPlan(planBuilder.getRoot(), plan.getScope(), fieldMappings);
  //    }

  @Override
  protected RelationPlan visitAliasedRelation(AliasedRelation node, Void context) {
    RelationPlan subPlan = process(node.getRelation(), context);

    PlanNode root = subPlan.getRoot();
    List<Symbol> mappings = subPlan.getFieldMappings();

    if (node.getColumnNames() != null) {
      ImmutableList.Builder<Symbol> newMappings = ImmutableList.builder();

      // Adjust the mappings to expose only the columns visible in the scope of the aliased relation
      for (int i = 0; i < subPlan.getDescriptor().getAllFieldCount(); i++) {
        if (!subPlan.getDescriptor().getFieldByIndex(i).isHidden()) {
          newMappings.add(subPlan.getFieldMappings().get(i));
        }
      }

      mappings = newMappings.build();
    }

    return new RelationPlan(root, analysis.getScope(node), mappings);
  }

  @Override
  protected RelationPlan visitQuery(Query node, Void context) {
    return new QueryPlanner(analysis, symbolAllocator, idAllocator, session, recursiveSubqueries)
        .plan(node);
  }

  @Override
  protected RelationPlan visitQuerySpecification(QuerySpecification node, Void context) {
    return new QueryPlanner(analysis, symbolAllocator, idAllocator, session, recursiveSubqueries)
        .plan(node);
  }

  @Override
  protected RelationPlan visitSubqueryExpression(SubqueryExpression node, Void context) {
    return process(node.getQuery(), context);
  }
}
