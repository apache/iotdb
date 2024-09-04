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
import org.apache.iotdb.db.queryengine.common.MPPQueryContext;
import org.apache.iotdb.db.queryengine.common.QueryId;
import org.apache.iotdb.db.queryengine.common.SessionInfo;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.write.InsertRowNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.write.RelationalInsertRowNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.write.RelationalInsertRowsNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.write.RelationalInsertTabletNode;
import org.apache.iotdb.db.queryengine.plan.relational.analyzer.Analysis;
import org.apache.iotdb.db.queryengine.plan.relational.analyzer.Field;
import org.apache.iotdb.db.queryengine.plan.relational.analyzer.NodeRef;
import org.apache.iotdb.db.queryengine.plan.relational.analyzer.Scope;
import org.apache.iotdb.db.queryengine.plan.relational.metadata.ColumnSchema;
import org.apache.iotdb.db.queryengine.plan.relational.metadata.QualifiedObjectName;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.TableScanNode;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.AliasedRelation;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.AstVisitor;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.Except;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.InsertRow;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.InsertRows;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.InsertTablet;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.Intersect;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.Join;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.Node;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.QualifiedName;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.Query;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.QuerySpecification;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.SubqueryExpression;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.Table;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.TableSubquery;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.Union;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.Values;
import org.apache.iotdb.db.queryengine.plan.statement.crud.InsertRowStatement;
import org.apache.iotdb.db.queryengine.plan.statement.crud.InsertRowsStatement;
import org.apache.iotdb.db.queryengine.plan.statement.crud.InsertTabletStatement;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static java.util.Objects.requireNonNull;

public class RelationPlanner extends AstVisitor<RelationPlan, Void> {

  private final Analysis analysis;
  private final SymbolAllocator symbolAllocator;
  private final MPPQueryContext queryContext;
  private final QueryId idAllocator;
  private final Optional<TranslationMap> outerContext;
  private final SessionInfo sessionInfo;
  private final SubqueryPlanner subqueryPlanner;
  private final Map<NodeRef<Node>, RelationPlan> recursiveSubqueries;

  public RelationPlanner(
      Analysis analysis,
      SymbolAllocator symbolAllocator,
      MPPQueryContext queryContext,
      Optional<TranslationMap> outerContext,
      SessionInfo sessionInfo,
      Map<NodeRef<Node>, RelationPlan> recursiveSubqueries) {
    requireNonNull(analysis, "analysis is null");
    requireNonNull(symbolAllocator, "symbolAllocator is null");
    requireNonNull(queryContext, "queryContext is null");
    requireNonNull(outerContext, "outerContext is null");
    requireNonNull(sessionInfo, "session is null");
    requireNonNull(recursiveSubqueries, "recursiveSubqueries is null");

    this.analysis = analysis;
    this.symbolAllocator = symbolAllocator;
    this.queryContext = queryContext;
    this.idAllocator = queryContext.getQueryId();
    this.outerContext = outerContext;
    this.sessionInfo = sessionInfo;
    this.subqueryPlanner =
        new SubqueryPlanner(
            analysis,
            symbolAllocator,
            queryContext,
            outerContext,
            sessionInfo,
            recursiveSubqueries);
    this.recursiveSubqueries = recursiveSubqueries;
  }

  @Override
  protected RelationPlan visitQuery(Query node, Void context) {
    return new QueryPlanner(
            analysis, symbolAllocator, queryContext, outerContext, sessionInfo, recursiveSubqueries)
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

    Scope scope = analysis.getScope(table);
    ImmutableList.Builder<Symbol> outputSymbolsBuilder = ImmutableList.builder();
    ImmutableMap.Builder<Symbol, ColumnSchema> symbolToColumnSchema = ImmutableMap.builder();
    Collection<Field> fields = scope.getRelationType().getAllFields();
    QualifiedName qualifiedName = analysis.getRelationName(table);
    if (!qualifiedName.getPrefix().isPresent()) {
      throw new IllegalStateException("Table " + table.getName() + " has no prefix!");
    }

    QualifiedObjectName qualifiedObjectName =
        new QualifiedObjectName(
            qualifiedName.getPrefix().map(QualifiedName::toString).orElse(null),
            qualifiedName.getSuffix());

    Set<String> usedColumns = analysis.getUsedColumns(qualifiedObjectName);

    // on the basis of that the order of fields is same with the column category order of segments
    // in DeviceEntry
    Map<Symbol, Integer> idAndAttributeIndexMap = new HashMap<>();
    int idIndex = 0;
    for (Field field : fields) {
      TsTableColumnCategory category = field.getColumnCategory();
      // only keep used columns and all ID columns
      if (category != TsTableColumnCategory.ID
          && field.getOriginColumnName().isPresent()
          && !usedColumns.contains(field.getOriginColumnName().get())) {
        continue;
      }
      Symbol symbol = symbolAllocator.newSymbol(field);
      outputSymbolsBuilder.add(symbol);
      symbolToColumnSchema.put(
          symbol,
          new ColumnSchema(
              field.getName().orElse(null), field.getType(), field.isHidden(), category));
      if (category == TsTableColumnCategory.ID) {
        idAndAttributeIndexMap.put(symbol, idIndex++);
      }
    }

    List<Symbol> outputSymbols = outputSymbolsBuilder.build();

    Map<Symbol, ColumnSchema> tableColumnSchema = symbolToColumnSchema.build();
    analysis.addTableSchema(qualifiedObjectName, tableColumnSchema);
    TableScanNode tableScanNode =
        new TableScanNode(
            idAllocator.genPlanNodeId(),
            qualifiedObjectName,
            outputSymbols,
            tableColumnSchema,
            idAndAttributeIndexMap);
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
            analysis, symbolAllocator, queryContext, outerContext, sessionInfo, recursiveSubqueries)
        .plan(node);
  }

  @Override
  protected RelationPlan visitNode(Node node, Void context) {
    throw new IllegalStateException("Unsupported node type: " + node.getClass().getName());
  }

  @Override
  protected RelationPlan visitTableSubquery(TableSubquery node, Void context) {
    RelationPlan plan = process(node.getQuery(), context);
    // TODO transmit outerContext
    return new RelationPlan(plan.getRoot(), analysis.getScope(node), plan.getFieldMappings());
  }

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

  // ================================ Implemented later =====================================

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

  @Override
  protected RelationPlan visitInsertTablet(InsertTablet node, Void context) {
    final InsertTabletStatement insertTabletStatement = node.getInnerTreeStatement();
    RelationalInsertTabletNode insertNode =
        new RelationalInsertTabletNode(
            idAllocator.genPlanNodeId(),
            insertTabletStatement.getDevicePath(),
            insertTabletStatement.isAligned(),
            insertTabletStatement.getMeasurements(),
            insertTabletStatement.getDataTypes(),
            insertTabletStatement.getMeasurementSchemas(),
            insertTabletStatement.getTimes(),
            insertTabletStatement.getBitMaps(),
            insertTabletStatement.getColumns(),
            insertTabletStatement.getRowCount(),
            insertTabletStatement.getColumnCategories());
    insertNode.setFailedMeasurementNumber(insertTabletStatement.getFailedMeasurementNumber());
    return new RelationPlan(insertNode, analysis.getRootScope(), Collections.emptyList());
  }

  @Override
  protected RelationPlan visitInsertRow(InsertRow node, Void context) {
    InsertRowStatement insertRowStatement = node.getInnerTreeStatement();
    RelationalInsertRowNode insertNode = fromInsertRowStatement(insertRowStatement);
    return new RelationPlan(insertNode, analysis.getRootScope(), Collections.emptyList());
  }

  protected RelationalInsertRowNode fromInsertRowStatement(InsertRowStatement insertRowStatement) {
    RelationalInsertRowNode insertNode =
        new RelationalInsertRowNode(
            idAllocator.genPlanNodeId(),
            insertRowStatement.getDevicePath(),
            insertRowStatement.isAligned(),
            insertRowStatement.getMeasurements(),
            insertRowStatement.getDataTypes(),
            insertRowStatement.getTime(),
            insertRowStatement.getValues(),
            insertRowStatement.isNeedInferType(),
            insertRowStatement.getColumnCategories());
    insertNode.setFailedMeasurementNumber(insertRowStatement.getFailedMeasurementNumber());
    insertNode.setMeasurementSchemas(insertRowStatement.getMeasurementSchemas());
    return insertNode;
  }

  @Override
  protected RelationPlan visitInsertRows(InsertRows node, Void context) {
    InsertRowsStatement insertRowsStatement = node.getInnerTreeStatement();
    List<Integer> indices = new ArrayList<>();
    List<InsertRowNode> insertRowStatements = new ArrayList<>();
    for (int i = 0; i < insertRowsStatement.getInsertRowStatementList().size(); i++) {
      indices.add(i);
      insertRowStatements.add(
          fromInsertRowStatement(insertRowsStatement.getInsertRowStatementList().get(i)));
    }
    RelationalInsertRowsNode relationalInsertRowsNode =
        new RelationalInsertRowsNode(idAllocator.genPlanNodeId(), indices, insertRowStatements);
    return new RelationPlan(
        relationalInsertRowsNode, analysis.getRootScope(), Collections.emptyList());
  }
}
