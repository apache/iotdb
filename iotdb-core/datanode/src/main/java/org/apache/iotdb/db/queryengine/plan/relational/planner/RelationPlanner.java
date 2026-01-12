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

package org.apache.iotdb.db.queryengine.plan.relational.planner;

import org.apache.iotdb.commons.schema.table.column.TsTableColumnCategory;
import org.apache.iotdb.db.exception.sql.SemanticException;
import org.apache.iotdb.db.queryengine.common.MPPQueryContext;
import org.apache.iotdb.db.queryengine.common.QueryId;
import org.apache.iotdb.db.queryengine.common.SessionInfo;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.WritePlanNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.load.LoadTsFileNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.pipe.PipeEnrichedDeleteDataNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.pipe.PipeEnrichedInsertNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.pipe.PipeEnrichedWritePlanNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.write.InsertNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.write.InsertRowNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.write.RelationalDeleteDataNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.write.RelationalInsertRowNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.write.RelationalInsertRowsNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.write.RelationalInsertTabletNode;
import org.apache.iotdb.db.queryengine.plan.relational.analyzer.Analysis;
import org.apache.iotdb.db.queryengine.plan.relational.analyzer.Field;
import org.apache.iotdb.db.queryengine.plan.relational.analyzer.NodeRef;
import org.apache.iotdb.db.queryengine.plan.relational.analyzer.PatternRecognitionAnalysis;
import org.apache.iotdb.db.queryengine.plan.relational.analyzer.PatternRecognitionAnalysis.AggregationDescriptor;
import org.apache.iotdb.db.queryengine.plan.relational.analyzer.PatternRecognitionAnalysis.ClassifierDescriptor;
import org.apache.iotdb.db.queryengine.plan.relational.analyzer.PatternRecognitionAnalysis.MatchNumberDescriptor;
import org.apache.iotdb.db.queryengine.plan.relational.analyzer.PatternRecognitionAnalysis.Navigation;
import org.apache.iotdb.db.queryengine.plan.relational.analyzer.PatternRecognitionAnalysis.ScalarInputDescriptor;
import org.apache.iotdb.db.queryengine.plan.relational.analyzer.RelationType;
import org.apache.iotdb.db.queryengine.plan.relational.analyzer.Scope;
import org.apache.iotdb.db.queryengine.plan.relational.analyzer.tablefunction.TableArgumentAnalysis;
import org.apache.iotdb.db.queryengine.plan.relational.analyzer.tablefunction.TableFunctionInvocationAnalysis;
import org.apache.iotdb.db.queryengine.plan.relational.metadata.ColumnSchema;
import org.apache.iotdb.db.queryengine.plan.relational.metadata.QualifiedObjectName;
import org.apache.iotdb.db.queryengine.plan.relational.metadata.TableMetadataImpl;
import org.apache.iotdb.db.queryengine.plan.relational.metadata.TreeDeviceViewSchema;
import org.apache.iotdb.db.queryengine.plan.relational.planner.ir.IrUtils;
import org.apache.iotdb.db.queryengine.plan.relational.planner.ir.PredicateWithUncorrelatedScalarSubqueryReconstructor;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.CteScanNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.DeviceTableScanNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.ExceptNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.FilterNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.InformationSchemaTableScanNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.IntersectNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.JoinNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.Measure;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.PatternRecognitionNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.ProjectNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.RowsPerMatch;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.SkipToPosition;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.TableFunctionNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.TableScanNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.TreeDeviceViewScanNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.UnionNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.rowpattern.AggregationLabelSet;
import org.apache.iotdb.db.queryengine.plan.relational.planner.rowpattern.AggregationValuePointer;
import org.apache.iotdb.db.queryengine.plan.relational.planner.rowpattern.ClassifierValuePointer;
import org.apache.iotdb.db.queryengine.plan.relational.planner.rowpattern.ExpressionAndValuePointers;
import org.apache.iotdb.db.queryengine.plan.relational.planner.rowpattern.ExpressionAndValuePointers.Assignment;
import org.apache.iotdb.db.queryengine.plan.relational.planner.rowpattern.IrLabel;
import org.apache.iotdb.db.queryengine.plan.relational.planner.rowpattern.IrRowPattern;
import org.apache.iotdb.db.queryengine.plan.relational.planner.rowpattern.LogicalIndexPointer;
import org.apache.iotdb.db.queryengine.plan.relational.planner.rowpattern.MatchNumberValuePointer;
import org.apache.iotdb.db.queryengine.plan.relational.planner.rowpattern.RowPatternToIrRewriter;
import org.apache.iotdb.db.queryengine.plan.relational.planner.rowpattern.ScalarValuePointer;
import org.apache.iotdb.db.queryengine.plan.relational.planner.rowpattern.ValuePointer;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.AliasedRelation;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.AsofJoinOn;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.AstVisitor;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.CoalesceExpression;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.ComparisonExpression;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.Delete;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.DereferenceExpression;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.Except;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.Expression;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.Identifier;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.InsertRow;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.InsertRows;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.InsertTablet;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.Intersect;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.Join;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.JoinCriteria;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.JoinOn;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.JoinUsing;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.LoadTsFile;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.LogicalExpression;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.MeasureDefinition;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.Node;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.PatternRecognitionRelation;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.PipeEnriched;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.QualifiedName;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.Query;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.QuerySpecification;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.Relation;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.RowPattern;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.SetOperation;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.SkipTo;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.SortItem;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.SubqueryExpression;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.SubsetDefinition;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.Table;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.TableFunctionInvocation;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.TableSubquery;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.Union;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.Values;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.VariableDefinition;
import org.apache.iotdb.db.queryengine.plan.relational.type.InternalTypeManager;
import org.apache.iotdb.db.queryengine.plan.statement.crud.InsertRowStatement;
import org.apache.iotdb.db.queryengine.plan.statement.crud.InsertRowsStatement;
import org.apache.iotdb.db.queryengine.plan.statement.crud.InsertTabletStatement;
import org.apache.iotdb.db.utils.cte.CteDataStore;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableListMultimap;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.ListMultimap;
import org.apache.tsfile.read.common.type.Type;
import org.apache.tsfile.write.schema.MeasurementSchema;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static org.apache.iotdb.commons.schema.table.InformationSchema.INFORMATION_DATABASE;
import static org.apache.iotdb.db.queryengine.plan.relational.analyzer.PatternRecognitionAnalysis.NavigationAnchor.LAST;
import static org.apache.iotdb.db.queryengine.plan.relational.analyzer.PatternRecognitionAnalysis.NavigationMode.RUNNING;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.OrderingTranslator.sortItemToSortOrder;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.PlanBuilder.newPlanBuilder;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.QueryPlanner.coerce;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.QueryPlanner.coerceIfNecessary;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.QueryPlanner.extractPatternRecognitionExpressions;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.QueryPlanner.pruneInvisibleFields;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.ir.IrUtils.extractPredicates;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.node.AggregationNode.singleAggregation;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.node.AggregationNode.singleGroupingSet;
import static org.apache.iotdb.db.queryengine.plan.relational.sql.ast.Join.Type.CROSS;
import static org.apache.iotdb.db.queryengine.plan.relational.sql.ast.Join.Type.FULL;
import static org.apache.iotdb.db.queryengine.plan.relational.sql.ast.Join.Type.IMPLICIT;
import static org.apache.iotdb.db.queryengine.plan.relational.sql.ast.Join.Type.INNER;
import static org.apache.iotdb.db.queryengine.plan.relational.sql.ast.Join.Type.LEFT;
import static org.apache.iotdb.db.queryengine.plan.relational.sql.ast.Join.Type.RIGHT;
import static org.apache.iotdb.db.queryengine.plan.relational.sql.ast.PatternRecognitionRelation.RowsPerMatch.ONE;
import static org.apache.iotdb.db.queryengine.plan.relational.sql.ast.SkipTo.Position.PAST_LAST;
import static org.apache.iotdb.db.queryengine.plan.relational.utils.NodeUtils.getSortItemsFromOrderBy;
import static org.apache.tsfile.read.common.type.LongType.INT64;
import static org.apache.tsfile.read.common.type.StringType.STRING;

public class RelationPlanner extends AstVisitor<RelationPlan, Void> {

  private final Analysis analysis;
  private final SymbolAllocator symbolAllocator;
  private final MPPQueryContext queryContext;
  private final QueryId idAllocator;
  private final Optional<TranslationMap> outerContext;
  private final SessionInfo sessionInfo;
  private final SubqueryPlanner subqueryPlanner;
  private final Map<NodeRef<Node>, RelationPlan> recursiveSubqueries;

  private final PredicateWithUncorrelatedScalarSubqueryReconstructor
      predicateWithUncorrelatedScalarSubqueryReconstructor;

  public RelationPlanner(
      final Analysis analysis,
      final SymbolAllocator symbolAllocator,
      final MPPQueryContext queryContext,
      final Optional<TranslationMap> outerContext,
      final SessionInfo sessionInfo,
      final Map<NodeRef<Node>, RelationPlan> recursiveSubqueries,
      PredicateWithUncorrelatedScalarSubqueryReconstructor
          predicateWithUncorrelatedScalarSubqueryReconstructor) {
    requireNonNull(analysis, "analysis is null");
    requireNonNull(symbolAllocator, "symbolAllocator is null");
    requireNonNull(queryContext, "queryContext is null");
    requireNonNull(outerContext, "outerContext is null");
    requireNonNull(sessionInfo, "session is null");
    requireNonNull(recursiveSubqueries, "recursiveSubqueries is null");
    requireNonNull(
        predicateWithUncorrelatedScalarSubqueryReconstructor,
        "predicateWithUncorrelatedScalarSubqueryReconstructor is null");

    this.analysis = analysis;
    this.symbolAllocator = symbolAllocator;
    this.queryContext = queryContext;
    this.idAllocator = queryContext.getQueryId();
    this.outerContext = outerContext;
    this.sessionInfo = sessionInfo;
    this.predicateWithUncorrelatedScalarSubqueryReconstructor =
        predicateWithUncorrelatedScalarSubqueryReconstructor;
    this.subqueryPlanner =
        new SubqueryPlanner(
            analysis,
            symbolAllocator,
            queryContext,
            outerContext,
            sessionInfo,
            recursiveSubqueries,
            predicateWithUncorrelatedScalarSubqueryReconstructor);
    this.recursiveSubqueries = recursiveSubqueries;
  }

  @Override
  protected RelationPlan visitQuery(final Query node, final Void context) {
    return new QueryPlanner(
            analysis,
            symbolAllocator,
            queryContext,
            outerContext,
            sessionInfo,
            recursiveSubqueries,
            predicateWithUncorrelatedScalarSubqueryReconstructor)
        .plan(node);
  }

  @Override
  protected RelationPlan visitTable(final Table table, final Void context) {
    // is this a recursive reference in expandable named query? If so, there's base relation already
    // planned.
    final RelationPlan expansion = recursiveSubqueries.get(NodeRef.of(table));
    if (expansion != null) {
      // put the pre-planned recursive subquery in the actual outer context to enable resolving
      // correlation
      return new RelationPlan(
          expansion.getRoot(), expansion.getScope(), expansion.getFieldMappings(), outerContext);
    }

    final Scope scope = analysis.getScope(table);
    final Query namedQuery = analysis.getNamedQuery(table);

    // Common Table Expression
    if (namedQuery != null) {
      return processNamedQuery(table, namedQuery, scope);
    }

    return processPhysicalTable(table, scope);
  }

  private RelationPlan processNamedQuery(Table table, Query namedQuery, Scope scope) {
    if (analysis.isExpandableQuery(namedQuery)) {
      throw new SemanticException("unexpected recursive cte");
    }

    if (namedQuery.isMaterialized() && namedQuery.isDone()) {
      RelationPlan materializedCtePlan = processMaterializedCte(table, namedQuery, scope);
      if (materializedCtePlan != null) {
        return materializedCtePlan;
      }
    }

    return processRegularCte(table, namedQuery, scope);
  }

  private RelationPlan processMaterializedCte(Table table, Query query, Scope scope) {
    CteDataStore dataStore = query.getCteDataStore();
    if (dataStore == null) {
      return null;
    }

    List<Symbol> cteSymbols =
        dataStore.getTableSchema().getColumns().stream()
            .map(column -> symbolAllocator.newSymbol(column.getName(), column.getType()))
            .collect(Collectors.toList());

    CteScanNode cteScanNode =
        new CteScanNode(idAllocator.genPlanNodeId(), table.getName(), cteSymbols, dataStore);

    List<Integer> columnIndex2TsBlockColumnIndexList =
        dataStore.getColumnIndex2TsBlockColumnIndexList();
    if (columnIndex2TsBlockColumnIndexList == null) {
      return new RelationPlan(cteScanNode, scope, cteSymbols, outerContext);
    }

    List<Symbol> outputSymbols = new ArrayList<>();
    Assignments.Builder assignments = Assignments.builder();
    for (int index : columnIndex2TsBlockColumnIndexList) {
      Symbol columnSymbol = cteSymbols.get(index);
      outputSymbols.add(columnSymbol);
      assignments.put(columnSymbol, columnSymbol.toSymbolReference());
    }

    // Project Node
    ProjectNode projectNode =
        new ProjectNode(
            queryContext.getQueryId().genPlanNodeId(), cteScanNode, assignments.build());

    return new RelationPlan(projectNode, scope, outputSymbols, outerContext);
  }

  private RelationPlan processRegularCte(Table table, Query namedQuery, Scope scope) {
    RelationPlan subPlan = process(namedQuery, null);
    // Add implicit coercions if view query produces types that don't match the declared output
    // types of the view (e.g., if the underlying tables referenced by the view changed)
    List<Type> types =
        analysis.getOutputDescriptor(table).getAllFields().stream()
            .map(Field::getType)
            .collect(toImmutableList());

    NodeAndMappings coerced = coerce(subPlan, types, symbolAllocator, idAllocator);
    return new RelationPlan(coerced.getNode(), scope, coerced.getFields(), outerContext);
  }

  private RelationPlan processPhysicalTable(Table table, Scope scope) {
    final ImmutableList.Builder<Symbol> outputSymbolsBuilder = ImmutableList.builder();
    final ImmutableMap.Builder<Symbol, ColumnSchema> symbolToColumnSchema = ImmutableMap.builder();
    final Collection<Field> fields = scope.getRelationType().getAllFields();
    final QualifiedName qualifiedName = analysis.getRelationName(table);

    if (!qualifiedName.getPrefix().isPresent()) {
      throw new IllegalStateException("Table " + table.getName() + " has no prefix!");
    }

    final QualifiedObjectName qualifiedObjectName =
        new QualifiedObjectName(
            qualifiedName.getPrefix().map(QualifiedName::toString).orElse(null),
            qualifiedName.getSuffix());

    // on the basis of that the order of fields is same with the column category order of segments
    // in DeviceEntry
    final Map<Symbol, Integer> tagAndAttributeIndexMap = new HashMap<>();
    int idIndex = 0;
    for (final Field field : fields) {
      final TsTableColumnCategory category = field.getColumnCategory();
      final Symbol symbol = symbolAllocator.newSymbol(field);
      outputSymbolsBuilder.add(symbol);
      symbolToColumnSchema.put(
          symbol,
          new ColumnSchema(
              field.getName().orElse(null), field.getType(), field.isHidden(), category));
      if (category == TsTableColumnCategory.TAG) {
        tagAndAttributeIndexMap.put(symbol, idIndex++);
      }
    }

    final List<Symbol> outputSymbols = outputSymbolsBuilder.build();
    final Map<Symbol, ColumnSchema> tableColumnSchema = symbolToColumnSchema.build();
    analysis.addTableSchema(qualifiedObjectName, tableColumnSchema);

    if (analysis.getTableHandle(table) instanceof TreeDeviceViewSchema) {
      TreeDeviceViewSchema treeDeviceViewSchema =
          (TreeDeviceViewSchema) analysis.getTableHandle(table);
      return new RelationPlan(
          new TreeDeviceViewScanNode(
              idAllocator.genPlanNodeId(),
              qualifiedObjectName,
              outputSymbols,
              tableColumnSchema,
              tagAndAttributeIndexMap,
              null,
              treeDeviceViewSchema.getColumn2OriginalNameMap()),
          scope,
          outputSymbols,
          outerContext);
    }

    TableScanNode tableScanNode =
        qualifiedObjectName.getDatabaseName().equals(INFORMATION_DATABASE)
            ? new InformationSchemaTableScanNode(
                idAllocator.genPlanNodeId(), qualifiedObjectName, outputSymbols, tableColumnSchema)
            : new DeviceTableScanNode(
                idAllocator.genPlanNodeId(),
                qualifiedObjectName,
                outputSymbols,
                tableColumnSchema,
                tagAndAttributeIndexMap);
    return new RelationPlan(tableScanNode, scope, outputSymbols, outerContext);
  }

  @Override
  protected RelationPlan visitQuerySpecification(
      final QuerySpecification node, final Void context) {
    return new QueryPlanner(
            analysis,
            symbolAllocator,
            queryContext,
            outerContext,
            sessionInfo,
            recursiveSubqueries,
            predicateWithUncorrelatedScalarSubqueryReconstructor)
        .plan(node);
  }

  @Override
  protected RelationPlan visitNode(final Node node, final Void context) {
    throw new IllegalStateException("Unsupported node type: " + node.getClass().getName());
  }

  @Override
  protected RelationPlan visitTableSubquery(final TableSubquery node, final Void context) {
    final RelationPlan plan = process(node.getQuery(), context);
    return new RelationPlan(
        plan.getRoot(), analysis.getScope(node), plan.getFieldMappings(), outerContext);
  }

  @Override
  protected RelationPlan visitJoin(final Join node, final Void context) {
    final RelationPlan leftPlan = process(node.getLeft(), context);
    final RelationPlan rightPlan = process(node.getRight(), context);

    if (node.getCriteria().isPresent() && node.getCriteria().get() instanceof JoinUsing) {
      return planJoinUsing(node, leftPlan, rightPlan);
    }

    Expression asofCriteria = null;
    if (node.getCriteria().isPresent()) {
      JoinCriteria criteria = node.getCriteria().get();
      checkArgument(criteria instanceof JoinOn);
      if (criteria instanceof AsofJoinOn) {
        asofCriteria = ((AsofJoinOn) criteria).getAsofExpression();
      }
    }
    return planJoin(
        analysis.getJoinCriteria(node),
        asofCriteria,
        node.getType(),
        analysis.getScope(node),
        leftPlan,
        rightPlan,
        analysis.getSubqueries(node));
  }

  private RelationPlan planJoinUsing(Join node, RelationPlan left, RelationPlan right) {
    /* Given: l JOIN r USING (k1, ..., kn)

       produces:

        - project
                coalesce(l.k1, r.k1)
                ...,
                coalesce(l.kn, r.kn)
                l.v1,
                ...,
                l.vn,
                r.v1,
                ...,
                r.vn
          - join (l.k1 = r.k1 and ... l.kn = r.kn)
                - project
                    cast(l.k1 as commonType(l.k1, r.k1))
                    ...
                - project
                    cast(rl.k1 as commonType(l.k1, r.k1))

        If casts are redundant (due to column type and common type being equal),
        they will be removed by optimization passes.
    */

    List<Identifier> joinColumns =
        ((JoinUsing)
                node.getCriteria()
                    .orElseThrow(() -> new IllegalStateException("JoinUsing criteria is empty")))
            .getColumns();

    Analysis.JoinUsingAnalysis joinAnalysis = analysis.getJoinUsing(node);

    ImmutableList.Builder<JoinNode.EquiJoinClause> clauses = ImmutableList.builder();

    Map<Identifier, Symbol> leftJoinColumns = new HashMap<>();
    Map<Identifier, Symbol> rightJoinColumns = new HashMap<>();

    Assignments.Builder leftCoercions = Assignments.builder();
    Assignments.Builder rightCoercions = Assignments.builder();

    leftCoercions.putIdentities(left.getRoot().getOutputSymbols());
    rightCoercions.putIdentities(right.getRoot().getOutputSymbols());
    for (int i = 0; i < joinColumns.size(); i++) {
      Identifier identifier = joinColumns.get(i);
      Type type = analysis.getType(identifier);

      // compute the coercion for the field on the left to the common supertype of left & right
      Symbol leftOutput = symbolAllocator.newSymbol(identifier, type);
      int leftField = joinAnalysis.getLeftJoinFields().get(i);
      // will not appear the situation: Cast(toSqlType(type))
      leftCoercions.put(leftOutput, left.getSymbol(leftField).toSymbolReference());
      leftJoinColumns.put(identifier, leftOutput);

      // compute the coercion for the field on the right to the common supertype of left & right
      Symbol rightOutput = symbolAllocator.newSymbol(identifier, type);
      int rightField = joinAnalysis.getRightJoinFields().get(i);
      rightCoercions.put(rightOutput, right.getSymbol(rightField).toSymbolReference());
      rightJoinColumns.put(identifier, rightOutput);

      clauses.add(new JoinNode.EquiJoinClause(leftOutput, rightOutput));
    }

    ProjectNode leftCoercion =
        new ProjectNode(
            queryContext.getQueryId().genPlanNodeId(), left.getRoot(), leftCoercions.build());
    ProjectNode rightCoercion =
        new ProjectNode(
            queryContext.getQueryId().genPlanNodeId(), right.getRoot(), rightCoercions.build());

    JoinNode join =
        new JoinNode(
            queryContext.getQueryId().genPlanNodeId(),
            mapJoinType(node.getType()),
            leftCoercion,
            rightCoercion,
            clauses.build(),
            Optional.empty(),
            leftCoercion.getOutputSymbols(),
            rightCoercion.getOutputSymbols(),
            Optional.empty(),
            Optional.empty());
    // Transform RIGHT JOIN to LEFT
    if (join.getJoinType() == JoinNode.JoinType.RIGHT) {
      join = join.flip();
    }

    // Add a projection to produce the outputs of the columns in the USING clause,
    // which are defined as coalesce(l.k, r.k)
    Assignments.Builder assignments = Assignments.builder();

    ImmutableList.Builder<Symbol> outputs = ImmutableList.builder();
    for (Identifier column : joinColumns) {
      Symbol output = symbolAllocator.newSymbol(column, analysis.getType(column));
      outputs.add(output);
      if (node.getType() == INNER || node.getType() == LEFT) {
        assignments.put(output, leftJoinColumns.get(column).toSymbolReference());
      } else if (node.getType() == FULL) {
        assignments.put(
            output,
            new CoalesceExpression(
                leftJoinColumns.get(column).toSymbolReference(),
                rightJoinColumns.get(column).toSymbolReference()));
      } else if (node.getType() == RIGHT) {
        assignments.put(output, rightJoinColumns.get(column).toSymbolReference());
      } else {
        throw new IllegalStateException("Unexpected Join Type: " + node.getType());
      }
    }

    for (int field : joinAnalysis.getOtherLeftFields()) {
      Symbol symbol = left.getFieldMappings().get(field);
      outputs.add(symbol);
      assignments.putIdentity(symbol);
    }

    for (int field : joinAnalysis.getOtherRightFields()) {
      Symbol symbol = right.getFieldMappings().get(field);
      outputs.add(symbol);
      assignments.putIdentity(symbol);
    }

    return new RelationPlan(
        new ProjectNode(queryContext.getQueryId().genPlanNodeId(), join, assignments.build()),
        analysis.getScope(node),
        outputs.build(),
        outerContext);
  }

  public RelationPlan planJoin(
      Expression criteria,
      Expression asofCriteria,
      Join.Type type,
      Scope scope,
      RelationPlan leftPlan,
      RelationPlan rightPlan,
      Analysis.SubqueryAnalysis subqueries) {
    // NOTE: symbols must be in the same order as the outputDescriptor
    List<Symbol> outputSymbols =
        ImmutableList.<Symbol>builder()
            .addAll(leftPlan.getFieldMappings())
            .addAll(rightPlan.getFieldMappings())
            .build();

    PlanBuilder leftPlanBuilder =
        newPlanBuilder(leftPlan, analysis).withScope(scope, outputSymbols);
    PlanBuilder rightPlanBuilder =
        newPlanBuilder(rightPlan, analysis).withScope(scope, outputSymbols);

    ImmutableList.Builder<JoinNode.EquiJoinClause> equiClauses = ImmutableList.builder();
    Optional<JoinNode.AsofJoinClause> asofJoinClause = Optional.empty();
    List<Expression> complexJoinExpressions = new ArrayList<>();
    List<Expression> postInnerJoinConditions = new ArrayList<>();

    RelationType left = leftPlan.getDescriptor();
    RelationType right = rightPlan.getDescriptor();

    if (type != CROSS && type != IMPLICIT) {
      List<Expression> leftComparisonExpressions = new ArrayList<>();
      List<Expression> rightComparisonExpressions = new ArrayList<>();
      List<ComparisonExpression.Operator> joinConditionComparisonOperators = new ArrayList<>();

      if (asofCriteria != null) {
        Expression firstExpression = ((ComparisonExpression) asofCriteria).getLeft();
        Expression secondExpression = ((ComparisonExpression) asofCriteria).getRight();
        ComparisonExpression.Operator comparisonOperator =
            ((ComparisonExpression) asofCriteria).getOperator();
        Set<QualifiedName> firstDependencies =
            SymbolsExtractor.extractNames(firstExpression, analysis.getColumnReferences());
        Set<QualifiedName> secondDependencies =
            SymbolsExtractor.extractNames(secondExpression, analysis.getColumnReferences());

        if (firstDependencies.stream().allMatch(left::canResolve)
            && secondDependencies.stream().allMatch(right::canResolve)) {
          leftComparisonExpressions.add(firstExpression);
          rightComparisonExpressions.add(secondExpression);
          joinConditionComparisonOperators.add(comparisonOperator);
        } else if (firstDependencies.stream().allMatch(right::canResolve)
            && secondDependencies.stream().allMatch(left::canResolve)) {
          leftComparisonExpressions.add(secondExpression);
          rightComparisonExpressions.add(firstExpression);
          joinConditionComparisonOperators.add(comparisonOperator.flip());
        } else {
          // the case when we mix symbols from both left and right join side on either side of
          // condition.
          throw new SemanticException(
              format("Complex ASOF main join expression [%s] is not supported", asofCriteria));
        }
      }

      if (criteria != null) {
        for (Expression conjunct : extractPredicates(LogicalExpression.Operator.AND, criteria)) {
          if (!isEqualComparisonExpression(conjunct) && type != INNER) {
            complexJoinExpressions.add(conjunct);
            continue;
          }

          Set<QualifiedName> dependencies =
              SymbolsExtractor.extractNames(conjunct, analysis.getColumnReferences());

          if (dependencies.stream().allMatch(left::canResolve)
              || dependencies.stream().allMatch(right::canResolve)) {
            // If the conjunct can be evaluated entirely with the inputs on either side of the join,
            // add
            // it to the list complex expressions and let the optimizers figure out how to push it
            // down later.
            complexJoinExpressions.add(conjunct);
          } else if (conjunct instanceof ComparisonExpression) {
            Expression firstExpression = ((ComparisonExpression) conjunct).getLeft();
            Expression secondExpression = ((ComparisonExpression) conjunct).getRight();
            ComparisonExpression.Operator comparisonOperator =
                ((ComparisonExpression) conjunct).getOperator();
            Set<QualifiedName> firstDependencies =
                SymbolsExtractor.extractNames(firstExpression, analysis.getColumnReferences());
            Set<QualifiedName> secondDependencies =
                SymbolsExtractor.extractNames(secondExpression, analysis.getColumnReferences());

            if (firstDependencies.stream().allMatch(left::canResolve)
                && secondDependencies.stream().allMatch(right::canResolve)) {
              leftComparisonExpressions.add(firstExpression);
              rightComparisonExpressions.add(secondExpression);
              joinConditionComparisonOperators.add(comparisonOperator);
            } else if (firstDependencies.stream().allMatch(right::canResolve)
                && secondDependencies.stream().allMatch(left::canResolve)) {
              leftComparisonExpressions.add(secondExpression);
              rightComparisonExpressions.add(firstExpression);
              joinConditionComparisonOperators.add(comparisonOperator.flip());
            } else {
              // the case when we mix symbols from both left and right join side on either side of
              // condition.
              complexJoinExpressions.add(conjunct);
            }
          } else {
            complexJoinExpressions.add(conjunct);
          }
        }
      }

      // leftPlanBuilder = subqueryPlanner.handleSubqueries(leftPlanBuilder,
      // leftComparisonExpressions, subqueries);
      // rightPlanBuilder = subqueryPlanner.handleSubqueries(rightPlanBuilder,
      // rightComparisonExpressions, subqueries);

      // Add projections for join criteria
      leftPlanBuilder =
          leftPlanBuilder.appendProjections(
              leftComparisonExpressions, symbolAllocator, queryContext);
      rightPlanBuilder =
          rightPlanBuilder.appendProjections(
              rightComparisonExpressions, symbolAllocator, queryContext);

      QueryPlanner.PlanAndMappings leftCoercions =
          coerce(
              leftPlanBuilder, leftComparisonExpressions, analysis, idAllocator, symbolAllocator);
      leftPlanBuilder = leftCoercions.getSubPlan();
      QueryPlanner.PlanAndMappings rightCoercions =
          coerce(
              rightPlanBuilder, rightComparisonExpressions, analysis, idAllocator, symbolAllocator);
      rightPlanBuilder = rightCoercions.getSubPlan();

      for (int i = 0; i < leftComparisonExpressions.size(); i++) {
        if (asofCriteria != null && i == 0) {
          Symbol leftSymbol = leftCoercions.get(leftComparisonExpressions.get(i));
          Symbol rightSymbol = rightCoercions.get(rightComparisonExpressions.get(i));

          asofJoinClause =
              Optional.of(
                  new JoinNode.AsofJoinClause(
                      joinConditionComparisonOperators.get(i), leftSymbol, rightSymbol));
          continue;
        }

        if (joinConditionComparisonOperators.get(i) == ComparisonExpression.Operator.EQUAL) {
          Symbol leftSymbol = leftCoercions.get(leftComparisonExpressions.get(i));
          Symbol rightSymbol = rightCoercions.get(rightComparisonExpressions.get(i));

          equiClauses.add(new JoinNode.EquiJoinClause(leftSymbol, rightSymbol));
        } else {
          postInnerJoinConditions.add(
              new ComparisonExpression(
                  joinConditionComparisonOperators.get(i),
                  leftCoercions.get(leftComparisonExpressions.get(i)).toSymbolReference(),
                  rightCoercions.get(rightComparisonExpressions.get(i)).toSymbolReference()));
        }
      }
    }

    PlanNode root =
        new JoinNode(
            idAllocator.genPlanNodeId(),
            mapJoinType(type),
            leftPlanBuilder.getRoot(),
            rightPlanBuilder.getRoot(),
            equiClauses.build(),
            asofJoinClause,
            leftPlanBuilder.getRoot().getOutputSymbols(),
            rightPlanBuilder.getRoot().getOutputSymbols(),
            Optional.empty(),
            Optional.empty());
    if (type == RIGHT && asofCriteria == null) {
      root = ((JoinNode) root).flip();
    }

    if (type != INNER) {
      for (Expression complexExpression : complexJoinExpressions) {
        Set<QualifiedName> dependencies =
            SymbolsExtractor.extractNamesNoSubqueries(
                complexExpression, analysis.getColumnReferences());

        // This is for handling uncorreled subqueries. Correlated subqueries are not currently
        // supported and are dealt with
        // during analysis.
        // Make best effort to plan the subquery in the branch of the join involving the other
        // inputs to the expression.
        // E.g.,
        //  t JOIN u ON t.x = (...) get's planned on the t side
        //  t JOIN u ON t.x = (...) get's planned on the u side
        //  t JOIN u ON t.x + u.x = (...) get's planned on an arbitrary side
        if (dependencies.stream().allMatch(left::canResolve)) {
          // leftPlanBuilder = subqueryPlanner.handleSubqueries(leftPlanBuilder, complexExpression,
          // subqueries);
        } else {
          // rightPlanBuilder = subqueryPlanner.handleSubqueries(rightPlanBuilder,
          // complexExpression, subqueries);
        }
      }
    }
    TranslationMap translationMap =
        new TranslationMap(
                Optional.empty(),
                scope,
                analysis,
                outputSymbols,
                new PlannerContext(new TableMetadataImpl(), new InternalTypeManager()))
            .withAdditionalMappings(leftPlanBuilder.getTranslations().getMappings())
            .withAdditionalMappings(rightPlanBuilder.getTranslations().getMappings());

    if (type != INNER && !complexJoinExpressions.isEmpty()) {
      root =
          new JoinNode(
              idAllocator.genPlanNodeId(),
              mapJoinType(type),
              leftPlanBuilder.getRoot(),
              rightPlanBuilder.getRoot(),
              equiClauses.build(),
              asofJoinClause,
              leftPlanBuilder.getRoot().getOutputSymbols(),
              rightPlanBuilder.getRoot().getOutputSymbols(),
              Optional.of(
                  IrUtils.and(
                      complexJoinExpressions.stream()
                          .map(e -> coerceIfNecessary(analysis, e, translationMap.rewrite(e)))
                          .collect(Collectors.toList()))),
              Optional.empty());
      if (type == RIGHT && asofCriteria == null) {
        root = ((JoinNode) root).flip();
      }
    }

    if (type == INNER) {
      // rewrite all the other conditions using output symbols from left + right plan node.
      PlanBuilder rootPlanBuilder = new PlanBuilder(translationMap, root);
      // rootPlanBuilder = subqueryPlanner.handleSubqueries(rootPlanBuilder, complexJoinExpressions,
      // subqueries);

      for (Expression expression : complexJoinExpressions) {
        postInnerJoinConditions.add(
            coerceIfNecessary(analysis, expression, rootPlanBuilder.rewrite(expression)));
      }
      root = rootPlanBuilder.getRoot();

      Expression postInnerJoinCriteria;
      if (!postInnerJoinConditions.isEmpty()) {
        postInnerJoinCriteria = IrUtils.and(postInnerJoinConditions);
        root = new FilterNode(idAllocator.genPlanNodeId(), root, postInnerJoinCriteria);
      }
    }

    return new RelationPlan(root, scope, outputSymbols, outerContext);
  }

  public static JoinNode.JoinType mapJoinType(Join.Type joinType) {
    switch (joinType) {
      case CROSS:
      case IMPLICIT:
      case INNER:
        return JoinNode.JoinType.INNER;
      case LEFT:
        return JoinNode.JoinType.LEFT;
      case RIGHT:
        return JoinNode.JoinType.RIGHT;
      case FULL:
        return JoinNode.JoinType.FULL;
    }
    throw new UnsupportedOperationException(joinType + " Join type is not supported");
  }

  private static boolean isEqualComparisonExpression(Expression conjunct) {
    return conjunct instanceof ComparisonExpression
        && ((ComparisonExpression) conjunct).getOperator() == ComparisonExpression.Operator.EQUAL;
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

    return new RelationPlan(root, analysis.getScope(node), mappings, outerContext);
  }

  @Override
  protected RelationPlan visitSubqueryExpression(SubqueryExpression node, Void context) {
    return process(node.getQuery(), context);
  }

  @Override
  protected RelationPlan visitPatternRecognitionRelation(
      PatternRecognitionRelation node, Void context) {
    RelationPlan subPlan = process(node.getInput(), context);

    // Pre-project inputs for PARTITION BY and ORDER BY
    List<Expression> inputs =
        ImmutableList.<Expression>builder()
            .addAll(node.getPartitionBy())
            .addAll(
                getSortItemsFromOrderBy(node.getOrderBy()).stream()
                    .map(SortItem::getSortKey)
                    .collect(toImmutableList()))
            .build();

    PlanBuilder planBuilder = newPlanBuilder(subPlan, analysis);

    // no handleSubqueries because subqueries are not allowed here
    planBuilder = planBuilder.appendProjections(inputs, symbolAllocator, queryContext);

    ImmutableList.Builder<Symbol> outputLayout = ImmutableList.builder();
    RowsPerMatch rowsPerMatch = mapRowsPerMatch(node.getRowsPerMatch().orElse(ONE));
    boolean oneRowOutput = rowsPerMatch.isOneRow();

    // Rewrite PARTITION BY
    ImmutableList.Builder<Symbol> partitionBySymbols = ImmutableList.builder();
    for (Expression expression : node.getPartitionBy()) {
      partitionBySymbols.add(planBuilder.translate(expression));
    }
    List<Symbol> partitionBy = partitionBySymbols.build();

    // Rewrite ORDER BY
    LinkedHashMap<Symbol, SortOrder> orderings = new LinkedHashMap<>();
    for (SortItem item : getSortItemsFromOrderBy(node.getOrderBy())) {
      Symbol symbol = planBuilder.translate(item.getSortKey());
      // don't override existing keys, i.e. when "ORDER BY a ASC, a DESC" is specified
      orderings.putIfAbsent(symbol, sortItemToSortOrder(item));
    }

    Optional<OrderingScheme> orderingScheme = Optional.empty();
    if (!orderings.isEmpty()) {
      orderingScheme =
          Optional.of(new OrderingScheme(ImmutableList.copyOf(orderings.keySet()), orderings));
    }

    outputLayout.addAll(partitionBy);
    if (!oneRowOutput) {
      getSortItemsFromOrderBy(node.getOrderBy()).stream()
          .map(SortItem::getSortKey)
          .map(planBuilder::translate)
          .forEach(outputLayout::add);
    }

    List<Expression> expressions =
        extractPatternRecognitionExpressions(node.getVariableDefinitions(), node.getMeasures());
    planBuilder =
        subqueryPlanner.handleSubqueries(planBuilder, expressions, analysis.getSubqueries(node));

    PatternRecognitionComponents components =
        planPatternRecognitionComponents(
            planBuilder.getTranslations(),
            node.getSubsets(),
            node.getMeasures(),
            node.getAfterMatchSkipTo(),
            node.getPattern(),
            node.getVariableDefinitions());

    outputLayout.addAll(components.getMeasureOutputs());

    for (Expression expr : expressions) {
      predicateWithUncorrelatedScalarSubqueryReconstructor.clearShadowExpression(expr);
    }

    if (!oneRowOutput) {
      Set<Symbol> inputSymbolsOnOutput = ImmutableSet.copyOf(outputLayout.build());
      subPlan.getFieldMappings().stream()
          .filter(symbol -> !inputSymbolsOnOutput.contains(symbol))
          .forEach(outputLayout::add);
    }

    PatternRecognitionNode planNode =
        new PatternRecognitionNode(
            idAllocator.genPlanNodeId(),
            planBuilder.getRoot(),
            partitionBy,
            orderingScheme,
            Optional.empty(),
            components.getMeasures(),
            rowsPerMatch,
            components.getSkipToLabels(),
            components.getSkipToPosition(),
            components.getPattern(),
            components.getVariableDefinitions());

    return new RelationPlan(planNode, analysis.getScope(node), outputLayout.build(), outerContext);
  }

  private RowsPerMatch mapRowsPerMatch(PatternRecognitionRelation.RowsPerMatch rowsPerMatch) {
    switch (rowsPerMatch) {
      case ONE:
        return RowsPerMatch.ONE;
      case ALL_SHOW_EMPTY:
        return RowsPerMatch.ALL_SHOW_EMPTY;
      case ALL_OMIT_EMPTY:
        return RowsPerMatch.ALL_OMIT_EMPTY;
      case ALL_WITH_UNMATCHED:
        return RowsPerMatch.ALL_WITH_UNMATCHED;
      default:
        throw new SemanticException("Unexpected rows per match: " + rowsPerMatch);
    }
  }

  public PatternRecognitionComponents planPatternRecognitionComponents(
      TranslationMap translations,
      List<SubsetDefinition> subsets,
      List<MeasureDefinition> measures,
      Optional<SkipTo> skipTo,
      RowPattern pattern,
      List<VariableDefinition> variableDefinitions) {
    // NOTE: There might be aggregate functions in measure definitions and variable definitions.
    // They are handled different than top level aggregations in a query:
    // 1. Their arguments are not pre-projected and replaced with single symbols. This is because
    // the arguments might
    //    not be eligible for pre-projection, when they contain references to CLASSIFIER() or
    // MATCH_NUMBER() functions
    //    which are evaluated at runtime. If some aggregation arguments can be pre-projected, it
    // will be done in the
    //    Optimizer.
    // 2. Their arguments do not need to be coerced by hand. Since the pattern aggregation arguments
    // are rewritten as
    //    parts of enclosing expressions, and not as standalone expressions, all necessary coercions
    // will be applied by the
    //    TranslationMap.

    // rewrite subsets
    ImmutableMap.Builder<IrLabel, Set<IrLabel>> rewrittenSubsetsBuilder = ImmutableMap.builder();
    for (SubsetDefinition subsetDefinition : subsets) {
      String label = analysis.getResolvedLabel(subsetDefinition.getName());
      Set<IrLabel> elements =
          analysis.getSubsetLabels(subsetDefinition).stream()
              .map(IrLabel::new)
              .collect(toImmutableSet());
      rewrittenSubsetsBuilder.put(new IrLabel(label), elements);
    }
    Map<IrLabel, Set<IrLabel>> rewrittenSubsets = rewrittenSubsetsBuilder.buildOrThrow();

    // rewrite measures
    ImmutableMap.Builder<Symbol, Measure> rewrittenMeasures = ImmutableMap.builder();
    ImmutableList.Builder<Symbol> measureOutputs = ImmutableList.builder();

    for (MeasureDefinition definition : measures) {
      Type type = analysis.getType(definition.getExpression());
      Symbol symbol = symbolAllocator.newSymbol(definition.getName().getValue(), type);
      ExpressionAndValuePointers measure =
          planPatternRecognitionExpression(
              translations,
              rewrittenSubsets,
              definition.getName().getValue(),
              definition.getExpression());
      rewrittenMeasures.put(symbol, new Measure(measure, type));
      measureOutputs.add(symbol);
    }

    // rewrite variable definitions
    ImmutableMap.Builder<IrLabel, ExpressionAndValuePointers> rewrittenVariableDefinitions =
        ImmutableMap.builder();
    for (VariableDefinition definition : variableDefinitions) {
      String label = analysis.getResolvedLabel(definition.getName());
      ExpressionAndValuePointers variable =
          planPatternRecognitionExpression(
              translations,
              rewrittenSubsets,
              definition.getName().getValue(),
              definition.getExpression());
      rewrittenVariableDefinitions.put(new IrLabel(label), variable);
    }
    // add `true` definition for undefined labels
    for (String label : analysis.getUndefinedLabels(pattern)) {
      IrLabel irLabel = new IrLabel(label);
      rewrittenVariableDefinitions.put(irLabel, ExpressionAndValuePointers.TRUE);
    }

    Set<IrLabel> skipToLabels =
        skipTo
            .flatMap(SkipTo::getIdentifier)
            .map(Identifier::getValue)
            .map(
                label ->
                    rewrittenSubsets.getOrDefault(
                        new IrLabel(label), ImmutableSet.of(new IrLabel(label))))
            .orElse(ImmutableSet.of());

    return new PatternRecognitionComponents(
        rewrittenMeasures.buildOrThrow(),
        measureOutputs.build(),
        skipToLabels,
        mapSkipToPosition(skipTo.map(SkipTo::getPosition).orElse(PAST_LAST)),
        RowPatternToIrRewriter.rewrite(pattern, analysis),
        rewrittenVariableDefinitions.buildOrThrow());
  }

  private ExpressionAndValuePointers planPatternRecognitionExpression(
      TranslationMap translations,
      Map<IrLabel, Set<IrLabel>> subsets,
      String name,
      Expression expression) {
    Map<NodeRef<Expression>, Symbol> patternVariableTranslations = new HashMap<>();

    ImmutableList.Builder<Assignment> assignments = ImmutableList.builder();
    for (PatternRecognitionAnalysis.PatternFunctionAnalysis accessor :
        analysis.getPatternInputsAnalysis(expression)) {
      ValuePointer pointer;
      if (accessor.getDescriptor() instanceof MatchNumberDescriptor) {
        pointer = new MatchNumberValuePointer();
      } else if (accessor.getDescriptor() instanceof ClassifierDescriptor) {
        ClassifierDescriptor descriptor = (ClassifierDescriptor) accessor.getDescriptor();
        pointer =
            new ClassifierValuePointer(
                planValuePointer(descriptor.getLabel(), descriptor.getNavigation(), subsets));
      } else if (accessor.getDescriptor() instanceof ScalarInputDescriptor) {
        ScalarInputDescriptor descriptor = (ScalarInputDescriptor) accessor.getDescriptor();
        pointer =
            new ScalarValuePointer(
                planValuePointer(descriptor.getLabel(), descriptor.getNavigation(), subsets),
                Symbol.from(translations.rewrite(accessor.getExpression())));
      } else if (accessor.getDescriptor() instanceof AggregationDescriptor) {
        AggregationDescriptor descriptor = (AggregationDescriptor) accessor.getDescriptor();

        Map<NodeRef<Expression>, Symbol> mappings = new HashMap<>();

        Optional<Symbol> matchNumberSymbol = Optional.empty();
        if (!descriptor.getMatchNumberCalls().isEmpty()) {
          Symbol symbol = symbolAllocator.newSymbol("match_number", INT64);
          for (Expression call : descriptor.getMatchNumberCalls()) {
            mappings.put(NodeRef.of(call), symbol);
          }
          matchNumberSymbol = Optional.of(symbol);
        }

        Optional<Symbol> classifierSymbol = Optional.empty();
        if (!descriptor.getClassifierCalls().isEmpty()) {
          Symbol symbol = symbolAllocator.newSymbol("classifier", STRING);

          for (Expression call : descriptor.getClassifierCalls()) {
            mappings.put(NodeRef.of(call), symbol);
          }
          classifierSymbol = Optional.of(symbol);
        }

        TranslationMap argumentTranslation = translations.withAdditionalIdentityMappings(mappings);

        Set<IrLabel> labels =
            descriptor.getLabels().stream()
                .flatMap(label -> planLabels(Optional.of(label), subsets).stream())
                .collect(Collectors.toSet());

        pointer =
            new AggregationValuePointer(
                descriptor.getFunction(),
                new AggregationLabelSet(labels, descriptor.getMode() == RUNNING),
                descriptor.getArguments().stream()
                    .filter(
                        argument -> !DereferenceExpression.isQualifiedAllFieldsReference(argument))
                    .map(
                        argument ->
                            coerceIfNecessary(
                                analysis, argument, argumentTranslation.rewrite(argument)))
                    .collect(Collectors.toList()),
                classifierSymbol,
                matchNumberSymbol);
      } else {
        throw new SemanticException(
            "Unexpected descriptor type: " + accessor.getDescriptor().getClass().getName());
      }

      Symbol symbol = symbolAllocator.newSymbol(name, analysis.getType(accessor.getExpression()));
      assignments.add(new Assignment(symbol, pointer));

      patternVariableTranslations.put(NodeRef.of(accessor.getExpression()), symbol);
    }

    Expression rewritten =
        translations
            .withAdditionalIdentityMappings(patternVariableTranslations)
            .rewrite(expression);

    return new ExpressionAndValuePointers(rewritten, assignments.build());
  }

  private Set<IrLabel> planLabels(Optional<String> label, Map<IrLabel, Set<IrLabel>> subsets) {
    return label
        .map(IrLabel::new)
        .map(value -> subsets.getOrDefault(value, ImmutableSet.of(value)))
        .orElse(ImmutableSet.of());
  }

  private LogicalIndexPointer planValuePointer(
      Optional<String> label, Navigation navigation, Map<IrLabel, Set<IrLabel>> subsets) {
    return new LogicalIndexPointer(
        planLabels(label, subsets),
        navigation.getAnchor() == LAST,
        navigation.getMode() == RUNNING,
        navigation.getLogicalOffset(),
        navigation.getPhysicalOffset());
  }

  private SkipToPosition mapSkipToPosition(SkipTo.Position position) {
    switch (position) {
      case NEXT:
        return SkipToPosition.NEXT;
      case PAST_LAST:
        return SkipToPosition.PAST_LAST;
      case FIRST:
        return SkipToPosition.FIRST;
      case LAST:
        return SkipToPosition.LAST;
      default:
        throw new SemanticException("Unexpected skip to position: " + position);
    }
  }

  @Override
  protected RelationPlan visitUnion(Union node, Void context) {
    Preconditions.checkArgument(!node.getRelations().isEmpty(), "No relations specified for UNION");

    SetOperationPlan setOperationPlan = process(node);

    PlanNode planNode =
        new UnionNode(
            idAllocator.genPlanNodeId(),
            setOperationPlan.getChildren(),
            setOperationPlan.getSymbolMapping(),
            ImmutableList.copyOf(setOperationPlan.getSymbolMapping().keySet()));
    if (node.isDistinct()) {
      planNode = distinct(planNode);
    }
    return new RelationPlan(
        planNode, analysis.getScope(node), planNode.getOutputSymbols(), outerContext);
  }

  @Override
  protected RelationPlan visitIntersect(Intersect node, Void context) {
    Preconditions.checkArgument(
        !node.getRelations().isEmpty(), "No relations specified for intersect");
    SetOperationPlan setOperationPlan = process(node);

    PlanNode intersectNode =
        new IntersectNode(
            idAllocator.genPlanNodeId(),
            setOperationPlan.getChildren(),
            setOperationPlan.getSymbolMapping(),
            ImmutableList.copyOf(setOperationPlan.getSymbolMapping().keySet()),
            node.isDistinct());

    return new RelationPlan(
        intersectNode, analysis.getScope(node), intersectNode.getOutputSymbols(), outerContext);
  }

  @Override
  protected RelationPlan visitExcept(Except node, Void context) {
    Preconditions.checkArgument(
        !node.getRelations().isEmpty(), "No relations specified for except");
    SetOperationPlan setOperationPlan = process(node);

    PlanNode exceptNode =
        new ExceptNode(
            idAllocator.genPlanNodeId(),
            setOperationPlan.getChildren(),
            setOperationPlan.getSymbolMapping(),
            ImmutableList.copyOf(setOperationPlan.getSymbolMapping().keySet()),
            node.isDistinct());

    return new RelationPlan(
        exceptNode, analysis.getScope(node), exceptNode.getOutputSymbols(), outerContext);
  }

  private SetOperationPlan process(SetOperation node) {
    RelationType outputFields = analysis.getOutputDescriptor(node);
    List<Symbol> outputs =
        outputFields.getAllFields().stream()
            .map(symbolAllocator::newSymbol)
            .collect(toImmutableList());

    ImmutableListMultimap.Builder<Symbol, Symbol> symbolMapping = ImmutableListMultimap.builder();
    ImmutableList.Builder<PlanNode> children = ImmutableList.builder();

    for (Relation child : node.getRelations()) {
      RelationPlan plan = process(child, null);

      NodeAndMappings planAndMappings;
      List<Type> types = analysis.getRelationCoercion(child);
      if (types == null) {
        // no coercion required, only prune invisible fields from child outputs
        planAndMappings = pruneInvisibleFields(plan, idAllocator);
      } else {
        // apply required coercion and prune invisible fields from child outputs
        planAndMappings = coerce(plan, types, symbolAllocator, idAllocator);
      }
      for (int i = 0; i < outputFields.getAllFields().size(); i++) {
        symbolMapping.put(outputs.get(i), planAndMappings.getFields().get(i));
      }

      children.add(planAndMappings.getNode());
    }
    return new SetOperationPlan(children.build(), symbolMapping.build());
  }

  private PlanNode distinct(PlanNode node) {
    return singleAggregation(
        idAllocator.genPlanNodeId(),
        node,
        ImmutableMap.of(),
        singleGroupingSet(node.getOutputSymbols()));
  }

  // ================================ Implemented later =====================================

  @Override
  protected RelationPlan visitValues(Values node, Void context) {
    throw new IllegalStateException("Values is not supported in current version.");
  }

  @Override
  protected RelationPlan visitInsertTablet(InsertTablet node, Void context) {
    final InsertTabletStatement insertTabletStatement = node.getInnerTreeStatement();

    String[] measurements = insertTabletStatement.getMeasurements();
    MeasurementSchema[] measurementSchemas = insertTabletStatement.getMeasurementSchemas();
    stayConsistent(measurements, measurementSchemas);

    RelationalInsertTabletNode insertNode =
        new RelationalInsertTabletNode(
            idAllocator.genPlanNodeId(),
            insertTabletStatement.getDevicePath(),
            insertTabletStatement.isAligned(),
            measurements,
            insertTabletStatement.getDataTypes(),
            measurementSchemas,
            insertTabletStatement.getTimes(),
            insertTabletStatement.getBitMaps(),
            insertTabletStatement.getColumns(),
            insertTabletStatement.getRowCount(),
            insertTabletStatement.getColumnCategories());
    insertNode.setFailedMeasurementNumber(insertTabletStatement.getFailedMeasurementNumber());
    if (insertTabletStatement.isSingleDevice()) {
      insertNode.setSingleDevice();
    }
    return new RelationPlan(
        insertNode, analysis.getRootScope(), Collections.emptyList(), outerContext);
  }

  @Override
  protected RelationPlan visitInsertRow(InsertRow node, Void context) {
    InsertRowStatement insertRowStatement = node.getInnerTreeStatement();
    RelationalInsertRowNode insertNode = fromInsertRowStatement(insertRowStatement);
    return new RelationPlan(
        insertNode, analysis.getRootScope(), Collections.emptyList(), outerContext);
  }

  protected RelationalInsertRowNode fromInsertRowStatement(
      final InsertRowStatement insertRowStatement) {

    String[] measurements = insertRowStatement.getMeasurements();
    MeasurementSchema[] measurementSchemas = insertRowStatement.getMeasurementSchemas();
    stayConsistent(measurements, measurementSchemas);

    final RelationalInsertRowNode insertNode =
        new RelationalInsertRowNode(
            idAllocator.genPlanNodeId(),
            insertRowStatement.getDevicePath(),
            insertRowStatement.isAligned(),
            measurements,
            insertRowStatement.getDataTypes(),
            measurementSchemas,
            insertRowStatement.getTime(),
            insertRowStatement.getValues(),
            insertRowStatement.isNeedInferType(),
            insertRowStatement.getColumnCategories());
    insertNode.setFailedMeasurementNumber(insertRowStatement.getFailedMeasurementNumber());
    return insertNode;
  }

  @Override
  protected RelationPlan visitInsertRows(final InsertRows node, final Void context) {
    final InsertRowsStatement insertRowsStatement = node.getInnerTreeStatement();
    final List<Integer> indices = new ArrayList<>();
    final List<InsertRowNode> insertRowStatements = new ArrayList<>();
    for (int i = 0; i < insertRowsStatement.getInsertRowStatementList().size(); i++) {
      indices.add(i);
      insertRowStatements.add(
          fromInsertRowStatement(insertRowsStatement.getInsertRowStatementList().get(i)));
    }
    final RelationalInsertRowsNode relationalInsertRowsNode =
        new RelationalInsertRowsNode(idAllocator.genPlanNodeId(), indices, insertRowStatements);
    return new RelationPlan(
        relationalInsertRowsNode, analysis.getRootScope(), Collections.emptyList(), outerContext);
  }

  @Override
  protected RelationPlan visitLoadTsFile(final LoadTsFile node, final Void context) {
    final List<Boolean> isTableModel = new ArrayList<>();
    for (int i = 0; i < node.getResources().size(); i++) {
      isTableModel.add(node.getIsTableModel().get(i));
    }
    return new RelationPlan(
        new LoadTsFileNode(
            idAllocator.genPlanNodeId(), node.getResources(), isTableModel, node.getDatabase()),
        analysis.getRootScope(),
        Collections.emptyList(),
        outerContext);
  }

  @Override
  protected RelationPlan visitPipeEnriched(final PipeEnriched node, final Void context) {
    final RelationPlan relationPlan = node.getInnerStatement().accept(this, context);

    if (relationPlan.getRoot() instanceof LoadTsFileNode) {
      return relationPlan;
    } else if (relationPlan.getRoot() instanceof InsertNode) {
      return new RelationPlan(
          new PipeEnrichedInsertNode((InsertNode) relationPlan.getRoot()),
          analysis.getRootScope(),
          Collections.emptyList(),
          outerContext);
    } else if (relationPlan.getRoot() instanceof RelationalDeleteDataNode) {
      return new RelationPlan(
          new PipeEnrichedDeleteDataNode((RelationalDeleteDataNode) relationPlan.getRoot()),
          analysis.getRootScope(),
          Collections.emptyList(),
          outerContext);
    }

    return new RelationPlan(
        new PipeEnrichedWritePlanNode((WritePlanNode) relationPlan.getRoot()),
        analysis.getRootScope(),
        Collections.emptyList(),
        outerContext);
  }

  @Override
  protected RelationPlan visitDelete(final Delete node, final Void context) {
    return new RelationPlan(
        new RelationalDeleteDataNode(idAllocator.genPlanNodeId(), node),
        analysis.getRootScope(),
        Collections.emptyList(),
        outerContext);
  }

  @Override
  public RelationPlan visitTableFunctionInvocation(TableFunctionInvocation node, Void context) {
    TableFunctionInvocationAnalysis functionAnalysis = analysis.getTableFunctionAnalysis(node);

    ImmutableList.Builder<PlanNode> sources = ImmutableList.builder();
    ImmutableList.Builder<TableFunctionNode.TableArgumentProperties> sourceProperties =
        ImmutableList.builder();
    ImmutableList.Builder<Symbol> outputSymbols = ImmutableList.builder();

    // create new symbols for table function's proper columns
    RelationType relationType = analysis.getScope(node).getRelationType();
    List<Symbol> properOutputs =
        IntStream.range(0, functionAnalysis.getProperColumnsCount())
            .mapToObj(relationType::getFieldByIndex)
            .map(symbolAllocator::newSymbol)
            .collect(toImmutableList());

    outputSymbols.addAll(properOutputs);

    // process sources in order of argument declarations
    for (TableArgumentAnalysis tableArgument : functionAnalysis.getTableArgumentAnalyses()) {
      RelationPlan sourcePlan = process(tableArgument.getRelation(), context);
      PlanBuilder sourcePlanBuilder = newPlanBuilder(sourcePlan, analysis);

      // required columns are a subset of visible columns of the source. remap required column
      // indexes to field indexes in source relation type.
      RelationType sourceRelationType = sourcePlan.getScope().getRelationType();
      int[] fieldIndexForVisibleColumn = new int[sourceRelationType.getVisibleFieldCount()];
      int visibleColumn = 0;
      for (int i = 0; i < sourceRelationType.getAllFieldCount(); i++) {
        if (!sourceRelationType.getFieldByIndex(i).isHidden()) {
          fieldIndexForVisibleColumn[visibleColumn] = i;
          visibleColumn++;
        }
      }
      List<Symbol> requiredColumns =
          functionAnalysis.getRequiredColumns().get(tableArgument.getArgumentName()).stream()
              .map(column -> fieldIndexForVisibleColumn[column])
              .map(sourcePlan::getSymbol)
              .collect(toImmutableList());

      Optional<DataOrganizationSpecification> specification = Optional.empty();

      // if the table argument has set semantics, create Specification
      if (!tableArgument.isRowSemantics()) {
        // partition by
        List<Symbol> partitionBy = ImmutableList.of();
        // if there are partitioning columns, they might have to be coerced for copartitioning
        if (tableArgument.getPartitionBy().isPresent()
            && !tableArgument.getPartitionBy().get().isEmpty()) {
          List<Expression> partitioningColumns = tableArgument.getPartitionBy().get();
          QueryPlanner.PlanAndMappings copartitionCoercions =
              coerce(
                  sourcePlanBuilder, partitioningColumns, analysis, idAllocator, symbolAllocator);
          sourcePlanBuilder = copartitionCoercions.getSubPlan();
          partitionBy =
              partitioningColumns.stream()
                  .map(copartitionCoercions::get)
                  .collect(toImmutableList());
          analysis.setSortNode(true);
        }

        // order by
        Optional<OrderingScheme> orderBy = Optional.empty();
        if (tableArgument.getOrderBy().isPresent()) {
          // the ordering symbols are not coerced
          orderBy =
              Optional.of(
                  QueryPlanner.translateOrderingScheme(
                      tableArgument.getOrderBy().get().getSortItems(),
                      sourcePlanBuilder::translate));
          analysis.setSortNode(true);
        }

        if (!partitionBy.isEmpty() || orderBy.isPresent()) {
          specification = Optional.of(new DataOrganizationSpecification(partitionBy, orderBy));
        }
      }

      // add output symbols passed from the table argument
      ImmutableList.Builder<TableFunctionNode.PassThroughColumn> passThroughColumns =
          ImmutableList.builder();
      if (tableArgument.isPassThroughColumns()) {
        // the original output symbols from the source node, not coerced
        // note: hidden columns are included. They are present in sourcePlan.fieldMappings
        outputSymbols.addAll(sourcePlan.getFieldMappings());
        Set<Symbol> partitionBy =
            specification
                .map(DataOrganizationSpecification::getPartitionBy)
                .map(ImmutableSet::copyOf)
                .orElse(ImmutableSet.of());
        sourcePlan.getFieldMappings().stream()
            .map(
                symbol ->
                    new TableFunctionNode.PassThroughColumn(symbol, partitionBy.contains(symbol)))
            .forEach(passThroughColumns::add);
      } else if (tableArgument.getPartitionBy().isPresent()) {
        tableArgument.getPartitionBy().get().stream()
            // the original symbols for partitioning columns, not coerced
            .map(sourcePlanBuilder::translate)
            .forEach(
                symbol -> {
                  outputSymbols.add(symbol);
                  passThroughColumns.add(new TableFunctionNode.PassThroughColumn(symbol, true));
                });
      }

      sources.add(sourcePlanBuilder.getRoot());
      sourceProperties.add(
          new TableFunctionNode.TableArgumentProperties(
              tableArgument.getArgumentName(),
              tableArgument.isRowSemantics(),
              new TableFunctionNode.PassThroughSpecification(
                  tableArgument.isPassThroughColumns(), passThroughColumns.build()),
              requiredColumns,
              specification,
              functionAnalysis.isRequiredRecordSnapshot()));
    }

    PlanNode root =
        new TableFunctionNode(
            idAllocator.genPlanNodeId(),
            functionAnalysis.getFunctionName(),
            functionAnalysis.getTableFunctionHandle(),
            properOutputs,
            sources.build(),
            sourceProperties.build());

    return new RelationPlan(root, analysis.getScope(node), outputSymbols.build(), outerContext);
  }

  private static void stayConsistent(
      String[] measurements, MeasurementSchema[] measurementSchemas) {
    int minLength = Math.min(measurements.length, measurementSchemas.length);
    for (int j = 0; j < minLength; j++) {
      if (measurements[j] == null || measurementSchemas[j] == null) {
        measurements[j] = null;
        measurementSchemas[j] = null;
      }
    }
  }

  private static final class SetOperationPlan {
    private final List<PlanNode> children;
    private final ListMultimap<Symbol, Symbol> symbolMapping;

    private SetOperationPlan(List<PlanNode> children, ListMultimap<Symbol, Symbol> symbolMapping) {
      this.children = children;
      this.symbolMapping = symbolMapping;
    }

    public List<PlanNode> getChildren() {
      return children;
    }

    public ListMultimap<Symbol, Symbol> getSymbolMapping() {
      return symbolMapping;
    }
  }

  public static class PatternRecognitionComponents {
    private final Map<Symbol, Measure> measures;
    private final List<Symbol> measureOutputs;
    private final Set<IrLabel> skipToLabels;
    private final SkipToPosition skipToPosition;
    private final IrRowPattern pattern;
    private final Map<IrLabel, ExpressionAndValuePointers> variableDefinitions;

    public PatternRecognitionComponents(
        Map<Symbol, Measure> measures,
        List<Symbol> measureOutputs,
        Set<IrLabel> skipToLabels,
        SkipToPosition skipToPosition,
        IrRowPattern pattern,
        Map<IrLabel, ExpressionAndValuePointers> variableDefinitions) {
      this.measures = requireNonNull(measures, "measures is null");
      this.measureOutputs = requireNonNull(measureOutputs, "measureOutputs is null");
      this.skipToLabels = ImmutableSet.copyOf(skipToLabels);
      this.skipToPosition = requireNonNull(skipToPosition, "skipToPosition is null");
      this.pattern = requireNonNull(pattern, "pattern is null");
      this.variableDefinitions = requireNonNull(variableDefinitions, "variableDefinitions is null");
    }

    public Map<Symbol, Measure> getMeasures() {
      return measures;
    }

    public List<Symbol> getMeasureOutputs() {
      return measureOutputs;
    }

    public Set<IrLabel> getSkipToLabels() {
      return skipToLabels;
    }

    public SkipToPosition getSkipToPosition() {
      return skipToPosition;
    }

    public IrRowPattern getPattern() {
      return pattern;
    }

    public Map<IrLabel, ExpressionAndValuePointers> getVariableDefinitions() {
      return variableDefinitions;
    }
  }
}
