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
import org.apache.iotdb.db.queryengine.common.MPPQueryContext;
import org.apache.iotdb.db.queryengine.common.QueryId;
import org.apache.iotdb.db.queryengine.common.SessionInfo;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.load.LoadTsFileNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.pipe.PipeEnrichedInsertNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.write.InsertNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.write.InsertRowNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.write.RelationalDeleteDataNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.write.RelationalInsertRowNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.write.RelationalInsertRowsNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.write.RelationalInsertTabletNode;
import org.apache.iotdb.db.queryengine.plan.relational.analyzer.Analysis;
import org.apache.iotdb.db.queryengine.plan.relational.analyzer.Field;
import org.apache.iotdb.db.queryengine.plan.relational.analyzer.NodeRef;
import org.apache.iotdb.db.queryengine.plan.relational.analyzer.RelationType;
import org.apache.iotdb.db.queryengine.plan.relational.analyzer.Scope;
import org.apache.iotdb.db.queryengine.plan.relational.metadata.ColumnSchema;
import org.apache.iotdb.db.queryengine.plan.relational.metadata.QualifiedObjectName;
import org.apache.iotdb.db.queryengine.plan.relational.metadata.TableMetadataImpl;
import org.apache.iotdb.db.queryengine.plan.relational.planner.ir.IrUtils;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.DeviceTableScanNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.FilterNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.InformationSchemaTableScanNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.JoinNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.ProjectNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.TableScanNode;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.AliasedRelation;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.AstVisitor;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.CoalesceExpression;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.ComparisonExpression;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.Delete;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.Except;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.Expression;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.Identifier;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.InsertRow;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.InsertRows;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.InsertTablet;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.Intersect;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.Join;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.JoinUsing;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.LoadTsFile;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.LogicalExpression;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.Node;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.PipeEnriched;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.QualifiedName;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.Query;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.QuerySpecification;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.SubqueryExpression;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.Table;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.TableSubquery;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.Union;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.Values;
import org.apache.iotdb.db.queryengine.plan.relational.type.InternalTypeManager;
import org.apache.iotdb.db.queryengine.plan.statement.crud.InsertRowStatement;
import org.apache.iotdb.db.queryengine.plan.statement.crud.InsertRowsStatement;
import org.apache.iotdb.db.queryengine.plan.statement.crud.InsertTabletStatement;
import org.apache.iotdb.db.storageengine.load.config.LoadTsFileConfigurator;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.apache.tsfile.read.common.type.Type;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import static java.util.Objects.requireNonNull;
import static org.apache.iotdb.commons.schema.table.InformationSchemaTable.INFORMATION_SCHEMA;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.PlanBuilder.newPlanBuilder;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.QueryPlanner.coerce;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.QueryPlanner.coerceIfNecessary;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.ir.IrUtils.extractPredicates;
import static org.apache.iotdb.db.queryengine.plan.relational.sql.ast.Join.Type.CROSS;
import static org.apache.iotdb.db.queryengine.plan.relational.sql.ast.Join.Type.FULL;
import static org.apache.iotdb.db.queryengine.plan.relational.sql.ast.Join.Type.IMPLICIT;
import static org.apache.iotdb.db.queryengine.plan.relational.sql.ast.Join.Type.INNER;

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
          expansion.getRoot(), expansion.getScope(), expansion.getFieldMappings(), outerContext);
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

    // on the basis of that the order of fields is same with the column category order of segments
    // in DeviceEntry
    Map<Symbol, Integer> idAndAttributeIndexMap = new HashMap<>();
    int idIndex = 0;
    for (Field field : fields) {
      TsTableColumnCategory category = field.getColumnCategory();
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
        qualifiedObjectName.getDatabaseName().equals(INFORMATION_SCHEMA)
            ? new InformationSchemaTableScanNode(
                idAllocator.genPlanNodeId(), qualifiedObjectName, outputSymbols, tableColumnSchema)
            : new DeviceTableScanNode(
                idAllocator.genPlanNodeId(),
                qualifiedObjectName,
                outputSymbols,
                tableColumnSchema,
                idAndAttributeIndexMap);
    return new RelationPlan(tableScanNode, scope, outputSymbols, outerContext);

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
    return new RelationPlan(
        plan.getRoot(), analysis.getScope(node), plan.getFieldMappings(), outerContext);
  }

  @Override
  protected RelationPlan visitJoin(Join node, Void context) {
    RelationPlan leftPlan = process(node.getLeft(), context);
    RelationPlan rightPlan = process(node.getRight(), context);

    if (node.getCriteria().isPresent() && node.getCriteria().get() instanceof JoinUsing) {
      return planJoinUsing(node, leftPlan, rightPlan);
    }

    return planJoin(
        analysis.getJoinCriteria(node),
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
            leftCoercion.getOutputSymbols(),
            rightCoercion.getOutputSymbols(),
            Optional.empty(),
            Optional.empty());

    // Add a projection to produce the outputs of the columns in the USING clause,
    // which are defined as coalesce(l.k, r.k)
    Assignments.Builder assignments = Assignments.builder();

    ImmutableList.Builder<Symbol> outputs = ImmutableList.builder();
    for (Identifier column : joinColumns) {
      Symbol output = symbolAllocator.newSymbol(column, analysis.getType(column));
      outputs.add(output);
      if (node.getType() == INNER) {
        assignments.put(output, leftJoinColumns.get(column).toSymbolReference());
      } else if (node.getType() == FULL) {
        assignments.put(
            output,
            new CoalesceExpression(
                leftJoinColumns.get(column).toSymbolReference(),
                rightJoinColumns.get(column).toSymbolReference()));
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
    List<Expression> complexJoinExpressions = new ArrayList<>();
    List<Expression> postInnerJoinConditions = new ArrayList<>();

    RelationType left = leftPlan.getDescriptor();
    RelationType right = rightPlan.getDescriptor();

    if (type != CROSS && type != IMPLICIT) {
      List<Expression> leftComparisonExpressions = new ArrayList<>();
      List<Expression> rightComparisonExpressions = new ArrayList<>();
      List<ComparisonExpression.Operator> joinConditionComparisonOperators = new ArrayList<>();

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
            leftPlanBuilder.getRoot().getOutputSymbols(),
            rightPlanBuilder.getRoot().getOutputSymbols(),
            Optional.empty(),
            Optional.empty());

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
              leftPlanBuilder.getRoot().getOutputSymbols(),
              rightPlanBuilder.getRoot().getOutputSymbols(),
              Optional.of(
                  IrUtils.and(
                      complexJoinExpressions.stream()
                          .map(e -> coerceIfNecessary(analysis, e, translationMap.rewrite(e)))
                          .collect(Collectors.toList()))),
              Optional.empty());
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

  // ================================ Implemented later =====================================

  @Override
  protected RelationPlan visitValues(Values node, Void context) {
    throw new IllegalStateException("Values is not supported in current version.");
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
        relationalInsertRowsNode, analysis.getRootScope(), Collections.emptyList(), outerContext);
  }

  @Override
  protected RelationPlan visitLoadTsFile(LoadTsFile node, Void context) {
    final List<Boolean> isTableModel = new ArrayList<>();
    for (int i = 0; i < node.getResources().size(); i++) {
      isTableModel.add(node.getModel().equals(LoadTsFileConfigurator.MODEL_TABLE_VALUE));
    }
    return new RelationPlan(
        new LoadTsFileNode(
            idAllocator.genPlanNodeId(), node.getResources(), isTableModel, node.getDatabase()),
        analysis.getRootScope(),
        Collections.emptyList(),
        outerContext);
  }

  @Override
  protected RelationPlan visitPipeEnriched(PipeEnriched node, Void context) {
    RelationPlan relationPlan = node.getInnerStatement().accept(this, context);

    if (relationPlan.getRoot() instanceof LoadTsFileNode) {
      return relationPlan;
    } else if (relationPlan.getRoot() instanceof InsertNode) {
      return new RelationPlan(
          new PipeEnrichedInsertNode((InsertNode) relationPlan.getRoot()),
          analysis.getRootScope(),
          Collections.emptyList(),
          outerContext);
    }
    throw new IllegalStateException("Other WritePlanNode is not supported in current version.");
  }

  @Override
  protected RelationPlan visitDelete(Delete node, Void context) {
    return new RelationPlan(
        new RelationalDeleteDataNode(idAllocator.genPlanNodeId(), node),
        analysis.getRootScope(),
        Collections.emptyList(),
        outerContext);
  }
}
