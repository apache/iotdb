/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iotdb.db.queryengine.plan.relational.analyzer;

import org.apache.iotdb.commons.schema.table.TsTable;
import org.apache.iotdb.commons.schema.table.column.TsTableColumnCategory;
import org.apache.iotdb.commons.schema.table.column.TsTableColumnSchema;
import org.apache.iotdb.db.exception.sql.SemanticException;
import org.apache.iotdb.db.queryengine.common.MPPQueryContext;
import org.apache.iotdb.db.queryengine.common.SessionInfo;
import org.apache.iotdb.db.queryengine.execution.warnings.IoTDBWarning;
import org.apache.iotdb.db.queryengine.execution.warnings.WarningCollector;
import org.apache.iotdb.db.queryengine.plan.analyze.AnalyzeUtils;
import org.apache.iotdb.db.queryengine.plan.analyze.QueryType;
import org.apache.iotdb.db.queryengine.plan.analyze.schema.SchemaValidator;
import org.apache.iotdb.db.queryengine.plan.relational.metadata.ColumnSchema;
import org.apache.iotdb.db.queryengine.plan.relational.metadata.Metadata;
import org.apache.iotdb.db.queryengine.plan.relational.metadata.QualifiedObjectName;
import org.apache.iotdb.db.queryengine.plan.relational.metadata.TableSchema;
import org.apache.iotdb.db.queryengine.plan.relational.planner.PlannerContext;
import org.apache.iotdb.db.queryengine.plan.relational.planner.ScopeAware;
import org.apache.iotdb.db.queryengine.plan.relational.planner.Symbol;
import org.apache.iotdb.db.queryengine.plan.relational.planner.TranslationMap;
import org.apache.iotdb.db.queryengine.plan.relational.security.AccessControl;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.AbstractQueryDeviceWithCache;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.AbstractTraverseDevice;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.AddColumn;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.AliasedRelation;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.AllColumns;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.AllRows;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.AstVisitor;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.ComparisonExpression;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.CountDevice;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.CreateDB;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.CreateIndex;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.CreateOrUpdateDevice;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.CreateTable;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.Delete;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.DereferenceExpression;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.DescribeTable;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.DropColumn;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.DropDB;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.DropFunction;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.DropIndex;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.DropTable;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.Except;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.Explain;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.ExplainAnalyze;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.Expression;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.FetchDevice;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.FieldReference;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.FunctionCall;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.GroupBy;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.GroupingElement;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.GroupingSets;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.Identifier;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.Insert;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.InsertRow;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.InsertRows;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.InsertTablet;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.Intersect;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.Join;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.JoinCriteria;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.JoinOn;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.JoinUsing;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.Limit;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.LongLiteral;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.NaturalJoin;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.Node;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.Offset;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.OrderBy;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.Property;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.QualifiedName;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.Query;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.QuerySpecification;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.Relation;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.RenameColumn;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.RenameTable;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.Row;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.Select;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.SelectItem;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.SetOperation;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.SetProperties;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.ShowDB;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.ShowDevice;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.ShowFunctions;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.ShowIndex;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.ShowTables;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.SimpleGroupBy;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.SingleColumn;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.SortItem;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.Statement;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.SubqueryExpression;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.SymbolReference;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.Table;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.TableSubquery;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.Union;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.Update;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.UpdateAssignment;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.Use;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.Values;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.With;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.WithQuery;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.WrappedInsertStatement;
import org.apache.iotdb.db.queryengine.plan.statement.crud.InsertBaseStatement;
import org.apache.iotdb.db.schemaengine.table.DataNodeTableCache;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableMultimap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.ListMultimap;
import com.google.common.collect.Streams;
import org.apache.tsfile.read.common.type.RowType;
import org.apache.tsfile.read.common.type.Type;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static com.google.common.collect.Iterables.getLast;
import static com.google.common.collect.Iterables.getOnlyElement;
import static java.lang.Math.toIntExact;
import static java.util.Collections.emptyList;
import static java.util.Locale.ENGLISH;
import static java.util.Objects.requireNonNull;
import static org.apache.iotdb.commons.schema.table.TsTable.TABLE_ALLOWED_PROPERTIES_2_DEFAULT_VALUE_MAP;
import static org.apache.iotdb.commons.schema.table.TsTable.TIME_COLUMN_NAME;
import static org.apache.iotdb.db.queryengine.execution.warnings.StandardWarningCode.REDUNDANT_ORDER_BY;
import static org.apache.iotdb.db.queryengine.plan.relational.analyzer.AggregationAnalyzer.verifyOrderByAggregations;
import static org.apache.iotdb.db.queryengine.plan.relational.analyzer.AggregationAnalyzer.verifySourceAggregations;
import static org.apache.iotdb.db.queryengine.plan.relational.analyzer.CanonicalizationAware.canonicalizationAwareKey;
import static org.apache.iotdb.db.queryengine.plan.relational.analyzer.ExpressionTreeUtils.asQualifiedName;
import static org.apache.iotdb.db.queryengine.plan.relational.analyzer.ExpressionTreeUtils.extractAggregateFunctions;
import static org.apache.iotdb.db.queryengine.plan.relational.analyzer.Scope.BasisType.TABLE;
import static org.apache.iotdb.db.queryengine.plan.relational.metadata.MetadataUtil.createQualifiedObjectName;
import static org.apache.iotdb.db.queryengine.plan.relational.sql.ast.Join.Type.FULL;
import static org.apache.iotdb.db.queryengine.plan.relational.sql.ast.Join.Type.INNER;
import static org.apache.iotdb.db.queryengine.plan.relational.sql.ast.Join.Type.LEFT;
import static org.apache.iotdb.db.queryengine.plan.relational.sql.ast.Join.Type.RIGHT;
import static org.apache.iotdb.db.queryengine.plan.relational.sql.util.AstUtil.preOrder;
import static org.apache.iotdb.db.queryengine.plan.relational.utils.NodeUtils.getSortItemsFromOrderBy;
import static org.apache.tsfile.read.common.type.BooleanType.BOOLEAN;

public class StatementAnalyzer {

  private final StatementAnalyzerFactory statementAnalyzerFactory;

  private Analysis analysis;
  private final MPPQueryContext queryContext;

  private final AccessControl accessControl;

  private final WarningCollector warningCollector;

  private final SessionInfo sessionContext;

  private final Metadata metadata;

  private final CorrelationSupport correlationSupport;

  public static final String ONLY_SUPPORT_TIME_COLUMN_EQUI_JOIN =
      "Only support time column equi-join in current version";

  public static final String ONLY_SUPPORT_TIME_COLUMN_IN_USING_CLAUSE =
      "Only support time column as the parameter in JOIN USING";

  public StatementAnalyzer(
      StatementAnalyzerFactory statementAnalyzerFactory,
      Analysis analysis,
      MPPQueryContext queryContext,
      AccessControl accessControl,
      WarningCollector warningCollector,
      SessionInfo sessionContext,
      Metadata metadata,
      CorrelationSupport correlationSupport) {
    this.statementAnalyzerFactory = statementAnalyzerFactory;
    this.analysis = analysis;
    this.queryContext = queryContext;
    this.accessControl = accessControl;
    this.warningCollector = warningCollector;
    this.sessionContext = sessionContext;
    this.metadata = metadata;
    this.correlationSupport = correlationSupport;
  }

  public Scope analyze(Node node) {
    return analyze(node, Optional.empty(), true);
  }

  public Scope analyze(Node node, Scope outerQueryScope) {
    return analyze(node, Optional.of(outerQueryScope), false);
  }

  private Scope analyze(Node node, Optional<Scope> outerQueryScope, boolean isTopLevel) {
    return new Visitor(outerQueryScope, warningCollector, Optional.empty(), isTopLevel)
        .process(node, Optional.empty());
  }

  public Scope analyzeForUpdate(
      Relation relation, Optional<Scope> outerQueryScope, UpdateKind updateKind) {
    return new Visitor(outerQueryScope, warningCollector, Optional.of(updateKind), true)
        .process(relation, Optional.empty());
  }

  private enum UpdateKind {
    DELETE,
    UPDATE,
    MERGE,
  }

  /**
   * Visitor context represents local query scope (if exists). The invariant is that the local query
   * scopes hierarchy should always have outer query scope (if provided) as ancestor.
   */
  private final class Visitor extends AstVisitor<Scope, Optional<Scope>> {

    private final boolean isTopLevel;
    private final Optional<Scope> outerQueryScope;
    private final WarningCollector warningCollector;
    private final Optional<UpdateKind> updateKind;

    private Visitor(
        Optional<Scope> outerQueryScope,
        WarningCollector warningCollector,
        Optional<UpdateKind> updateKind,
        boolean isTopLevel) {
      this.outerQueryScope = requireNonNull(outerQueryScope, "outerQueryScope is null");
      this.warningCollector = requireNonNull(warningCollector, "warningCollector is null");
      this.updateKind = requireNonNull(updateKind, "updateKind is null");
      this.isTopLevel = isTopLevel;
    }

    @Override
    public Scope process(final Node node, final Optional<Scope> scope) {
      final Scope returnScope = super.process(node, scope);
      if (node instanceof CreateOrUpdateDevice
          || node instanceof FetchDevice
          || node instanceof ShowDevice
          || node instanceof CountDevice
          || node instanceof Update) {
        return returnScope;
      }
      checkState(
          returnScope.getOuterQueryParent().equals(outerQueryScope),
          "result scope should have outer query scope equal with parameter outer query scope");
      scope.ifPresent(
          value ->
              checkState(
                  hasScopeAsLocalParent(returnScope, value),
                  "return scope should have context scope as one of its ancestors"));
      return returnScope;
    }

    private Scope process(Node node, Scope scope) {
      return process(node, Optional.of(scope));
    }

    @Override
    protected Scope visitNode(Node node, Optional<Scope> context) {
      throw new IllegalStateException("Unsupported node type: " + node.getClass().getName());
    }

    @Override
    protected Scope visitCreateDB(CreateDB node, Optional<Scope> context) {
      throw new SemanticException("Create Database statement is not supported yet.");
    }

    @Override
    protected Scope visitDropDB(DropDB node, Optional<Scope> context) {
      throw new SemanticException("Drop Database statement is not supported yet.");
    }

    @Override
    protected Scope visitShowDB(ShowDB node, Optional<Scope> context) {
      throw new SemanticException("Show Database statement is not supported yet.");
    }

    @Override
    protected Scope visitCreateTable(final CreateTable node, final Optional<Scope> context) {
      validateProperties(node.getProperties(), context);
      return createAndAssignScope(node, context);
    }

    @Override
    protected Scope visitDropTable(final DropTable node, final Optional<Scope> context) {
      return createAndAssignScope(node, context);
    }

    @Override
    protected Scope visitShowTables(ShowTables node, Optional<Scope> context) {
      throw new SemanticException("Show Tables statement is not supported yet.");
    }

    @Override
    protected Scope visitRenameTable(RenameTable node, Optional<Scope> context) {
      return createAndAssignScope(node, context);
    }

    @Override
    protected Scope visitDescribeTable(DescribeTable node, Optional<Scope> context) {
      throw new SemanticException("Describe Table statement is not supported yet.");
    }

    @Override
    protected Scope visitSetProperties(final SetProperties node, final Optional<Scope> context) {
      validateProperties(node.getProperties(), context);
      return createAndAssignScope(node, context);
    }

    @Override
    protected Scope visitRenameColumn(RenameColumn node, Optional<Scope> context) {
      return createAndAssignScope(node, context);
    }

    @Override
    protected Scope visitDropColumn(DropColumn node, Optional<Scope> context) {
      return createAndAssignScope(node, context);
    }

    @Override
    protected Scope visitAddColumn(AddColumn node, Optional<Scope> context) {
      throw new SemanticException("Add Column statement is not supported yet.");
    }

    @Override
    protected Scope visitCreateIndex(CreateIndex node, Optional<Scope> context) {
      throw new SemanticException("Create Index statement is not supported yet.");
    }

    @Override
    protected Scope visitDropIndex(DropIndex node, Optional<Scope> context) {
      throw new SemanticException("Drop Index statement is not supported yet.");
    }

    @Override
    protected Scope visitShowIndex(ShowIndex node, Optional<Scope> context) {
      throw new SemanticException("Show Index statement is not supported yet.");
    }

    @Override
    protected Scope visitUpdate(final Update node, final Optional<Scope> context) {
      queryContext.setQueryType(QueryType.WRITE);
      final TranslationMap translationMap = analyzeTraverseDevice(node, context, true);
      final TsTable table =
          DataNodeTableCache.getInstance().getTable(node.getDatabase(), node.getTableName());
      node.parseRawExpression(
          null,
          table,
          table.getColumnList().stream()
              .filter(
                  columnSchema ->
                      columnSchema.getColumnCategory().equals(TsTableColumnCategory.ATTRIBUTE))
              .map(TsTableColumnSchema::getColumnName)
              .collect(Collectors.toList()),
          queryContext);

      final Set<SymbolReference> attributeNames = new HashSet<>();
      node.setAssignments(
          node.getAssignments().stream()
              .map(
                  assignment -> {
                    final Expression parsedColumn =
                        analyzeAndRewriteExpression(
                            translationMap, translationMap.getScope(), assignment.getName());
                    if (!(parsedColumn instanceof SymbolReference)
                        || table
                                .getColumnSchema(((SymbolReference) parsedColumn).getName())
                                .getColumnCategory()
                            != TsTableColumnCategory.ATTRIBUTE) {
                      throw new SemanticException("Update can only specify attribute columns.");
                    }
                    if (attributeNames.contains(parsedColumn)) {
                      throw new SemanticException(
                          "Update attribute shall specify a attribute only once.");
                    }
                    attributeNames.add((SymbolReference) parsedColumn);

                    return new UpdateAssignment(
                        parsedColumn,
                        analyzeAndRewriteExpression(
                            translationMap, translationMap.getScope(), assignment.getValue()));
                  })
              .collect(Collectors.toList()));
      return null;
    }

    @Override
    protected Scope visitDropFunction(DropFunction node, Optional<Scope> context) {
      throw new SemanticException("Drop Function statement is not supported yet.");
    }

    @Override
    protected Scope visitShowFunctions(ShowFunctions node, Optional<Scope> context) {
      throw new SemanticException("Show Function statement is not supported yet.");
    }

    @Override
    protected Scope visitUse(Use node, Optional<Scope> scope) {
      throw new SemanticException("USE statement is not supported yet.");
    }

    @Override
    protected Scope visitInsert(Insert insert, Optional<Scope> scope) {
      throw new SemanticException("Insert statement is not supported yet.");
    }

    @Override
    protected Scope visitInsertRow(InsertRow node, Optional<Scope> context) {
      return visitInsert(node, context);
    }

    protected Scope visitInsertTablet(InsertTablet insert, Optional<Scope> scope) {
      return visitInsert(insert, scope);
    }

    @Override
    protected Scope visitInsertRows(InsertRows node, Optional<Scope> context) {
      return visitInsert(node, context);
    }

    private Scope visitInsert(WrappedInsertStatement insert, Optional<Scope> scope) {
      final Scope ret = Scope.create();

      final MPPQueryContext context = insert.getContext();
      InsertBaseStatement innerInsert = insert.getInnerTreeStatement();

      innerInsert.semanticCheck();
      innerInsert =
          AnalyzeUtils.analyzeInsert(
              context,
              innerInsert,
              () -> SchemaValidator.validate(metadata, insert, context),
              metadata::getOrCreateDataPartition,
              AnalyzeUtils::computeTableDataPartitionParams,
              analysis,
              false);
      insert.setInnerTreeStatement(innerInsert);
      analysis.setScope(insert, ret);

      return ret;
    }

    @Override
    protected Scope visitDelete(Delete node, Optional<Scope> scope) {
      throw new SemanticException("Delete statement is not supported yet.");
    }

    @Override
    protected Scope visitExplain(Explain node, Optional<Scope> context) {
      analysis.setFinishQueryAfterAnalyze();
      return visitQuery((Query) node.getStatement(), context);
    }

    @Override
    protected Scope visitExplainAnalyze(ExplainAnalyze node, Optional<Scope> context) {
      throw new SemanticException("Explain Analyze statement is not supported yet.");
    }

    @Override
    protected Scope visitQuery(Query node, Optional<Scope> context) {
      Scope withScope = analyzeWith(node, context);
      Scope queryBodyScope = process(node.getQueryBody(), withScope);

      List<Expression> orderByExpressions = emptyList();
      if (node.getOrderBy().isPresent()) {
        orderByExpressions =
            analyzeOrderBy(node, getSortItemsFromOrderBy(node.getOrderBy()), queryBodyScope);

        if ((queryBodyScope.getOuterQueryParent().isPresent() || !isTopLevel)
            && !node.getLimit().isPresent()
            && !node.getOffset().isPresent()) {
          // not the root scope and ORDER BY is ineffective
          analysis.markRedundantOrderBy(node.getOrderBy().get());
          warningCollector.add(
              new IoTDBWarning(REDUNDANT_ORDER_BY, "ORDER BY in subquery may have no effect"));
        }
      }
      analysis.setOrderByExpressions(node, orderByExpressions);

      if (node.getOffset().isPresent()) {
        analyzeOffset(node.getOffset().get(), queryBodyScope);
      }

      if (node.getLimit().isPresent()) {
        boolean requiresOrderBy = analyzeLimit(node.getLimit().get(), queryBodyScope);
        if (requiresOrderBy && !node.getOrderBy().isPresent()) {
          throw new SemanticException("FETCH FIRST WITH TIES clause requires ORDER BY");
        }
      }

      // Input fields == Output fields
      analysis.setSelectExpressions(
          node,
          descriptorToFields(queryBodyScope).stream()
              .map(expression -> new Analysis.SelectExpression(expression, Optional.empty()))
              .collect(toImmutableList()));

      Scope queryScope =
          Scope.builder()
              .withParent(withScope)
              .withRelationType(RelationId.of(node), queryBodyScope.getRelationType())
              .build();

      analysis.setScope(node, queryScope);
      return queryScope;
    }

    private List<Expression> descriptorToFields(Scope scope) {
      ImmutableList.Builder<Expression> builder = ImmutableList.builder();
      for (int fieldIndex = 0;
          fieldIndex < scope.getRelationType().getAllFieldCount();
          fieldIndex++) {
        FieldReference expression = new FieldReference(fieldIndex);
        builder.add(expression);
        analyzeExpression(expression, scope);
      }
      return builder.build();
    }

    private Scope analyzeWith(Query node, Optional<Scope> scope) {
      if (!node.getWith().isPresent()) {
        return createScope(scope);
      }

      // analyze WITH clause
      With with = node.getWith().get();
      Scope.Builder withScopeBuilder = scopeBuilder(scope);

      for (WithQuery withQuery : with.getQueries()) {
        String name = withQuery.getName().getValue().toLowerCase(ENGLISH);
        if (withScopeBuilder.containsNamedQuery(name)) {
          throw new SemanticException(
              String.format("WITH query name '%s' specified more than once", name));
        }

        boolean isRecursive = false;
        if (with.isRecursive()) {
          // cannot nest pattern recognition within recursive query

          isRecursive = tryProcessRecursiveQuery(withQuery, name, withScopeBuilder);
          // WITH query is not shaped accordingly to the rules for expandable query and will be
          // processed like a plain WITH query.
          // Since RECURSIVE is specified, any reference to WITH query name is considered a
          // recursive reference and is not allowed.
          if (!isRecursive) {
            List<Node> recursiveReferences =
                findReferences(withQuery.getQuery(), withQuery.getName());
            if (!recursiveReferences.isEmpty()) {
              throw new SemanticException("recursive reference not allowed in this context");
            }
          }
        }

        if (!isRecursive) {
          Query query = withQuery.getQuery();
          analyze(query, withScopeBuilder.build());

          // check if all or none of the columns are explicitly alias
          if (withQuery.getColumnNames().isPresent()) {
            validateColumnAliases(
                withQuery.getColumnNames().get(),
                analysis.getOutputDescriptor(query).getVisibleFieldCount());
          }

          withScopeBuilder.withNamedQuery(name, withQuery);
        }
      }
      Scope withScope = withScopeBuilder.build();
      analysis.setScope(with, withScope);
      return withScope;
    }

    private boolean tryProcessRecursiveQuery(
        WithQuery withQuery, String name, Scope.Builder withScopeBuilder) {
      if (!withQuery.getColumnNames().isPresent()) {
        throw new SemanticException("missing column aliases in recursive WITH query");
      }
      preOrder(withQuery.getQuery())
          .filter(child -> child instanceof With && ((With) child).isRecursive())
          .findFirst()
          .ifPresent(
              child -> {
                throw new SemanticException("nested recursive WITH query");
              });
      // if RECURSIVE is specified, all queries in the WITH list are considered potentially
      // recursive
      // try resolve WITH query as expandable query
      // a) validate shape of the query and location of recursive reference
      if (!(withQuery.getQuery().getQueryBody() instanceof Union)) {
        return false;
      }
      Union union = (Union) withQuery.getQuery().getQueryBody();
      if (union.getRelations().size() != 2) {
        return false;
      }
      Relation anchor = union.getRelations().get(0);
      Relation step = union.getRelations().get(1);
      List<Node> anchorReferences = findReferences(anchor, withQuery.getName());
      if (!anchorReferences.isEmpty()) {
        throw new SemanticException(
            "WITH table name is referenced in the base relation of recursion");
      }
      // a WITH query is linearly recursive if it has a single recursive reference
      List<Node> stepReferences = findReferences(step, withQuery.getName());
      if (stepReferences.size() > 1) {
        throw new SemanticException(
            "multiple recursive references in the step relation of recursion");
      }
      if (stepReferences.size() != 1) {
        return false;
      }
      // search for QuerySpecification in parenthesized subquery
      Relation specification = step;
      while (specification instanceof TableSubquery) {
        Query query = ((TableSubquery) specification).getQuery();
        query
            .getLimit()
            .ifPresent(
                limit -> {
                  throw new SemanticException(
                      "FETCH FIRST / LIMIT clause in the step relation of recursion");
                });
        specification = query.getQueryBody();
      }
      if (!(specification instanceof QuerySpecification)
          || !((QuerySpecification) specification).getFrom().isPresent()) {
        throw new SemanticException(
            "recursive reference outside of FROM clause of the step relation of recursion");
      }
      Relation from = ((QuerySpecification) specification).getFrom().get();
      List<Node> fromReferences = findReferences(from, withQuery.getName());
      if (fromReferences.isEmpty()) {
        throw new SemanticException(
            "recursive reference outside of FROM clause of the step relation of recursion");
      }

      // b) validate top-level shape of recursive query
      withQuery
          .getQuery()
          .getWith()
          .ifPresent(
              innerWith -> {
                throw new SemanticException(
                    "immediate WITH clause in recursive query is not supported");
              });
      withQuery
          .getQuery()
          .getOrderBy()
          .ifPresent(
              orderBy -> {
                throw new SemanticException(
                    "immediate ORDER BY clause in recursive query is not supported");
              });
      withQuery
          .getQuery()
          .getOffset()
          .ifPresent(
              offset -> {
                throw new SemanticException(
                    "immediate OFFSET clause in recursive query is not supported");
              });
      withQuery
          .getQuery()
          .getLimit()
          .ifPresent(
              limit -> {
                throw new SemanticException(
                    "immediate FETCH FIRST / LIMIT clause in recursive query is not support");
              });

      // c) validate recursion step has no illegal clauses
      validateFromClauseOfRecursiveTerm(from, withQuery.getName());

      // shape validation complete - process query as expandable query
      Scope parentScope = withScopeBuilder.build();
      // process expandable query -- anchor
      Scope anchorScope = process(anchor, parentScope);
      // set aliases in anchor scope as defined for WITH query. Recursion step will refer to anchor
      // fields by aliases.
      Scope aliasedAnchorScope =
          setAliases(anchorScope, withQuery.getName(), withQuery.getColumnNames().get());
      // record expandable query base scope for recursion step analysis
      Node recursiveReference = fromReferences.get(0);
      analysis.setExpandableBaseScope(recursiveReference, aliasedAnchorScope);
      // process expandable query -- recursion step
      Scope stepScope = process(step, parentScope);

      // verify anchor and step have matching descriptors
      RelationType anchorType = aliasedAnchorScope.getRelationType().withOnlyVisibleFields();
      RelationType stepType = stepScope.getRelationType().withOnlyVisibleFields();
      if (anchorType.getVisibleFieldCount() != stepType.getVisibleFieldCount()) {
        throw new SemanticException(
            String.format(
                "base and step relations of recursion have different number of fields: %s, %s",
                anchorType.getVisibleFieldCount(), stepType.getVisibleFieldCount()));
      }

      List<Type> anchorFieldTypes =
          anchorType.getVisibleFields().stream().map(Field::getType).collect(toImmutableList());
      List<Type> stepFieldTypes =
          stepType.getVisibleFields().stream().map(Field::getType).collect(toImmutableList());

      for (int i = 0; i < anchorFieldTypes.size(); i++) {
        if (stepFieldTypes.get(i) != anchorFieldTypes.get(i)) {
          // TODO for more precise error location, pass the mismatching select expression instead of
          // `step`
          throw new SemanticException(
              String.format(
                  "recursion step relation output type (%s) is not coercible to recursion base relation output type (%s) at column %s",
                  stepFieldTypes.get(i), anchorFieldTypes.get(i), i + 1));
        }
      }

      if (!anchorFieldTypes.equals(stepFieldTypes)) {
        analysis.addRelationCoercion(step, anchorFieldTypes.toArray(new Type[0]));
      }

      analysis.setScope(withQuery.getQuery(), aliasedAnchorScope);
      analysis.registerExpandableQuery(withQuery.getQuery(), recursiveReference);
      withScopeBuilder.withNamedQuery(name, withQuery);
      return true;
    }

    @Override
    protected Scope visitTableSubquery(TableSubquery node, Optional<Scope> scope) {
      StatementAnalyzer analyzer =
          statementAnalyzerFactory.createStatementAnalyzer(
              analysis, queryContext, sessionContext, warningCollector, CorrelationSupport.ALLOWED);
      Scope queryScope =
          analyzer.analyze(
              node.getQuery(),
              scope.orElseThrow(() -> new NoSuchElementException("No value present")));
      return createAndAssignScope(node, scope, queryScope.getRelationType());
    }

    @Override
    protected Scope visitQuerySpecification(QuerySpecification node, Optional<Scope> scope) {
      // TODO: extract candidate names from SELECT, WHERE, HAVING, GROUP BY and ORDER BY expressions
      // to pass down to analyzeFrom

      Scope sourceScope = analyzeFrom(node, scope);

      node.getWhere().ifPresent(where -> analyzeWhere(node, sourceScope, where));

      List<Expression> outputExpressions = analyzeSelect(node, sourceScope);
      Analysis.GroupingSetAnalysis groupByAnalysis =
          analyzeGroupBy(node, sourceScope, outputExpressions);
      analyzeHaving(node, sourceScope);

      Scope outputScope = computeAndAssignOutputScope(node, scope, sourceScope);

      List<Expression> orderByExpressions = emptyList();
      Optional<Scope> orderByScope = Optional.empty();
      if (node.getOrderBy().isPresent()) {
        OrderBy orderBy = node.getOrderBy().get();
        orderByScope = Optional.of(computeAndAssignOrderByScope(orderBy, sourceScope, outputScope));

        orderByExpressions = analyzeOrderBy(node, orderBy.getSortItems(), orderByScope.get());

        if ((sourceScope.getOuterQueryParent().isPresent() || !isTopLevel)
            && !node.getLimit().isPresent()
            && !node.getOffset().isPresent()) {
          // not the root scope and ORDER BY is ineffective
          analysis.markRedundantOrderBy(orderBy);
          warningCollector.add(
              new IoTDBWarning(REDUNDANT_ORDER_BY, "ORDER BY in subquery may have no effect"));
        }
      }
      analysis.setOrderByExpressions(node, orderByExpressions);

      if (node.getOffset().isPresent()) {
        analyzeOffset(node.getOffset().get(), outputScope);
      }

      if (node.getLimit().isPresent()) {
        boolean requiresOrderBy = analyzeLimit(node.getLimit().get(), outputScope);
        if (requiresOrderBy && !node.getOrderBy().isPresent()) {
          throw new SemanticException("FETCH FIRST WITH TIES clause requires ORDER BY");
        }
      }

      List<Expression> sourceExpressions = new ArrayList<>();
      analysis.getSelectExpressions(node).stream()
          .map(Analysis.SelectExpression::getExpression)
          .forEach(sourceExpressions::add);
      node.getHaving().ifPresent(sourceExpressions::add);

      analyzeAggregations(
          node, sourceScope, orderByScope, groupByAnalysis, sourceExpressions, orderByExpressions);

      if (analysis.isAggregation(node) && node.getOrderBy().isPresent()) {
        ImmutableList.Builder<Expression> aggregates =
            ImmutableList.<Expression>builder()
                .addAll(groupByAnalysis.getOriginalExpressions())
                .addAll(extractAggregateFunctions(orderByExpressions));

        analysis.setOrderByAggregates(node.getOrderBy().get(), aggregates.build());
      }

      if (node.getOrderBy().isPresent() && node.getSelect().isDistinct()) {
        verifySelectDistinct(
            node,
            orderByExpressions,
            outputExpressions,
            sourceScope,
            orderByScope.orElseThrow(() -> new NoSuchElementException("No value present")));
      }

      return outputScope;
    }

    private Scope analyzeFrom(QuerySpecification node, Optional<Scope> scope) {
      if (node.getFrom().isPresent()) {
        return process(node.getFrom().get(), scope);
      }

      Scope result = createScope(scope);
      analysis.setImplicitFromScope(node, result);
      return result;
    }

    private void analyzeWhere(Node node, Scope scope, Expression predicate) {
      verifyNoAggregateWindowOrGroupingFunctions(predicate, "WHERE clause");

      ExpressionAnalysis expressionAnalysis = analyzeExpression(predicate, scope);
      analysis.recordSubqueries(node, expressionAnalysis);

      Type predicateType = expressionAnalysis.getType(predicate);
      if (!predicateType.equals(BOOLEAN)) {
        //        if (!predicateType.equals(UNKNOWN)) {
        throw new SemanticException(
            String.format(
                "WHERE clause must evaluate to a boolean: actual type %s", predicateType));
        //        }
        // coerce null to boolean
        //        analysis.addCoercion(predicate, BOOLEAN, false);
      }

      analysis.setWhere(node, predicate);
    }

    private List<Expression> analyzeSelect(QuerySpecification node, Scope scope) {
      ImmutableList.Builder<Expression> outputExpressionBuilder = ImmutableList.builder();
      ImmutableList.Builder<Analysis.SelectExpression> selectExpressionBuilder =
          ImmutableList.builder();

      for (SelectItem item : node.getSelect().getSelectItems()) {
        if (item instanceof AllColumns) {
          analyzeSelectAllColumns(
              (AllColumns) item, node, scope, outputExpressionBuilder, selectExpressionBuilder);
        } else if (item instanceof SingleColumn) {
          analyzeSelectSingleColumn(
              (SingleColumn) item, node, scope, outputExpressionBuilder, selectExpressionBuilder);
        } else {
          throw new IllegalArgumentException(
              "Unsupported SelectItem type: " + item.getClass().getName());
        }
      }
      analysis.setSelectExpressions(node, selectExpressionBuilder.build());

      return outputExpressionBuilder.build();
    }

    private void analyzeSelectAllColumns(
        AllColumns allColumns,
        QuerySpecification node,
        Scope scope,
        ImmutableList.Builder<Expression> outputExpressionBuilder,
        ImmutableList.Builder<Analysis.SelectExpression> selectExpressionBuilder) {
      // expand * and expression.*
      if (allColumns.getTarget().isPresent()) {
        // analyze AllColumns with target expression (expression.*)
        Expression expression = allColumns.getTarget().get();

        QualifiedName prefix = asQualifiedName(expression);
        if (prefix != null) {
          // analyze prefix as an 'asterisked identifier chain'
          Scope.AsteriskedIdentifierChainBasis identifierChainBasis =
              scope
                  .resolveAsteriskedIdentifierChainBasis(prefix, allColumns)
                  .orElseThrow(
                      () ->
                          new SemanticException(
                              String.format("Unable to resolve reference %s", prefix)));
          if (identifierChainBasis.getBasisType() == TABLE) {
            RelationType relationType =
                identifierChainBasis
                    .getRelationType()
                    .orElseThrow(() -> new NoSuchElementException("No value present"));
            List<Field> requestedFields =
                relationType.resolveVisibleFieldsWithRelationPrefix(Optional.of(prefix));
            List<Field> fields = filterInaccessibleFields(requestedFields);
            if (fields.isEmpty()) {
              if (!requestedFields.isEmpty()) {
                throw new SemanticException("Relation not found or not allowed");
              }
              throw new SemanticException("SELECT * not allowed from relation that has no columns");
            }
            boolean local =
                scope.isLocalScope(
                    identifierChainBasis
                        .getScope()
                        .orElseThrow(() -> new NoSuchElementException("No value present")));
            analyzeAllColumnsFromTable(
                fields,
                allColumns,
                node,
                local ? scope : identifierChainBasis.getScope().get(),
                outputExpressionBuilder,
                selectExpressionBuilder,
                relationType,
                local);
            return;
          }
        }
        // identifierChainBasis.get().getBasisType == FIELD or target expression isn't a
        // QualifiedName
        throw new SemanticException(
            "identifierChainBasis.get().getBasisType == FIELD or target expression isn't a QualifiedName");
        //        analyzeAllFieldsFromRowTypeExpression(expression, allColumns, node, scope,
        // outputExpressionBuilder,
        //            selectExpressionBuilder);
      } else {
        // analyze AllColumns without target expression ('*')
        if (!allColumns.getAliases().isEmpty()) {
          throw new SemanticException("Column aliases not supported");
        }

        List<Field> requestedFields = (List<Field>) scope.getRelationType().getVisibleFields();
        List<Field> fields = filterInaccessibleFields(requestedFields);
        if (fields.isEmpty()) {
          if (!node.getFrom().isPresent()) {
            throw new SemanticException("SELECT * not allowed in queries without FROM clause");
          }
          if (!requestedFields.isEmpty()) {
            throw new SemanticException("Relation not found or not allowed");
          }
          throw new SemanticException("SELECT * not allowed from relation that has no columns");
        }

        analyzeAllColumnsFromTable(
            fields,
            allColumns,
            node,
            scope,
            outputExpressionBuilder,
            selectExpressionBuilder,
            scope.getRelationType(),
            true);
      }
    }

    private List<Field> filterInaccessibleFields(List<Field> fields) {

      ImmutableSet.Builder<Field> accessibleFields = ImmutableSet.builder();

      // collect fields by table
      ListMultimap<QualifiedObjectName, Field> tableFieldsMap = ArrayListMultimap.create();
      fields.forEach(
          field -> {
            Optional<QualifiedObjectName> originTable = field.getOriginTable();
            if (originTable.isPresent()) {
              tableFieldsMap.put(originTable.get(), field);
            } else {
              // keep anonymous fields accessible
              accessibleFields.add(field);
            }
          });

      // TODO Auth control
      tableFieldsMap
          .asMap()
          .forEach(
              (table, tableFields) -> {
                //              Set<String> accessibleColumns = accessControl.filterColumns(
                //                      session.toSecurityContext(),
                //                      table.getCatalogName(),
                //                      ImmutableMap.of(
                //                          table.asSchemaTableName(),
                //                          tableFields.stream()
                //                              .map(field -> field.getOriginColumnName().get())
                //                              .collect(toImmutableSet())))
                //                  .getOrDefault(table.asSchemaTableName(), ImmutableSet.of());
                accessibleFields.addAll(
                    tableFields.stream()
                        // .filter(field ->
                        // accessibleColumns.contains(field.getOriginColumnName().get()))
                        .collect(toImmutableList()));
              });

      return fields.stream().filter(accessibleFields.build()::contains).collect(toImmutableList());
    }

    private void analyzeAllColumnsFromTable(
        List<Field> fields,
        AllColumns allColumns,
        QuerySpecification node,
        Scope scope,
        ImmutableList.Builder<Expression> outputExpressionBuilder,
        ImmutableList.Builder<Analysis.SelectExpression> selectExpressionBuilder,
        RelationType relationType,
        boolean local) {
      if (!allColumns.getAliases().isEmpty()) {
        validateColumnAliasesCount(allColumns.getAliases(), fields.size());
      }

      ImmutableList.Builder<Field> itemOutputFieldBuilder = ImmutableList.builder();

      for (int i = 0; i < fields.size(); i++) {
        Field field = fields.get(i);
        Expression fieldExpression;
        if (local) {
          fieldExpression = new FieldReference(relationType.indexOf(field));
        } else {
          if (!field.getName().isPresent()) {
            throw new SemanticException(
                "SELECT * from outer scope table not supported with anonymous columns");
          }
          checkState(field.getRelationAlias().isPresent(), "missing relation alias");
          fieldExpression =
              new DereferenceExpression(
                  DereferenceExpression.from(field.getRelationAlias().get()),
                  new Identifier(field.getName().get()));
        }
        analyzeExpression(fieldExpression, scope);
        outputExpressionBuilder.add(fieldExpression);
        selectExpressionBuilder.add(
            new Analysis.SelectExpression(fieldExpression, Optional.empty()));

        Optional<String> alias = field.getName();
        if (!allColumns.getAliases().isEmpty()) {
          alias = Optional.of(allColumns.getAliases().get(i).getValue());
        }

        Field newField =
            new Field(
                field.getRelationAlias(),
                alias,
                field.getType(),
                field.getColumnCategory(),
                false,
                field.getOriginTable(),
                field.getOriginColumnName(),
                !allColumns.getAliases().isEmpty() || field.isAliased());
        itemOutputFieldBuilder.add(newField);
        analysis.addSourceColumns(newField, analysis.getSourceColumns(field));

        Type type = field.getType();
        if (node.getSelect().isDistinct() && !type.isComparable()) {
          throw new SemanticException(
              String.format("DISTINCT can only be applied to comparable types (actual: %s)", type));
        }
      }
      analysis.setSelectAllResultFields(allColumns, itemOutputFieldBuilder.build());
    }

    //    private void analyzeAllFieldsFromRowTypeExpression(
    //        Expression expression,
    //        AllColumns allColumns,
    //        QuerySpecification node,
    //        Scope scope,
    //        ImmutableList.Builder<Expression> outputExpressionBuilder,
    //        ImmutableList.Builder<Analysis.SelectExpression> selectExpressionBuilder) {
    //      ImmutableList.Builder<Field> itemOutputFieldBuilder = ImmutableList.builder();
    //
    //      ExpressionAnalysis expressionAnalysis = analyzeExpression(expression, scope);
    //      Type type = expressionAnalysis.getType(expression);
    //      if (!(type instanceof RowType)) {
    //        throw semanticException(TYPE_MISMATCH, node.getSelect(), "expected expression of type
    // Row");
    //      }
    //      int referencedFieldsCount = ((RowType) type).getFields().size();
    //      if (!allColumns.getAliases().isEmpty()) {
    //        validateColumnAliasesCount(allColumns.getAliases(), referencedFieldsCount);
    //      }
    //      analysis.recordSubqueries(node, expressionAnalysis);
    //
    //      ImmutableList.Builder<Expression> unfoldedExpressionsBuilder = ImmutableList.builder();
    //      for (int i = 0; i < referencedFieldsCount; i++) {
    //        Expression outputExpression = new SubscriptExpression(expression, new LongLiteral("" +
    // (i + 1)));
    //        outputExpressionBuilder.add(outputExpression);
    //        analyzeExpression(outputExpression, scope);
    //        unfoldedExpressionsBuilder.add(outputExpression);
    //
    //        Type outputExpressionType = type.getTypeParameters().get(i);
    //        if (node.getSelect().isDistinct() && !outputExpressionType.isComparable()) {
    //          throw semanticException(TYPE_MISMATCH, node.getSelect(),
    //              "DISTINCT can only be applied to comparable types (actual: %s)",
    // type.getTypeParameters().get(i));
    //        }
    //
    //        Optional<String> name = ((RowType) type).getFields().get(i).getName();
    //        if (!allColumns.getAliases().isEmpty()) {
    //          name = Optional.of(allColumns.getAliases().get(i).getValue());
    //        }
    //        itemOutputFieldBuilder.add(Field.newUnqualified(name, outputExpressionType));
    //      }
    //      selectExpressionBuilder.add(new SelectExpression(expression,
    // Optional.of(unfoldedExpressionsBuilder.build())));
    //      analysis.setSelectAllResultFields(allColumns, itemOutputFieldBuilder.build());
    //    }

    private void analyzeSelectSingleColumn(
        SingleColumn singleColumn,
        QuerySpecification node,
        Scope scope,
        ImmutableList.Builder<Expression> outputExpressionBuilder,
        ImmutableList.Builder<Analysis.SelectExpression> selectExpressionBuilder) {
      Expression expression = singleColumn.getExpression();
      ExpressionAnalysis expressionAnalysis = analyzeExpression(expression, scope);
      analysis.recordSubqueries(node, expressionAnalysis);
      outputExpressionBuilder.add(expression);
      selectExpressionBuilder.add(new Analysis.SelectExpression(expression, Optional.empty()));

      Type type = expressionAnalysis.getType(expression);
      if (node.getSelect().isDistinct() && !type.isComparable()) {
        throw new SemanticException(
            String.format(
                "DISTINCT can only be applied to comparable types (actual: %s): %s",
                type, expression));
      }
    }

    private Analysis.GroupingSetAnalysis analyzeGroupBy(
        QuerySpecification node, Scope scope, List<Expression> outputExpressions) {
      if (node.getGroupBy().isPresent()) {
        ImmutableList.Builder<List<Set<FieldId>>> cubes = ImmutableList.builder();
        ImmutableList.Builder<List<Set<FieldId>>> rollups = ImmutableList.builder();
        ImmutableList.Builder<List<Set<FieldId>>> sets = ImmutableList.builder();
        ImmutableList.Builder<Expression> complexExpressions = ImmutableList.builder();
        ImmutableList.Builder<Expression> groupingExpressions = ImmutableList.builder();

        checkGroupingSetsCount(node.getGroupBy().get());
        for (GroupingElement groupingElement : node.getGroupBy().get().getGroupingElements()) {
          if (groupingElement instanceof SimpleGroupBy) {
            for (Expression column : groupingElement.getExpressions()) {
              // simple GROUP BY expressions allow ordinals or arbitrary expressions
              if (column instanceof LongLiteral) {
                long ordinal = ((LongLiteral) column).getParsedValue();
                if (ordinal < 1 || ordinal > outputExpressions.size()) {
                  throw new SemanticException(
                      String.format("GROUP BY position %s is not in select list", ordinal));
                }

                column = outputExpressions.get(toIntExact(ordinal - 1));
                verifyNoAggregateWindowOrGroupingFunctions(column, "GROUP BY clause");
              } else {
                verifyNoAggregateWindowOrGroupingFunctions(column, "GROUP BY clause");
                analyzeExpression(column, scope);
              }

              ResolvedField field = analysis.getColumnReferenceFields().get(NodeRef.of(column));
              if (field != null) {
                sets.add(ImmutableList.of(ImmutableSet.of(field.getFieldId())));
              } else {
                analysis.recordSubqueries(node, analyzeExpression(column, scope));
                complexExpressions.add(column);
              }

              groupingExpressions.add(column);
            }
          } else if (groupingElement instanceof GroupingSets) {
            GroupingSets element = (GroupingSets) groupingElement;
            for (Expression column : groupingElement.getExpressions()) {
              analyzeExpression(column, scope);
              if (!analysis.getColumnReferences().contains(NodeRef.of(column))) {
                throw new SemanticException(
                    String.format("GROUP BY expression must be a column reference: %s", column));
              }

              groupingExpressions.add(column);
            }

            List<Set<FieldId>> groupingSets =
                element.getSets().stream()
                    .map(
                        set ->
                            set.stream()
                                .map(NodeRef::of)
                                .map(analysis.getColumnReferenceFields()::get)
                                .map(ResolvedField::getFieldId)
                                .collect(toImmutableSet()))
                    .collect(toImmutableList());

            switch (element.getType()) {
              case CUBE:
                cubes.add(groupingSets);
                break;
              case ROLLUP:
                rollups.add(groupingSets);
                break;
              case EXPLICIT:
                sets.add(groupingSets);
                break;
            }
          }
        }

        List<Expression> expressions = groupingExpressions.build();
        for (Expression expression : expressions) {
          Type type = analysis.getType(expression);
          if (!type.isComparable()) {
            throw new SemanticException(
                String.format(
                    "%s is not comparable, and therefore cannot be used in GROUP BY", type));
          }
        }

        Analysis.GroupingSetAnalysis groupingSets =
            new Analysis.GroupingSetAnalysis(
                expressions,
                cubes.build(),
                rollups.build(),
                sets.build(),
                complexExpressions.build());
        analysis.setGroupingSets(node, groupingSets);

        return groupingSets;
      }

      Analysis.GroupingSetAnalysis result =
          new Analysis.GroupingSetAnalysis(
              ImmutableList.of(),
              ImmutableList.of(),
              ImmutableList.of(),
              ImmutableList.of(),
              ImmutableList.of());

      if (hasAggregates(node) || node.getHaving().isPresent()) {
        analysis.setGroupingSets(node, result);
      }

      return result;
    }

    private boolean hasAggregates(QuerySpecification node) {
      List<Node> toExtract =
          ImmutableList.<Node>builder()
              .addAll(node.getSelect().getSelectItems())
              .addAll(getSortItemsFromOrderBy(node.getOrderBy()))
              .build();

      List<FunctionCall> aggregates = extractAggregateFunctions(toExtract);

      return !aggregates.isEmpty();
    }

    private void checkGroupingSetsCount(GroupBy node) {
      // If groupBy is distinct then crossProduct will be overestimated if there are duplicate
      // grouping sets.
      int crossProduct = 1;
      for (GroupingElement element : node.getGroupingElements()) {
        try {
          int product = 0;
          if (element instanceof SimpleGroupBy) {
            product = 1;
          } else if (element instanceof GroupingSets) {
            GroupingSets groupingSets = (GroupingSets) element;
            switch (groupingSets.getType()) {
              case CUBE:
                int exponent = ((GroupingSets) element).getSets().size();
                if (exponent > 30) {
                  throw new ArithmeticException();
                }
                product = 1 << exponent;
                break;
              case ROLLUP:
                product = groupingSets.getSets().size() + 1;
                break;
              case EXPLICIT:
                product = groupingSets.getSets().size();
                break;
            }
          } else {
            throw new UnsupportedOperationException(
                "Unsupported grouping element type: " + element.getClass().getName());
          }
          crossProduct = Math.multiplyExact(crossProduct, product);
        } catch (ArithmeticException e) {
          throw new SemanticException(
              String.format("GROUP BY has more than %s grouping sets", Integer.MAX_VALUE));
        }
        //        if (crossProduct > getMaxGroupingSets(session)) {
        //          throw semanticException(TOO_MANY_GROUPING_SETS, node,
        //              "GROUP BY has %s grouping sets but can contain at most %s", crossProduct,
        // getMaxGroupingSets(session));
        //        }
      }
    }

    private void analyzeHaving(QuerySpecification node, Scope scope) {
      if (node.getHaving().isPresent()) {
        Expression predicate = node.getHaving().get();

        ExpressionAnalysis expressionAnalysis = analyzeExpression(predicate, scope);
        analysis.recordSubqueries(node, expressionAnalysis);

        Type predicateType = expressionAnalysis.getType(predicate);
        if (!predicateType.equals(BOOLEAN)) {
          throw new SemanticException(
              String.format(
                  "HAVING clause must evaluate to a boolean: actual type %s", predicateType));
        }

        analysis.setHaving(node, predicate);
      }
    }

    private Scope computeAndAssignOutputScope(
        QuerySpecification node, Optional<Scope> scope, Scope sourceScope) {
      ImmutableList.Builder<Field> outputFields = ImmutableList.builder();

      for (SelectItem item : node.getSelect().getSelectItems()) {
        if (item instanceof AllColumns) {
          AllColumns allColumns = (AllColumns) item;
          List<Field> fields = analysis.getSelectAllResultFields(allColumns);
          checkNotNull(fields, "output fields is null for select item %s", item);
          for (int i = 0; i < fields.size(); i++) {
            Field field = fields.get(i);

            Optional<String> name;
            if (!allColumns.getAliases().isEmpty()) {
              name = Optional.of(allColumns.getAliases().get(i).getCanonicalValue());
            } else {
              name = field.getName();
            }

            Field newField =
                Field.newUnqualified(
                    name,
                    field.getType(),
                    field.getColumnCategory(),
                    field.getOriginTable(),
                    field.getOriginColumnName(),
                    false);
            analysis.addSourceColumns(newField, analysis.getSourceColumns(field));
            outputFields.add(newField);
          }
        } else if (item instanceof SingleColumn) {
          SingleColumn column = (SingleColumn) item;
          Expression expression = column.getExpression();
          Optional<Identifier> field = column.getAlias();

          Optional<QualifiedObjectName> originTable = Optional.empty();
          Optional<String> originColumn = Optional.empty();
          QualifiedName name = null;

          if (expression instanceof Identifier) {
            name = QualifiedName.of(((Identifier) expression).getValue());
          } else if (expression instanceof DereferenceExpression) {
            name = DereferenceExpression.getQualifiedName((DereferenceExpression) expression);
          }

          if (name != null) {
            Field matchingField = null;
            try {
              matchingField = analysis.getResolvedField(expression).getField();
            } catch (IllegalArgumentException e) {
              List<Field> matchingFields = sourceScope.getRelationType().resolveFields(name);
              if (!matchingFields.isEmpty()) {
                matchingField = matchingFields.get(0);
              }
            }
            if (matchingField != null) {
              originTable = matchingField.getOriginTable();
              originColumn = matchingField.getOriginColumnName();
            }
          }

          if (!field.isPresent() && (name != null)) {
            field = Optional.of(getLast(name.getOriginalParts()));
          }

          Field newField =
              Field.newUnqualified(
                  field.map(Identifier::getValue),
                  analysis.getType(expression),
                  TsTableColumnCategory.MEASUREMENT,
                  originTable,
                  originColumn,
                  column.getAlias().isPresent()); // TODO don't use analysis as a side-channel. Use
          // outputExpressions to look up the type
          if (originTable.isPresent()) {
            analysis.addSourceColumns(
                newField,
                ImmutableSet.of(
                    new Analysis.SourceColumn(
                        originTable.get(),
                        originColumn.orElseThrow(
                            () -> new NoSuchElementException("No value present")))));
          } else {
            analysis.addSourceColumns(newField, analysis.getExpressionSourceColumns(expression));
          }
          outputFields.add(newField);
        } else {
          throw new IllegalArgumentException(
              "Unsupported SelectItem type: " + item.getClass().getName());
        }
      }

      return createAndAssignScope(node, scope, outputFields.build());
    }

    private Scope computeAndAssignOrderByScope(OrderBy node, Scope sourceScope, Scope outputScope) {
      // ORDER BY should "see" both output and FROM fields during initial analysis and
      // non-aggregation query planning
      Scope orderByScope =
          Scope.builder()
              .withParent(sourceScope)
              .withRelationType(outputScope.getRelationId(), outputScope.getRelationType())
              .build();
      analysis.setScope(node, orderByScope);
      return orderByScope;
    }

    @Override
    protected Scope visitSubqueryExpression(SubqueryExpression node, Optional<Scope> context) {
      return process(node.getQuery(), context);
    }

    @Override
    protected Scope visitSetOperation(SetOperation node, Optional<Scope> scope) {
      checkState(node.getRelations().size() >= 2);

      List<RelationType> childrenTypes =
          node.getRelations().stream()
              .map(relation -> process(relation, scope).getRelationType().withOnlyVisibleFields())
              .collect(toImmutableList());

      String setOperationName = node.getClass().getSimpleName().toUpperCase(ENGLISH);
      Type[] outputFieldTypes =
          childrenTypes.get(0).getVisibleFields().stream().map(Field::getType).toArray(Type[]::new);
      for (RelationType relationType : childrenTypes) {
        int outputFieldSize = outputFieldTypes.length;
        int descFieldSize = relationType.getVisibleFields().size();
        if (outputFieldSize != descFieldSize) {
          throw new SemanticException(
              String.format(
                  "%s query has different number of fields: %d, %d",
                  setOperationName, outputFieldSize, descFieldSize));
        }
        for (int i = 0; i < descFieldSize; i++) {
          Type descFieldType = relationType.getFieldByIndex(i).getType();
          if (descFieldType != outputFieldTypes[i]) {
            throw new SemanticException(
                String.format(
                    "column %d in %s query has incompatible types: %s, %s",
                    i + 1,
                    setOperationName,
                    outputFieldTypes[i].getDisplayName(),
                    descFieldType.getDisplayName()));
          }
        }
      }

      if (node instanceof Intersect
          || node instanceof Except
          || node instanceof Union && node.isDistinct()) {
        for (Type type : outputFieldTypes) {
          if (!type.isComparable()) {
            throw new SemanticException(
                String.format(
                    "Type %s is not comparable and therefore cannot be used in %s%s",
                    type, setOperationName, node instanceof Union ? " DISTINCT" : ""));
          }
        }
      }

      Field[] outputDescriptorFields = new Field[outputFieldTypes.length];
      RelationType firstDescriptor = childrenTypes.get(0);
      for (int i = 0; i < outputFieldTypes.length; i++) {
        Field oldField = firstDescriptor.getFieldByIndex(i);
        outputDescriptorFields[i] =
            new Field(
                oldField.getRelationAlias(),
                oldField.getName(),
                outputFieldTypes[i],
                oldField.getColumnCategory(),
                oldField.isHidden(),
                oldField.getOriginTable(),
                oldField.getOriginColumnName(),
                oldField.isAliased());

        int index = i; // Variable used in Lambda should be final
        analysis.addSourceColumns(
            outputDescriptorFields[index],
            childrenTypes.stream()
                .map(relationType -> relationType.getFieldByIndex(index))
                .flatMap(field -> analysis.getSourceColumns(field).stream())
                .collect(toImmutableSet()));
      }

      for (int i = 0; i < node.getRelations().size(); i++) {
        Relation relation = node.getRelations().get(i);
        RelationType relationType = childrenTypes.get(i);
        for (int j = 0; j < relationType.getVisibleFields().size(); j++) {
          Type outputFieldType = outputFieldTypes[j];
          Type descFieldType = relationType.getFieldByIndex(j).getType();
          if (!outputFieldType.equals(descFieldType)) {
            analysis.addRelationCoercion(relation, outputFieldTypes);
            break;
          }
        }
      }
      return createAndAssignScope(node, scope, outputDescriptorFields);
    }

    @Override
    protected Scope visitTable(Table table, Optional<Scope> scope) {
      if (!table.getName().getPrefix().isPresent()) {
        // is this a reference to a WITH query?
        Optional<WithQuery> withQuery =
            createScope(scope).getNamedQuery(table.getName().getSuffix());
        if (withQuery.isPresent()) {
          analysis.setRelationName(table, table.getName());
          return createScopeForCommonTableExpression(table, scope, withQuery.get());
        }
        // is this a recursive reference in expandable WITH query? If so, there's base scope
        // recorded.
        Optional<Scope> expandableBaseScope = analysis.getExpandableBaseScope(table);
        if (expandableBaseScope.isPresent()) {
          Scope baseScope = expandableBaseScope.get();
          // adjust local and outer parent scopes accordingly to the local context of the recursive
          // reference
          Scope resultScope =
              scopeBuilder(scope)
                  .withRelationType(baseScope.getRelationId(), baseScope.getRelationType())
                  .build();
          analysis.setScope(table, resultScope);
          analysis.setRelationName(table, table.getName());
          return resultScope;
        }
      }

      QualifiedObjectName name = createQualifiedObjectName(sessionContext, table.getName());
      analysis.setRelationName(
          table, QualifiedName.of(name.getDatabaseName(), name.getObjectName()));

      Optional<TableSchema> tableSchema = metadata.getTableSchema(sessionContext, name);
      // This can only be a table
      if (!tableSchema.isPresent()) {
        throw new SemanticException(String.format("Table '%s' does not exist", name));
      }
      analysis.addEmptyColumnReferencesForTable(accessControl, sessionContext.getIdentity(), name);

      ImmutableList.Builder<Field> fields = ImmutableList.builder();
      fields.addAll(analyzeTableOutputFields(table, name, tableSchema.get()));

      //      boolean addRowIdColumn = updateKind.isPresent();
      //
      //      if (addRowIdColumn) {
      //        // Add the row id field
      //        ColumnHandle rowIdColumnHandle = metadata.getMergeRowIdColumnHandle(session,
      // tableHandle.get());
      //        Type type = metadata.getColumnMetadata(session, tableHandle.get(),
      // rowIdColumnHandle).getType();
      //        Field field = Field.newUnqualified(Optional.empty(), type);
      //        fields.add(field);
      //        analysis.setColumn(field, rowIdColumnHandle);
      //      }

      List<Field> outputFields = fields.build();

      RelationType relationType = new RelationType(outputFields);
      Scope accessControlScope =
          Scope.builder().withRelationType(RelationId.anonymous(), relationType).build();
      //      analyzeFiltersAndMasks(table, name, new RelationType(outputFields),
      // accessControlScope);
      analysis.registerTable(table, tableSchema, name);

      Scope tableScope = createAndAssignScope(table, scope, relationType);

      //      if (addRowIdColumn) {
      //        FieldReference reference = new FieldReference(outputFields.size() - 1);
      //        analyzeExpression(reference, tableScope);
      //        analysis.setRowIdField(table, reference);
      //      }

      return tableScope;
    }

    private Scope createScopeForCommonTableExpression(
        Table table, Optional<Scope> scope, WithQuery withQuery) {
      Query query = withQuery.getQuery();
      analysis.registerNamedQuery(table, query);

      // re-alias the fields with the name assigned to the query in the WITH declaration
      RelationType queryDescriptor = analysis.getOutputDescriptor(query);

      List<Field> fields;
      Optional<List<Identifier>> columnNames = withQuery.getColumnNames();
      if (columnNames.isPresent()) {
        // if columns are explicitly aliased -> WITH cte(alias1, alias2 ...)
        checkState(
            columnNames.get().size() == queryDescriptor.getVisibleFieldCount(),
            "mismatched aliases");
        ImmutableList.Builder<Field> fieldBuilder = ImmutableList.builder();
        Iterator<Identifier> aliases = columnNames.get().iterator();
        for (int i = 0; i < queryDescriptor.getAllFieldCount(); i++) {
          Field inputField = queryDescriptor.getFieldByIndex(i);
          if (!inputField.isHidden()) {
            Field field =
                Field.newQualified(
                    QualifiedName.of(table.getName().getSuffix()),
                    Optional.of(aliases.next().getValue()),
                    inputField.getType(),
                    inputField.getColumnCategory(),
                    false,
                    inputField.getOriginTable(),
                    inputField.getOriginColumnName(),
                    inputField.isAliased());
            fieldBuilder.add(field);
            analysis.addSourceColumns(field, analysis.getSourceColumns(inputField));
          }
        }
        fields = fieldBuilder.build();
      } else {
        ImmutableList.Builder<Field> fieldBuilder = ImmutableList.builder();
        for (int i = 0; i < queryDescriptor.getAllFieldCount(); i++) {
          Field inputField = queryDescriptor.getFieldByIndex(i);
          if (!inputField.isHidden()) {
            Field field =
                Field.newQualified(
                    QualifiedName.of(table.getName().getSuffix()),
                    inputField.getName(),
                    inputField.getType(),
                    inputField.getColumnCategory(),
                    false,
                    inputField.getOriginTable(),
                    inputField.getOriginColumnName(),
                    inputField.isAliased());
            fieldBuilder.add(field);
            analysis.addSourceColumns(field, analysis.getSourceColumns(inputField));
          }
        }
        fields = fieldBuilder.build();
      }

      return createAndAssignScope(table, scope, fields);
    }

    private List<Field> analyzeTableOutputFields(
        final Table table, final QualifiedObjectName tableName, final TableSchema tableSchema) {
      // TODO: discover columns lazily based on where they are needed (to support connectors that
      // can't enumerate all tables)
      ImmutableList.Builder<Field> fields = ImmutableList.builder();
      for (ColumnSchema column : tableSchema.getColumns()) {
        Field field =
            Field.newQualified(
                table.getName(),
                Optional.of(column.getName()),
                column.getType(),
                column.getColumnCategory(),
                column.isHidden(),
                Optional.of(tableName),
                Optional.of(column.getName()),
                false);
        fields.add(field);
        analysis.addSourceColumns(
            field, ImmutableSet.of(new Analysis.SourceColumn(tableName, column.getName())));
      }
      return fields.build();
    }

    //    private void analyzeFiltersAndMasks(Table table, QualifiedObjectName name, RelationType
    // relationType,
    //                                        Scope accessControlScope) {
    //      for (int index = 0; index < relationType.getAllFieldCount(); index++) {
    //        Field field = relationType.getFieldByIndex(index);
    //        if (field.getName().isPresent()) {
    //          Optional<ViewExpression> mask =
    //              accessControl.getColumnMask(session.toSecurityContext(), name,
    // field.getName().get(), field.getType());
    //
    //          if (mask.isPresent() && checkCanSelectFromColumn(name,
    // field.getName().orElseThrow())) {
    //            analyzeColumnMask(session.getIdentity().getUser(), table, name, field,
    // accessControlScope, mask.get());
    //          }
    //        }
    //      }
    //
    //      accessControl.getRowFilters(session.toSecurityContext(), name)
    //          .forEach(
    //              filter -> analyzeRowFilter(session.getIdentity().getUser(), table, name,
    // accessControlScope, filter));
    //    }

    @Override
    protected Scope visitValues(Values node, Optional<Scope> scope) {
      checkState(!node.getRows().isEmpty());

      List<Type> rowTypes =
          node.getRows().stream()
              .map(row -> analyzeExpression(row, createScope(scope)).getType(row))
              .map(
                  type -> {
                    if (type instanceof RowType) {
                      return type;
                    }
                    return RowType.anonymousRow(type);
                  })
              .collect(toImmutableList());

      int fieldCount = rowTypes.get(0).getTypeParameters().size();
      Type commonSuperType = rowTypes.get(0);
      for (Type rowType : rowTypes) {
        // check field count consistency for rows
        if (rowType.getTypeParameters().size() != fieldCount) {
          throw new SemanticException(
              String.format(
                  "Values rows have mismatched sizes: %s vs %s",
                  fieldCount, rowType.getTypeParameters().size()));
        }

        // determine common super type of the rows
        //        commonSuperType = typeCoercion.getCommonSuperType(rowType, commonSuperType)
        //            .orElseThrow(() -> semanticException(TYPE_MISMATCH,
        //                node,
        //                "Values rows have mismatched types: %s vs %s",
        //                rowTypes.get(0),
        //                rowType));
      }

      // add coercions
      int rowIndex = 0;
      for (Expression row : node.getRows()) {
        Type actualType = analysis.getType(row);
        if (row instanceof Row) {
          // coerce Row by fields to preserve Row structure and enable optimizations based on this
          // structure, e.g.pruning, predicate extraction
          // TODO coerce the whole Row and add an Optimizer rule that converts CAST(ROW(...) AS
          // ...)into ROW (CAST(...),CAST(...), ...).
          //  The rule would also handle Row-type expressions that were specified as CAST(ROW).It
          // should support multiple casts over a ROW.
          for (int i = 0; i < actualType.getTypeParameters().size(); i++) {
            //            Expression item = ((Row) row).getItems().get(i);
            Type actualItemType = actualType.getTypeParameters().get(i);
            Type expectedItemType = commonSuperType.getTypeParameters().get(i);
            if (!actualItemType.equals(expectedItemType)) {
              throw new SemanticException(
                  String.format(
                      "Type of row %d column %d is mismatched, expected: %s, actual: %s",
                      rowIndex, i, expectedItemType, actualItemType));
              //              analysis.addCoercion(item, expectedItemType,
              //                  typeCoercion.isTypeOnlyCoercion(actualItemType,
              // expectedItemType));
            }
          }
        } else if (actualType instanceof RowType) {
          // coerce row-type expression as a whole
          //          if (!actualType.equals(commonSuperType)) {
          //            analysis.addCoercion(row, commonSuperType,
          //                typeCoercion.isTypeOnlyCoercion(actualType, commonSuperType));
          //          }

          throw new SemanticException(
              String.format(
                  "Type of row %d is mismatched, expected: %s, actual: %s",
                  rowIndex, commonSuperType, actualType));
        } else {
          // coerce field. it will be wrapped in Row by Planner
          Type superType = getOnlyElement(commonSuperType.getTypeParameters());
          if (!actualType.equals(superType)) {
            //            analysis.addCoercion(row, superType,
            // typeCoercion.isTypeOnlyCoercion(actualType,
            //                superType));
            throw new SemanticException(
                String.format(
                    "Type of row %d is mismatched, expected: %s, actual: %s",
                    rowIndex, superType, actualType));
          }
        }
        rowIndex++;
      }

      List<Field> fields =
          commonSuperType.getTypeParameters().stream()
              .map(
                  valueType ->
                      Field.newUnqualified(
                          Optional.empty(), valueType, TsTableColumnCategory.MEASUREMENT))
              .collect(toImmutableList());

      return createAndAssignScope(node, scope, fields);
    }

    @Override
    protected Scope visitAliasedRelation(AliasedRelation relation, Optional<Scope> scope) {
      analysis.setRelationName(relation, QualifiedName.of(ImmutableList.of(relation.getAlias())));
      analysis.addAliased(relation.getRelation());
      Scope relationScope = process(relation.getRelation(), scope);
      RelationType relationType = relationScope.getRelationType();

      // todo this check should be inside of TupleDescriptor.withAlias, but the exception needs the
      // node object
      if (relation.getColumnNames() != null) {
        int totalColumns = relationType.getVisibleFieldCount();
        if (totalColumns != relation.getColumnNames().size()) {
          throw new SemanticException(
              String.format(
                  "Column alias list has %s entries but '%s' has %s columns available",
                  relation.getColumnNames().size(), relation.getAlias(), totalColumns));
        }
      }

      List<String> aliases = null;
      Collection<Field> inputFields = relationType.getAllFields();
      if (relation.getColumnNames() != null) {
        aliases =
            relation.getColumnNames().stream()
                .map(Identifier::getValue)
                .collect(Collectors.toList());
        // hidden fields are not exposed when there are column aliases
        inputFields = relationType.getVisibleFields();
      }

      RelationType descriptor = relationType.withAlias(relation.getAlias().getValue(), aliases);

      checkArgument(
          inputFields.size() == descriptor.getAllFieldCount(),
          "Expected %s fields, got %s",
          descriptor.getAllFieldCount(),
          inputFields.size());

      Streams.forEachPair(
          descriptor.getAllFields().stream(),
          inputFields.stream(),
          (newField, field) ->
              analysis.addSourceColumns(newField, analysis.getSourceColumns(field)));

      return createAndAssignScope(relation, scope, descriptor);
    }

    @Override
    protected Scope visitJoin(Join node, Optional<Scope> scope) {
      JoinCriteria criteria = node.getCriteria().orElse(null);

      joinConditionCheck(criteria);

      Scope left = process(node.getLeft(), scope);
      Scope right = process(node.getRight(), scope);

      if (criteria instanceof JoinUsing) {
        return analyzeJoinUsing(node, ((JoinUsing) criteria).getColumns(), scope, left, right);
      }

      Scope output =
          createAndAssignScope(
              node, scope, left.getRelationType().joinWith(right.getRelationType()));

      if (node.getType() == Join.Type.CROSS
          || node.getType() == LEFT
          || node.getType() == RIGHT
          || node.getType() == FULL) {
        throw new SemanticException(
            String.format(
                "%s JOIN is not supported, only support INNER JOIN in current version.",
                node.getType()));
      } else if (node.getType() == Join.Type.IMPLICIT) {
        return output;
      }
      if (criteria instanceof JoinOn) {
        Expression expression = ((JoinOn) criteria).getExpression();
        verifyNoAggregateWindowOrGroupingFunctions(expression, "JOIN clause");

        // Need to register coercions in case when join criteria requires coercion (e.g. join on
        // char(1) = char(2))
        // Correlations are only currently support in the join criteria for INNER joins
        ExpressionAnalysis expressionAnalysis =
            analyzeExpression(
                expression,
                output,
                node.getType() == INNER
                    ? CorrelationSupport.ALLOWED
                    : CorrelationSupport.DISALLOWED);
        Type clauseType = expressionAnalysis.getType(expression);
        if (!clauseType.equals(BOOLEAN)) {
          //          if (!clauseType.equals(UNKNOWN)) {
          //            throw semanticException(
          //                TYPE_MISMATCH,
          //                expression,
          //                "JOIN ON clause must evaluate to a boolean: actual type %s",
          //                clauseType);
          //          }
          throw new SemanticException(
              String.format(
                  "JOIN ON clause must evaluate to a boolean: actual type %s", clauseType));
          // coerce expression to boolean
          //          analysis.addCoercion(expression, BOOLEAN, false);
        }

        analysis.recordSubqueries(node, expressionAnalysis);
        analysis.setJoinCriteria(node, expression);
      } else {
        throw new UnsupportedOperationException(
            "Unsupported join criteria: " + criteria.getClass().getName());
      }

      return output;
    }

    private void joinConditionCheck(JoinCriteria criteria) {
      if (criteria instanceof NaturalJoin) {
        throw new SemanticException("Natural join not supported");
      }

      if (criteria instanceof JoinOn) {
        JoinOn joinOn = (JoinOn) criteria;
        Expression expression = joinOn.getExpression();
        if (!(expression instanceof ComparisonExpression)) {
          throw new SemanticException(ONLY_SUPPORT_TIME_COLUMN_EQUI_JOIN);
        }
        ComparisonExpression comparisonExpression = (ComparisonExpression) expression;
        if (comparisonExpression.getOperator() != ComparisonExpression.Operator.EQUAL) {
          throw new SemanticException(ONLY_SUPPORT_TIME_COLUMN_EQUI_JOIN);
        }
        checkArgument(
            comparisonExpression.getLeft() instanceof DereferenceExpression,
            ONLY_SUPPORT_TIME_COLUMN_EQUI_JOIN);
        checkArgument(
            comparisonExpression.getRight() instanceof DereferenceExpression,
            ONLY_SUPPORT_TIME_COLUMN_EQUI_JOIN);
        DereferenceExpression left = (DereferenceExpression) comparisonExpression.getLeft();
        if (!left.getField().isPresent()
            || !left.getField().get().equals(new Identifier(TIME_COLUMN_NAME))) {
          throw new SemanticException(ONLY_SUPPORT_TIME_COLUMN_EQUI_JOIN);
        }
        DereferenceExpression right = (DereferenceExpression) comparisonExpression.getLeft();
        if (!right.getField().isPresent()
            || !right.getField().get().equals(new Identifier(TIME_COLUMN_NAME))) {
          throw new SemanticException(ONLY_SUPPORT_TIME_COLUMN_EQUI_JOIN);
        }
      } else if (criteria instanceof JoinUsing) {
        List<Identifier> identifiers = ((JoinUsing) criteria).getColumns();
        if (identifiers.size() != 1
            || !identifiers.get(0).equals(new Identifier(TIME_COLUMN_NAME))) {
          throw new SemanticException(ONLY_SUPPORT_TIME_COLUMN_IN_USING_CLAUSE);
        }
      }
    }

    private Scope analyzeJoinUsing(
        Join node, List<Identifier> columns, Optional<Scope> scope, Scope left, Scope right) {
      List<Field> joinFields = new ArrayList<>();

      List<Integer> leftJoinFields = new ArrayList<>();
      List<Integer> rightJoinFields = new ArrayList<>();

      Set<Identifier> seen = new HashSet<>();
      for (Identifier column : columns) {
        if (!seen.add(column)) {
          throw new SemanticException(
              String.format(
                  "Column '%s' appears multiple times in USING clause", column.getValue()));
        }

        ResolvedField leftField =
            left.tryResolveField(column)
                .orElseThrow(
                    () ->
                        new SemanticException(
                            String.format(
                                "Column '%s' is missing from left side of join",
                                column.getValue())));
        ResolvedField rightField =
            right
                .tryResolveField(column)
                .orElseThrow(
                    () ->
                        new SemanticException(
                            String.format(
                                "Column '%s' is missing from right side of join",
                                column.getValue())));

        // ensure a comparison operator exists for the given types (applying coercions if necessary)
        //        try {
        //          metadata.resolveOperator(OperatorType.EQUAL, ImmutableList.of(
        //              leftField.getType(), rightField.getType()));
        //        } catch (OperatorNotFoundException e) {
        //          throw semanticException(TYPE_MISMATCH, column, e, "%s", e.getMessage());
        //        }
        if (leftField.getType() != rightField.getType()) {
          throw new SemanticException(
              String.format(
                  "Column Types of left and right side are different: left is %s, right is %s",
                  leftField.getType(), rightField.getType()));
        }

        analysis.addTypes(ImmutableMap.of(NodeRef.of(column), leftField.getType()));

        joinFields.add(
            Field.newUnqualified(
                column.getValue(), leftField.getType(), leftField.getColumnCategory()));

        leftJoinFields.add(leftField.getRelationFieldIndex());
        rightJoinFields.add(rightField.getRelationFieldIndex());

        recordColumnAccess(leftField.getField());
        recordColumnAccess(rightField.getField());
      }

      ImmutableList.Builder<Field> outputs = ImmutableList.builder();
      outputs.addAll(joinFields);

      ImmutableList.Builder<Integer> leftFields = ImmutableList.builder();
      for (int i = 0; i < left.getRelationType().getAllFieldCount(); i++) {
        if (!leftJoinFields.contains(i)) {
          outputs.add(left.getRelationType().getFieldByIndex(i));
          leftFields.add(i);
        }
      }

      ImmutableList.Builder<Integer> rightFields = ImmutableList.builder();
      for (int i = 0; i < right.getRelationType().getAllFieldCount(); i++) {
        if (!rightJoinFields.contains(i)) {
          outputs.add(right.getRelationType().getFieldByIndex(i));
          rightFields.add(i);
        }
      }

      analysis.setJoinUsing(
          node,
          new Analysis.JoinUsingAnalysis(
              leftJoinFields, rightJoinFields, leftFields.build(), rightFields.build()));

      return createAndAssignScope(node, scope, new RelationType(outputs.build()));
    }

    private void recordColumnAccess(Field field) {
      if (field.getOriginTable().isPresent() && field.getOriginColumnName().isPresent()) {
        analysis.addTableColumnReferences(
            accessControl,
            sessionContext.getIdentity(),
            ImmutableMultimap.of(field.getOriginTable().get(), field.getOriginColumnName().get()));
      }
    }

    private List<Expression> analyzeOrderBy(
        Node node, List<SortItem> sortItems, Scope orderByScope) {
      ImmutableList.Builder<Expression> orderByFieldsBuilder = ImmutableList.builder();

      for (SortItem item : sortItems) {
        Expression expression = item.getSortKey();

        if (expression instanceof LongLiteral) {
          // this is an ordinal in the output tuple

          long ordinal = ((LongLiteral) expression).getParsedValue();
          if (ordinal < 1 || ordinal > orderByScope.getRelationType().getVisibleFieldCount()) {
            throw new SemanticException(
                String.format("ORDER BY position %s is not in select list", ordinal));
          }

          expression = new FieldReference(toIntExact(ordinal - 1));
        }

        ExpressionAnalysis expressionAnalysis =
            ExpressionAnalyzer.analyzeExpression(
                metadata,
                queryContext,
                sessionContext,
                statementAnalyzerFactory,
                accessControl,
                orderByScope,
                analysis,
                expression,
                WarningCollector.NOOP,
                correlationSupport);
        analysis.recordSubqueries(node, expressionAnalysis);

        Type type = analysis.getType(expression);
        if (!type.isOrderable()) {
          throw new SemanticException(
              String.format(
                  "Type %s is not orderable, and therefore cannot be used in ORDER BY: %s",
                  type, expression));
        }

        orderByFieldsBuilder.add(expression);
      }

      return orderByFieldsBuilder.build();
    }

    private void analyzeOffset(Offset node, Scope scope) {
      long rowCount;
      if (node.getRowCount() instanceof LongLiteral) {
        rowCount = ((LongLiteral) node.getRowCount()).getParsedValue();
      } else {
        //        checkState(
        //            node.getRowCount() instanceof Parameter,
        //            "unexpected OFFSET rowCount: " +
        // node.getRowCount().getClass().getSimpleName());
        throw new SemanticException(
            "unexpected OFFSET rowCount: " + node.getRowCount().getClass().getSimpleName());
        //        OptionalLong providedValue =
        //            analyzeParameterAsRowCount((Parameter) node.getRowCount(), scope, "OFFSET");
        //        rowCount = providedValue.orElse(0);
      }
      if (rowCount < 0) {
        throw new SemanticException(
            String.format(
                "OFFSET row count must be greater or equal to 0 (actual value: %s)", rowCount));
      }
      analysis.setOffset(node, rowCount);
    }

    /**
     * @return true if the Query / QuerySpecification containing the analyzed Limit or FetchFirst,
     *     must contain orderBy (i.e., for FetchFirst with ties).
     */
    private boolean analyzeLimit(Node node, Scope scope) {
      //      checkState(
      //          node instanceof FetchFirst || node instanceof Limit,
      //          "Invalid limit node type. Expected: FetchFirst or Limit. Actual: %s",
      // node.getClass().getName());
      checkState(
          node instanceof Limit,
          "Invalid limit node type. Expected: Limit. Actual: %s",
          node.getClass().getName());
      //      if (node instanceof FetchFirst) {
      //        return analyzeLimit((FetchFirst) node, scope);
      //      }
      return analyzeLimit((Limit) node, scope);
    }

    //    private boolean analyzeLimit(FetchFirst node, Scope scope) {
    //      long rowCount = 1;
    //      if (node.getRowCount().isPresent()) {
    //        Expression count = node.getRowCount().get();
    //        if (count instanceof LongLiteral) {
    //          rowCount = ((LongLiteral) count).getParsedValue();
    //        } else {
    //          checkState(count instanceof Parameter,
    //              "unexpected FETCH FIRST rowCount: " + count.getClass().getSimpleName());
    //          OptionalLong providedValue = analyzeParameterAsRowCount((Parameter) count, scope,
    // "FETCH FIRST");
    //          if (providedValue.isPresent()) {
    //            rowCount = providedValue.getAsLong();
    //          }
    //        }
    //      }
    //      if (rowCount <= 0) {
    //        throw semanticException(NUMERIC_VALUE_OUT_OF_RANGE, node,
    //            "FETCH FIRST row count must be positive (actual value: %s)", rowCount);
    //      }
    //      analysis.setLimit(node, rowCount);
    //
    //      return node.isWithTies();
    //    }

    private boolean analyzeLimit(Limit node, Scope scope) {
      OptionalLong rowCount;
      if (node.getRowCount() instanceof AllRows) {
        rowCount = OptionalLong.empty();
      } else if (node.getRowCount() instanceof LongLiteral) {
        rowCount = OptionalLong.of(((LongLiteral) node.getRowCount()).getParsedValue());
      } else {
        //        checkState(
        //            node.getRowCount() instanceof Parameter,
        //            "unexpected LIMIT rowCount: " +
        // node.getRowCount().getClass().getSimpleName());
        throw new SemanticException(
            "unexpected LIMIT rowCount: " + node.getRowCount().getClass().getSimpleName());
        //        rowCount = analyzeParameterAsRowCount((Parameter) node.getRowCount(), scope,
        // "LIMIT");
      }
      rowCount.ifPresent(
          count -> {
            if (count < 0) {
              throw new SemanticException(
                  String.format(
                      "LIMIT row count must be greater or equal to 0 (actual value: %s)", count));
            }
          });

      analysis.setLimit(node, rowCount);

      return false;
    }

    //    private OptionalLong analyzeParameterAsRowCount(
    //        Parameter parameter, Scope scope, String context) {
    //      // validate parameter index
    //      analyzeExpression(parameter, scope);
    //      Expression providedValue = analysis.getParameters().get(NodeRef.of(parameter));
    //      Object value;
    //      try {
    //        value =
    //            evaluateConstantExpression(
    //                providedValue,
    //                BIGINT,
    //                plannerContext,
    //                session,
    //                accessControl,
    //                analysis.getParameters());
    //      } catch (VerifyException e) {
    //        throw new SemanticException(
    //            String.format("Non constant parameter value for %s: %s", context, providedValue));
    //      }
    //      if (value == null) {
    //        throw new SemanticException(
    //            String.format("Parameter value provided for %s is NULL: %s", context,
    // providedValue));
    //      }
    //      return OptionalLong.of((long) value);
    //    }

    private void analyzeAggregations(
        QuerySpecification node,
        Scope sourceScope,
        Optional<Scope> orderByScope,
        Analysis.GroupingSetAnalysis groupByAnalysis,
        List<Expression> outputExpressions,
        List<Expression> orderByExpressions) {
      checkState(
          orderByExpressions.isEmpty() || orderByScope.isPresent(),
          "non-empty orderByExpressions list without orderByScope provided");

      List<FunctionCall> aggregates =
          extractAggregateFunctions(Iterables.concat(outputExpressions, orderByExpressions));
      analysis.setAggregates(node, aggregates);

      if (analysis.isAggregation(node)) {
        // ensure SELECT, ORDER BY and HAVING are constant with respect to group
        // e.g, these are all valid expressions:
        //     SELECT f(a) GROUP BY a
        //     SELECT f(a + 1) GROUP BY a + 1
        //     SELECT a + sum(b) GROUP BY a
        List<Expression> distinctGroupingColumns =
            ImmutableSet.copyOf(groupByAnalysis.getOriginalExpressions()).asList();

        verifySourceAggregations(distinctGroupingColumns, sourceScope, outputExpressions, analysis);
        if (!orderByExpressions.isEmpty()) {
          verifyOrderByAggregations(
              distinctGroupingColumns,
              sourceScope,
              orderByScope.orElseThrow(() -> new NoSuchElementException("No value present")),
              orderByExpressions,
              analysis);
        }
      }
    }

    private ExpressionAnalysis analyzeExpression(Expression expression, Scope scope) {
      return ExpressionAnalyzer.analyzeExpression(
          metadata,
          queryContext,
          sessionContext,
          statementAnalyzerFactory,
          accessControl,
          scope,
          analysis,
          expression,
          warningCollector,
          correlationSupport);
    }

    private ExpressionAnalysis analyzeExpression(
        Expression expression, Scope scope, CorrelationSupport correlationSupport) {
      return ExpressionAnalyzer.analyzeExpression(
          metadata,
          queryContext,
          sessionContext,
          statementAnalyzerFactory,
          accessControl,
          scope,
          analysis,
          expression,
          warningCollector,
          correlationSupport);
    }

    private List<Node> findReferences(Node node, Identifier name) {
      Stream<Node> allReferences = preOrder(node).filter(isTableWithName(name));

      // TODO: recursive references could be supported in subquery before the point of shadowing.
      // currently, the recursive query name is considered shadowed in the whole subquery if the
      // subquery defines a common table with the same name
      Set<Node> shadowedReferences =
          preOrder(node)
              .filter(isQueryWithNameShadowed(name))
              .flatMap(query -> preOrder(query).filter(isTableWithName(name)))
              .collect(toImmutableSet());

      return allReferences
          .filter(reference -> !shadowedReferences.contains(reference))
          .collect(toImmutableList());
    }

    private Predicate<Node> isTableWithName(Identifier name) {
      return node -> {
        if (!(node instanceof Table)) {
          return false;
        }
        Table table = (Table) node;
        QualifiedName tableName = table.getName();
        return !tableName.getPrefix().isPresent()
            && tableName.hasSuffix(QualifiedName.of(name.getValue()));
      };
    }

    private Predicate<Node> isQueryWithNameShadowed(Identifier name) {
      return node -> {
        if (!(node instanceof Query)) {
          return false;
        }
        Query query = (Query) node;
        if (!query.getWith().isPresent()) {
          return false;
        }
        return query.getWith().get().getQueries().stream()
            .map(WithQuery::getName)
            .map(Identifier::getValue)
            .anyMatch(withQueryName -> withQueryName.equalsIgnoreCase(name.getValue()));
      };
    }

    private void validateFromClauseOfRecursiveTerm(Relation from, Identifier name) {
      preOrder(from)
          .filter(Join.class::isInstance)
          .forEach(
              node -> {
                Join join = (Join) node;
                Join.Type type = join.getType();
                if (type == LEFT || type == RIGHT || type == FULL) {
                  List<Node> leftRecursiveReferences = findReferences(join.getLeft(), name);
                  List<Node> rightRecursiveReferences = findReferences(join.getRight(), name);
                  if (!leftRecursiveReferences.isEmpty() && (type == RIGHT || type == FULL)) {
                    throw new SemanticException(
                        String.format("recursive reference in left source of %s join", type));
                  }
                  if (!rightRecursiveReferences.isEmpty() && (type == LEFT || type == FULL)) {
                    throw new SemanticException(
                        String.format("recursive reference in right source of %s join", type));
                  }
                }
              });

      preOrder(from)
          .filter(node -> node instanceof Intersect && !((Intersect) node).isDistinct())
          .forEach(
              node -> {
                Intersect intersect = (Intersect) node;
                intersect.getRelations().stream()
                    .flatMap(relation -> findReferences(relation, name).stream())
                    .findFirst()
                    .ifPresent(
                        reference -> {
                          throw new SemanticException("recursive reference in INTERSECT ALL");
                        });
              });

      preOrder(from)
          .filter(Except.class::isInstance)
          .forEach(
              node -> {
                Except except = (Except) node;
                List<Node> rightRecursiveReferences = findReferences(except.getRight(), name);
                if (!rightRecursiveReferences.isEmpty()) {
                  throw new SemanticException(
                      String.format(
                          "recursive reference in right relation of EXCEPT %s",
                          except.isDistinct() ? "DISTINCT" : "ALL"));
                }
                if (!except.isDistinct()) {
                  List<Node> leftRecursiveReferences = findReferences(except.getLeft(), name);
                  if (!leftRecursiveReferences.isEmpty()) {
                    throw new SemanticException(
                        "recursive reference in left relation of EXCEPT ALL");
                  }
                }
              });
    }

    private Scope setAliases(Scope scope, Identifier tableName, List<Identifier> columnNames) {
      RelationType oldDescriptor = scope.getRelationType();
      validateColumnAliases(columnNames, oldDescriptor.getVisibleFieldCount());
      RelationType newDescriptor =
          oldDescriptor.withAlias(
              tableName.getValue(),
              columnNames.stream().map(Identifier::getValue).collect(toImmutableList()));

      Streams.forEachPair(
          oldDescriptor.getAllFields().stream(),
          newDescriptor.getAllFields().stream(),
          (newField, field) ->
              analysis.addSourceColumns(newField, analysis.getSourceColumns(field)));
      return scope.withRelationType(newDescriptor);
    }

    private void verifySelectDistinct(
        QuerySpecification node,
        List<Expression> orderByExpressions,
        List<Expression> outputExpressions,
        Scope sourceScope,
        Scope orderByScope) {
      Set<CanonicalizationAware<Identifier>> aliases = getAliases(node.getSelect());

      Set<ScopeAware<Expression>> expressions =
          outputExpressions.stream()
              .map(e -> ScopeAware.scopeAwareKey(e, analysis, sourceScope))
              .collect(Collectors.toSet());

      for (Expression expression : orderByExpressions) {
        if (expression instanceof FieldReference) {
          continue;
        }

        // In a query such as
        //    SELECT a FROM t ORDER BY a
        // the "a" in the SELECT clause is bound to the FROM scope, while the "a" in ORDER BY clause
        // is bound
        // to the "a" from the SELECT clause, so we can't compare by field id / relation id.
        if (expression instanceof Identifier
            && aliases.contains(canonicalizationAwareKey(expression))) {
          continue;
        }

        if (!expressions.contains(ScopeAware.scopeAwareKey(expression, analysis, orderByScope))) {
          throw new SemanticException(
              "For SELECT DISTINCT, ORDER BY expressions must appear in select list");
        }
      }

      //      for (Expression expression : orderByExpressions) {
      //        if (!isDeterministic(expression, this::getResolvedFunction)) {
      //          throw new SemanticException("Non deterministic ORDER BY expression is not
      // supported with SELECT DISTINCT");
      //        }
      //      }
    }

    private Set<CanonicalizationAware<Identifier>> getAliases(Select node) {
      ImmutableSet.Builder<CanonicalizationAware<Identifier>> aliases = ImmutableSet.builder();
      for (SelectItem item : node.getSelectItems()) {
        if (item instanceof SingleColumn) {
          SingleColumn column = (SingleColumn) item;
          Optional<Identifier> alias = column.getAlias();
          if (alias.isPresent()) {
            aliases.add(canonicalizationAwareKey(alias.get()));
          } else if (column.getExpression() instanceof Identifier) {
            Identifier identifier = (Identifier) column.getExpression();
            aliases.add(canonicalizationAwareKey(identifier));
          } else if (column.getExpression() instanceof DereferenceExpression) {
            DereferenceExpression dereferenceExpression =
                (DereferenceExpression) column.getExpression();
            aliases.add(
                canonicalizationAwareKey(
                    dereferenceExpression
                        .getField()
                        .orElseThrow(() -> new NoSuchElementException("No value present"))));
          }
        } else if (item instanceof AllColumns) {
          AllColumns allColumns = (AllColumns) item;
          List<Field> fields = analysis.getSelectAllResultFields(allColumns);
          checkNotNull(fields, "output fields is null for select item %s", item);
          for (int i = 0; i < fields.size(); i++) {
            Field field = fields.get(i);

            if (!allColumns.getAliases().isEmpty()) {
              aliases.add(canonicalizationAwareKey(allColumns.getAliases().get(i)));
            } else if (field.getName().isPresent()) {
              aliases.add(canonicalizationAwareKey(new Identifier(field.getName().get())));
            }
          }
        }
      }

      return aliases.build();
    }

    private void validateProperties(final List<Property> properties, final Optional<Scope> scope) {
      final Set<String> propertyNames = new HashSet<>();
      for (final Property property : properties) {
        final String key = property.getName().getValue().toLowerCase(Locale.ENGLISH);
        if (!TABLE_ALLOWED_PROPERTIES_2_DEFAULT_VALUE_MAP.containsKey(key)) {
          throw new SemanticException("Table property " + key + " is currently not allowed.");
        }
        if (!propertyNames.add(key)) {
          throw new SemanticException(
              String.format("Duplicate property: %s", property.getName().getValue()));
        }
        if (!property.isSetToDefault()) {
          final Expression value = property.getNonDefaultValue();
          if (!(value instanceof LongLiteral)) {
            throw new SemanticException(
                "TTL' value must be a LongLiteral, but now is: " + value.toString());
          }
        }
      }
      for (final Property property : properties) {
        process(property, scope);
      }
    }

    private void validateColumns(Statement node, RelationType descriptor) {
      // verify that all column names are specified and unique
      // TODO: collect errors and return them all at once
      Set<String> names = new HashSet<>();
      for (Field field : descriptor.getVisibleFields()) {
        String fieldName =
            field
                .getName()
                .orElseThrow(
                    () ->
                        new SemanticException(
                            String.format(
                                "Column name not specified at position %s",
                                descriptor.indexOf(field) + 1)));
        if (!names.add(fieldName)) {
          throw new SemanticException(
              String.format("Column name '%s' specified more than once", fieldName));
        }
      }
    }

    private void validateColumnAliases(List<Identifier> columnAliases, int sourceColumnSize) {
      validateColumnAliasesCount(columnAliases, sourceColumnSize);
      Set<String> names = new HashSet<>();
      for (Identifier identifier : columnAliases) {
        if (names.contains(identifier.getValue().toLowerCase(ENGLISH))) {
          throw new SemanticException(
              String.format("Column name '%s' specified more than once", identifier.getValue()));
        }
        names.add(identifier.getValue().toLowerCase(ENGLISH));
      }
    }

    private void validateColumnAliasesCount(List<Identifier> columnAliases, int sourceColumnSize) {
      if (columnAliases.size() != sourceColumnSize) {
        throw new SemanticException(
            String.format(
                "Column alias list has %s entries but relation has %s columns",
                columnAliases.size(), sourceColumnSize));
      }
    }

    private Scope createAndAssignScope(Node node, Optional<Scope> parentScope) {
      return createAndAssignScope(node, parentScope, emptyList());
    }

    private Scope createAndAssignScope(Node node, Optional<Scope> parentScope, Field... fields) {
      return createAndAssignScope(node, parentScope, new RelationType(fields));
    }

    private Scope createAndAssignScope(Node node, Optional<Scope> parentScope, List<Field> fields) {
      return createAndAssignScope(node, parentScope, new RelationType(fields));
    }

    private Scope createAndAssignScope(
        Node node, Optional<Scope> parentScope, RelationType relationType) {
      Scope scope =
          scopeBuilder(parentScope).withRelationType(RelationId.of(node), relationType).build();

      analysis.setScope(node, scope);
      return scope;
    }

    private Scope createScope(Optional<Scope> parentScope) {
      return scopeBuilder(parentScope).build();
    }

    private Scope.Builder scopeBuilder(Optional<Scope> parentScope) {
      Scope.Builder scopeBuilder = Scope.builder();

      if (parentScope.isPresent()) {
        // parent scope represents local query scope hierarchy. Local query scope
        // hierarchy should have outer query scope as ancestor already.
        scopeBuilder.withParent(parentScope.get());
      } else {
        outerQueryScope.ifPresent(scopeBuilder::withOuterQueryParent);
      }

      return scopeBuilder;
    }

    @Override
    protected Scope visitCreateDevice(
        final CreateOrUpdateDevice node, final Optional<Scope> context) {
      queryContext.setQueryType(QueryType.WRITE);
      return null;
    }

    @Override
    protected Scope visitFetchDevice(final FetchDevice node, final Optional<Scope> context) {
      return null;
    }

    @Override
    protected Scope visitShowDevice(final ShowDevice node, final Optional<Scope> context) {
      analyzeQueryDevice(node, context);
      // TODO: use real scope when parameter in offset and limit is supported
      if (Objects.nonNull(node.getOffset())) {
        analyzeOffset(node.getOffset(), null);
      }
      if (Objects.nonNull(node.getLimit())) {
        analyzeLimit(node.getLimit(), null);
      }
      return null;
    }

    @Override
    protected Scope visitCountDevice(final CountDevice node, final Optional<Scope> context) {
      analyzeQueryDevice(node, context);
      return null;
    }

    private void analyzeQueryDevice(
        final AbstractQueryDeviceWithCache node, final Optional<Scope> context) {
      analyzeTraverseDevice(node, context, node.getWhere().isPresent());
      final TsTable table =
          DataNodeTableCache.getInstance().getTable(node.getDatabase(), node.getTableName());
      if (!node.parseRawExpression(
          table,
          table.getColumnList().stream()
              .filter(
                  columnSchema ->
                      columnSchema.getColumnCategory().equals(TsTableColumnCategory.ATTRIBUTE))
              .map(TsTableColumnSchema::getColumnName)
              .collect(Collectors.toList()),
          queryContext)) {
        // Cache hit
        // Currently we disallow "Or" filter for precise get, thus if it hit cache
        // it'll be only one device
        // TODO: Ensure the disjointness of expressions and allow Or filter
        analysis.setFinishQueryAfterAnalyze();
      }
    }

    // NOTICE: We construct transition map here because currently we set the used fields in
    // the statement. Other queries shall not do this and shall do it in logical plan phase.
    private TranslationMap analyzeTraverseDevice(
        final AbstractTraverseDevice node,
        final Optional<Scope> context,
        final boolean shallCreateTranslationMap) {
      node.parseTable(sessionContext);

      final String database = node.getDatabase();
      final String tableName = node.getTableName();

      if (Objects.isNull(database)) {
        throw new SemanticException("The database must be set before show devices.");
      }

      if (!metadata.tableExists(new QualifiedObjectName(database, tableName))) {
        throw new SemanticException(
            String.format("Table '%s.%s' does not exist.", database, tableName));
      }
      node.setColumnHeaderList();

      TranslationMap translationMap = null;
      if (shallCreateTranslationMap) {
        final QualifiedObjectName name = new QualifiedObjectName(database, tableName);
        final Optional<TableSchema> tableSchema = metadata.getTableSchema(sessionContext, name);
        // This can only be a table
        if (!tableSchema.isPresent()) {
          throw new SemanticException(String.format("Table '%s' does not exist", name));
        }

        final TableSchema originalSchema = tableSchema.get();
        final ImmutableList.Builder<Field> fields = ImmutableList.builder();
        fields.addAll(
            analyzeTableOutputFields(
                node.getTable(),
                name,
                new TableSchema(
                    originalSchema.getTableName(),
                    originalSchema.getColumns().stream()
                        .filter(
                            columnSchema ->
                                columnSchema.getColumnCategory() == TsTableColumnCategory.ID
                                    || columnSchema.getColumnCategory()
                                        == TsTableColumnCategory.ATTRIBUTE)
                        .collect(Collectors.toList()))));
        final List<Field> fieldList = fields.build();
        final Scope scope = createAndAssignScope(node, context, fieldList);
        translationMap =
            new TranslationMap(
                Optional.empty(),
                scope,
                analysis,
                fieldList.stream()
                    .map(field -> Symbol.of(field.getName().orElse(null)))
                    .collect(Collectors.toList()),
                new PlannerContext(metadata, null));

        if (node.getWhere().isPresent()) {
          analyzeWhere(node, translationMap.getScope(), node.getWhere().get());
          node.setWhere(translationMap.rewrite(analysis.getWhere(node)));
        }
      }

      return translationMap;
    }

    private Expression analyzeAndRewriteExpression(
        final TranslationMap translationMap, final Scope scope, final Expression expression) {
      analyzeExpression(expression, scope);
      scope.getRelationType().getAllFields();
      return translationMap.rewrite(expression);
    }
  }

  private static boolean hasScopeAsLocalParent(Scope root, Scope parent) {
    Scope scope = root;
    while (scope.getLocalParent().isPresent()) {
      scope = scope.getLocalParent().get();
      if (scope.equals(parent)) {
        return true;
      }
    }

    return false;
  }

  static void verifyNoAggregateWindowOrGroupingFunctions(Expression predicate, String clause) {
    List<FunctionCall> aggregates = extractAggregateFunctions(ImmutableList.of(predicate));

    if (!aggregates.isEmpty()) {
      throw new SemanticException(
          String.format(
              "%s cannot contain aggregations, window functions or grouping operations: %s",
              clause, aggregates));
    }
  }
}
