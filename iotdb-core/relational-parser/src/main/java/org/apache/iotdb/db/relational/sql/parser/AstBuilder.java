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

package org.apache.iotdb.db.relational.sql.parser;

import org.apache.iotdb.db.relational.grammar.sql.RelationalSqlBaseVisitor;
import org.apache.iotdb.db.relational.grammar.sql.RelationalSqlLexer;
import org.apache.iotdb.db.relational.grammar.sql.RelationalSqlParser;
import org.apache.iotdb.db.relational.sql.tree.AddColumn;
import org.apache.iotdb.db.relational.sql.tree.AliasedRelation;
import org.apache.iotdb.db.relational.sql.tree.AllColumns;
import org.apache.iotdb.db.relational.sql.tree.AllRows;
import org.apache.iotdb.db.relational.sql.tree.ArithmeticBinaryExpression;
import org.apache.iotdb.db.relational.sql.tree.ArithmeticUnaryExpression;
import org.apache.iotdb.db.relational.sql.tree.BetweenPredicate;
import org.apache.iotdb.db.relational.sql.tree.BinaryLiteral;
import org.apache.iotdb.db.relational.sql.tree.BooleanLiteral;
import org.apache.iotdb.db.relational.sql.tree.Cast;
import org.apache.iotdb.db.relational.sql.tree.CoalesceExpression;
import org.apache.iotdb.db.relational.sql.tree.ColumnDefinition;
import org.apache.iotdb.db.relational.sql.tree.ComparisonExpression;
import org.apache.iotdb.db.relational.sql.tree.CreateDB;
import org.apache.iotdb.db.relational.sql.tree.CreateIndex;
import org.apache.iotdb.db.relational.sql.tree.CreateTable;
import org.apache.iotdb.db.relational.sql.tree.CurrentDatabase;
import org.apache.iotdb.db.relational.sql.tree.CurrentTime;
import org.apache.iotdb.db.relational.sql.tree.CurrentUser;
import org.apache.iotdb.db.relational.sql.tree.DataType;
import org.apache.iotdb.db.relational.sql.tree.DataTypeParameter;
import org.apache.iotdb.db.relational.sql.tree.Delete;
import org.apache.iotdb.db.relational.sql.tree.DereferenceExpression;
import org.apache.iotdb.db.relational.sql.tree.DescribeTable;
import org.apache.iotdb.db.relational.sql.tree.DoubleLiteral;
import org.apache.iotdb.db.relational.sql.tree.DropColumn;
import org.apache.iotdb.db.relational.sql.tree.DropDB;
import org.apache.iotdb.db.relational.sql.tree.DropIndex;
import org.apache.iotdb.db.relational.sql.tree.DropTable;
import org.apache.iotdb.db.relational.sql.tree.Except;
import org.apache.iotdb.db.relational.sql.tree.ExistsPredicate;
import org.apache.iotdb.db.relational.sql.tree.Expression;
import org.apache.iotdb.db.relational.sql.tree.FunctionCall;
import org.apache.iotdb.db.relational.sql.tree.GenericDataType;
import org.apache.iotdb.db.relational.sql.tree.GroupBy;
import org.apache.iotdb.db.relational.sql.tree.GroupingElement;
import org.apache.iotdb.db.relational.sql.tree.GroupingSets;
import org.apache.iotdb.db.relational.sql.tree.Identifier;
import org.apache.iotdb.db.relational.sql.tree.IfExpression;
import org.apache.iotdb.db.relational.sql.tree.InListExpression;
import org.apache.iotdb.db.relational.sql.tree.InPredicate;
import org.apache.iotdb.db.relational.sql.tree.Insert;
import org.apache.iotdb.db.relational.sql.tree.Intersect;
import org.apache.iotdb.db.relational.sql.tree.IsNotNullPredicate;
import org.apache.iotdb.db.relational.sql.tree.IsNullPredicate;
import org.apache.iotdb.db.relational.sql.tree.Join;
import org.apache.iotdb.db.relational.sql.tree.JoinCriteria;
import org.apache.iotdb.db.relational.sql.tree.JoinOn;
import org.apache.iotdb.db.relational.sql.tree.JoinUsing;
import org.apache.iotdb.db.relational.sql.tree.LikePredicate;
import org.apache.iotdb.db.relational.sql.tree.Limit;
import org.apache.iotdb.db.relational.sql.tree.LogicalExpression;
import org.apache.iotdb.db.relational.sql.tree.LongLiteral;
import org.apache.iotdb.db.relational.sql.tree.NaturalJoin;
import org.apache.iotdb.db.relational.sql.tree.Node;
import org.apache.iotdb.db.relational.sql.tree.NodeLocation;
import org.apache.iotdb.db.relational.sql.tree.NotExpression;
import org.apache.iotdb.db.relational.sql.tree.NullIfExpression;
import org.apache.iotdb.db.relational.sql.tree.NullLiteral;
import org.apache.iotdb.db.relational.sql.tree.NumericParameter;
import org.apache.iotdb.db.relational.sql.tree.Offset;
import org.apache.iotdb.db.relational.sql.tree.OrderBy;
import org.apache.iotdb.db.relational.sql.tree.Parameter;
import org.apache.iotdb.db.relational.sql.tree.Property;
import org.apache.iotdb.db.relational.sql.tree.QualifiedName;
import org.apache.iotdb.db.relational.sql.tree.QuantifiedComparisonExpression;
import org.apache.iotdb.db.relational.sql.tree.Query;
import org.apache.iotdb.db.relational.sql.tree.QueryBody;
import org.apache.iotdb.db.relational.sql.tree.QuerySpecification;
import org.apache.iotdb.db.relational.sql.tree.Relation;
import org.apache.iotdb.db.relational.sql.tree.RenameColumn;
import org.apache.iotdb.db.relational.sql.tree.RenameTable;
import org.apache.iotdb.db.relational.sql.tree.Row;
import org.apache.iotdb.db.relational.sql.tree.SearchedCaseExpression;
import org.apache.iotdb.db.relational.sql.tree.Select;
import org.apache.iotdb.db.relational.sql.tree.SelectItem;
import org.apache.iotdb.db.relational.sql.tree.SetProperties;
import org.apache.iotdb.db.relational.sql.tree.ShowDB;
import org.apache.iotdb.db.relational.sql.tree.ShowDevice;
import org.apache.iotdb.db.relational.sql.tree.ShowIndex;
import org.apache.iotdb.db.relational.sql.tree.ShowTables;
import org.apache.iotdb.db.relational.sql.tree.SimpleCaseExpression;
import org.apache.iotdb.db.relational.sql.tree.SimpleGroupBy;
import org.apache.iotdb.db.relational.sql.tree.SingleColumn;
import org.apache.iotdb.db.relational.sql.tree.SortItem;
import org.apache.iotdb.db.relational.sql.tree.StringLiteral;
import org.apache.iotdb.db.relational.sql.tree.SubqueryExpression;
import org.apache.iotdb.db.relational.sql.tree.Table;
import org.apache.iotdb.db.relational.sql.tree.TableSubquery;
import org.apache.iotdb.db.relational.sql.tree.Trim;
import org.apache.iotdb.db.relational.sql.tree.TypeParameter;
import org.apache.iotdb.db.relational.sql.tree.Union;
import org.apache.iotdb.db.relational.sql.tree.Update;
import org.apache.iotdb.db.relational.sql.tree.UpdateAssignment;
import org.apache.iotdb.db.relational.sql.tree.Use;
import org.apache.iotdb.db.relational.sql.tree.Values;
import org.apache.iotdb.db.relational.sql.tree.WhenClause;
import org.apache.iotdb.db.relational.sql.tree.With;
import org.apache.iotdb.db.relational.sql.tree.WithQuery;

import com.google.common.collect.ImmutableList;
import org.antlr.v4.runtime.ParserRuleContext;
import org.antlr.v4.runtime.Token;
import org.antlr.v4.runtime.tree.ParseTree;
import org.antlr.v4.runtime.tree.TerminalNode;

import javax.annotation.Nullable;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.function.Function;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toList;
import static org.apache.iotdb.db.relational.sql.tree.GroupingSets.Type.CUBE;
import static org.apache.iotdb.db.relational.sql.tree.GroupingSets.Type.EXPLICIT;
import static org.apache.iotdb.db.relational.sql.tree.GroupingSets.Type.ROLLUP;

public class AstBuilder extends RelationalSqlBaseVisitor<Node> {

  private int parameterPosition;

  @Nullable private final NodeLocation baseLocation;

  AstBuilder(@Nullable NodeLocation baseLocation) {
    this.baseLocation = baseLocation;
  }

  @Override
  public Node visitSingleStatement(RelationalSqlParser.SingleStatementContext ctx) {
    return visit(ctx.statement());
  }

  @Override
  public Node visitStandaloneExpression(RelationalSqlParser.StandaloneExpressionContext context) {
    return visit(context.expression());
  }

  @Override
  public Node visitStandaloneType(RelationalSqlParser.StandaloneTypeContext context) {
    return visit(context.type());
  }

  // ******************* statements **********************
  @Override
  public Node visitUseDatabaseStatement(RelationalSqlParser.UseDatabaseStatementContext ctx) {
    return new Use(getLocation(ctx), (Identifier) visit(ctx.database));
  }

  @Override
  public Node visitShowDatabasesStatement(RelationalSqlParser.ShowDatabasesStatementContext ctx) {
    return new ShowDB(getLocation(ctx));
  }

  @Override
  public Node visitCreateDbStatement(RelationalSqlParser.CreateDbStatementContext ctx) {
    List<Property> properties = ImmutableList.of();
    if (ctx.properties() != null) {
      properties = visit(ctx.properties().propertyAssignments().property(), Property.class);
    }

    return new CreateDB(
        getLocation(ctx),
        ctx.EXISTS() != null,
        ((Identifier) visit(ctx.database)).getValue(),
        properties);
  }

  @Override
  public Node visitDropDbStatement(RelationalSqlParser.DropDbStatementContext ctx) {
    return new DropDB(getLocation(ctx), (Identifier) visit(ctx.database), ctx.EXISTS() != null);
  }

  @Override
  public Node visitCreateTableStatement(RelationalSqlParser.CreateTableStatementContext ctx) {
    List<Property> properties = ImmutableList.of();
    if (ctx.properties() != null) {
      properties = visit(ctx.properties().propertyAssignments().property(), Property.class);
    }
    return new CreateTable(
        getLocation(ctx),
        getQualifiedName(ctx.qualifiedName()),
        visit(ctx.columnDefinition(), ColumnDefinition.class),
        ctx.EXISTS() != null,
        ctx.charsetDesc() == null
            ? null
            : ((Identifier) visit(ctx.charsetDesc().identifierOrString())).getValue(),
        properties);
  }

  @Override
  public Node visitColumnDefinition(RelationalSqlParser.ColumnDefinitionContext ctx) {
    return new ColumnDefinition(
        getLocation(ctx),
        (Identifier) visit(ctx.identifier()),
        (DataType) visit(ctx.type()),
        getColumnCategory(ctx.columnCategory),
        ctx.charsetName() == null
            ? null
            : ((Identifier) visit(ctx.charsetName().identifier())).getValue());
  }

  @Override
  public Node visitDropTableStatement(RelationalSqlParser.DropTableStatementContext ctx) {
    return new DropTable(
        getLocation(ctx), getQualifiedName(ctx.qualifiedName()), ctx.EXISTS() != null);
  }

  @Override
  public Node visitShowTableStatement(RelationalSqlParser.ShowTableStatementContext ctx) {
    if (ctx.database == null) {
      return new ShowTables(getLocation(ctx));
    } else {
      return new ShowTables(getLocation(ctx), (Identifier) visit(ctx.database));
    }
  }

  @Override
  public Node visitDescTableStatement(RelationalSqlParser.DescTableStatementContext ctx) {
    return new DescribeTable(getLocation(ctx), getQualifiedName(ctx.table));
  }

  @Override
  public Node visitRenameTable(RelationalSqlParser.RenameTableContext ctx) {
    return new RenameTable(
        getLocation(ctx), getQualifiedName(ctx.from), (Identifier) visit(ctx.to));
  }

  @Override
  public Node visitAddColumn(RelationalSqlParser.AddColumnContext ctx) {
    return new AddColumn(getQualifiedName(ctx.tableName), (ColumnDefinition) visit(ctx.column));
  }

  @Override
  public Node visitRenameColumn(RelationalSqlParser.RenameColumnContext ctx) {
    return new RenameColumn(
        getLocation(ctx),
        getQualifiedName(ctx.tableName),
        (Identifier) visit(ctx.from),
        (Identifier) visit(ctx.to));
  }

  @Override
  public Node visitDropColumn(RelationalSqlParser.DropColumnContext ctx) {
    return new DropColumn(
        getLocation(ctx), getQualifiedName(ctx.tableName), (Identifier) visit(ctx.column));
  }

  @Override
  public Node visitSetTableProperties(RelationalSqlParser.SetTablePropertiesContext ctx) {
    List<Property> properties = ImmutableList.of();
    if (ctx.propertyAssignments() != null) {
      properties = visit(ctx.propertyAssignments().property(), Property.class);
    }
    return new SetProperties(
        getLocation(ctx),
        SetProperties.Type.TABLE,
        getQualifiedName(ctx.qualifiedName()),
        properties);
  }

  @Override
  public Node visitCreateIndexStatement(RelationalSqlParser.CreateIndexStatementContext ctx) {
    return new CreateIndex(
        getLocation(ctx),
        getQualifiedName(ctx.tableName),
        (Identifier) visit(ctx.indexName),
        visit(ctx.identifierList().identifier(), Identifier.class));
  }

  @Override
  public Node visitDropIndexStatement(RelationalSqlParser.DropIndexStatementContext ctx) {
    return new DropIndex(
        getLocation(ctx), getQualifiedName(ctx.tableName), (Identifier) visit(ctx.indexName));
  }

  @Override
  public Node visitShowIndexStatement(RelationalSqlParser.ShowIndexStatementContext ctx) {
    return new ShowIndex(getLocation(ctx), getQualifiedName(ctx.tableName));
  }

  @Override
  public Node visitInsertStatement(RelationalSqlParser.InsertStatementContext ctx) {
    if (ctx.columnAliases() != null) {
      return new Insert(
          new Table(getQualifiedName(ctx.tableName)),
          visit(ctx.columnAliases().identifier(), Identifier.class),
          (Query) visit(ctx.query()));
    } else {
      return new Insert(new Table(getQualifiedName(ctx.tableName)), (Query) visit(ctx.query()));
    }
  }

  @Override
  public Node visitDeleteStatement(RelationalSqlParser.DeleteStatementContext ctx) {
    if (ctx.booleanExpression() != null) {
      return new Delete(
          getLocation(ctx),
          new Table(getLocation(ctx), getQualifiedName(ctx.tableName)),
          (Expression) visit(ctx.booleanExpression()));
    } else {
      return new Delete(
          getLocation(ctx), new Table(getLocation(ctx), getQualifiedName(ctx.tableName)));
    }
  }

  @Override
  public Node visitUpdateStatement(RelationalSqlParser.UpdateStatementContext ctx) {
    if (ctx.booleanExpression() != null) {
      return new Update(
          getLocation(ctx),
          new Table(getLocation(ctx), getQualifiedName(ctx.qualifiedName())),
          visit(ctx.updateAssignment(), UpdateAssignment.class),
          (Expression) visit(ctx.booleanExpression()));
    } else {
      return new Update(
          getLocation(ctx),
          new Table(getLocation(ctx), getQualifiedName(ctx.qualifiedName())),
          visit(ctx.updateAssignment(), UpdateAssignment.class));
    }
  }

  @Override
  public Node visitUpdateAssignment(RelationalSqlParser.UpdateAssignmentContext ctx) {
    return new UpdateAssignment(
        (Identifier) visit(ctx.identifier()), (Expression) visit(ctx.expression()));
  }

  @Override
  public Node visitCreateFunctionStatement(RelationalSqlParser.CreateFunctionStatementContext ctx) {
    return super.visitCreateFunctionStatement(ctx);
  }

  @Override
  public Node visitUriClause(RelationalSqlParser.UriClauseContext ctx) {
    return super.visitUriClause(ctx);
  }

  @Override
  public Node visitDropFunctionStatement(RelationalSqlParser.DropFunctionStatementContext ctx) {
    return super.visitDropFunctionStatement(ctx);
  }

  @Override
  public Node visitShowFunctionsStatement(RelationalSqlParser.ShowFunctionsStatementContext ctx) {
    return super.visitShowFunctionsStatement(ctx);
  }

  @Override
  public Node visitLoadTsFileStatement(RelationalSqlParser.LoadTsFileStatementContext ctx) {
    return super.visitLoadTsFileStatement(ctx);
  }

  @Override
  public Node visitShowDevicesStatement(RelationalSqlParser.ShowDevicesStatementContext ctx) {
    // todo parse where clause
    return new ShowDevice(getLocation(ctx));
  }

  @Override
  public Node visitCountDevicesStatement(RelationalSqlParser.CountDevicesStatementContext ctx) {
    return super.visitCountDevicesStatement(ctx);
  }

  @Override
  public Node visitShowClusterStatement(RelationalSqlParser.ShowClusterStatementContext ctx) {
    return super.visitShowClusterStatement(ctx);
  }

  @Override
  public Node visitShowRegionsStatement(RelationalSqlParser.ShowRegionsStatementContext ctx) {
    return super.visitShowRegionsStatement(ctx);
  }

  @Override
  public Node visitShowDataNodesStatement(RelationalSqlParser.ShowDataNodesStatementContext ctx) {
    return super.visitShowDataNodesStatement(ctx);
  }

  @Override
  public Node visitShowConfigNodesStatement(
      RelationalSqlParser.ShowConfigNodesStatementContext ctx) {
    return super.visitShowConfigNodesStatement(ctx);
  }

  @Override
  public Node visitShowClusterIdStatement(RelationalSqlParser.ShowClusterIdStatementContext ctx) {
    return super.visitShowClusterIdStatement(ctx);
  }

  @Override
  public Node visitShowRegionIdStatement(RelationalSqlParser.ShowRegionIdStatementContext ctx) {
    return super.visitShowRegionIdStatement(ctx);
  }

  @Override
  public Node visitShowTimeSlotListStatement(
      RelationalSqlParser.ShowTimeSlotListStatementContext ctx) {
    return super.visitShowTimeSlotListStatement(ctx);
  }

  @Override
  public Node visitCountTimeSlotListStatement(
      RelationalSqlParser.CountTimeSlotListStatementContext ctx) {
    return super.visitCountTimeSlotListStatement(ctx);
  }

  @Override
  public Node visitShowSeriesSlotListStatement(
      RelationalSqlParser.ShowSeriesSlotListStatementContext ctx) {
    return super.visitShowSeriesSlotListStatement(ctx);
  }

  @Override
  public Node visitMigrateRegionStatement(RelationalSqlParser.MigrateRegionStatementContext ctx) {
    return super.visitMigrateRegionStatement(ctx);
  }

  @Override
  public Node visitShowVariablesStatement(RelationalSqlParser.ShowVariablesStatementContext ctx) {
    return super.visitShowVariablesStatement(ctx);
  }

  @Override
  public Node visitFlushStatement(RelationalSqlParser.FlushStatementContext ctx) {
    return super.visitFlushStatement(ctx);
  }

  @Override
  public Node visitClearCacheStatement(RelationalSqlParser.ClearCacheStatementContext ctx) {
    return super.visitClearCacheStatement(ctx);
  }

  @Override
  public Node visitRepairDataStatement(RelationalSqlParser.RepairDataStatementContext ctx) {
    return super.visitRepairDataStatement(ctx);
  }

  @Override
  public Node visitSetSystemStatusStatement(
      RelationalSqlParser.SetSystemStatusStatementContext ctx) {
    return super.visitSetSystemStatusStatement(ctx);
  }

  @Override
  public Node visitShowVersionStatement(RelationalSqlParser.ShowVersionStatementContext ctx) {
    return super.visitShowVersionStatement(ctx);
  }

  @Override
  public Node visitShowQueriesStatement(RelationalSqlParser.ShowQueriesStatementContext ctx) {
    return super.visitShowQueriesStatement(ctx);
  }

  @Override
  public Node visitKillQueryStatement(RelationalSqlParser.KillQueryStatementContext ctx) {
    return super.visitKillQueryStatement(ctx);
  }

  @Override
  public Node visitLoadConfigurationStatement(
      RelationalSqlParser.LoadConfigurationStatementContext ctx) {
    return super.visitLoadConfigurationStatement(ctx);
  }

  @Override
  public Node visitLocalOrClusterMode(RelationalSqlParser.LocalOrClusterModeContext ctx) {
    return super.visitLocalOrClusterMode(ctx);
  }

  @Override
  public Node visitStatementDefault(RelationalSqlParser.StatementDefaultContext ctx) {
    return super.visitStatementDefault(ctx);
  }

  @Override
  public Node visitExplain(RelationalSqlParser.ExplainContext ctx) {
    return super.visitExplain(ctx);
  }

  @Override
  public Node visitExplainAnalyze(RelationalSqlParser.ExplainAnalyzeContext ctx) {
    return super.visitExplainAnalyze(ctx);
  }

  // ********************** query expressions ********************
  @Override
  public Node visitQuery(RelationalSqlParser.QueryContext ctx) {
    Query body = (Query) visit(ctx.queryNoWith());

    return new Query(
        getLocation(ctx),
        visitIfPresent(ctx.with(), With.class),
        body.getQueryBody(),
        body.getOrderBy(),
        body.getOffset(),
        body.getLimit());
  }

  @Override
  public Node visitWith(RelationalSqlParser.WithContext ctx) {
    return new With(
        getLocation(ctx), ctx.RECURSIVE() != null, visit(ctx.namedQuery(), WithQuery.class));
  }

  @Override
  public Node visitNamedQuery(RelationalSqlParser.NamedQueryContext ctx) {
    if (ctx.columnAliases() != null) {
      List<Identifier> columns = visit(ctx.columnAliases().identifier(), Identifier.class);
      return new WithQuery(
          getLocation(ctx), (Identifier) visit(ctx.name), (Query) visit(ctx.query()), columns);
    } else {
      return new WithQuery(
          getLocation(ctx), (Identifier) visit(ctx.name), (Query) visit(ctx.query()));
    }
  }

  @Override
  public Node visitQueryNoWith(RelationalSqlParser.QueryNoWithContext ctx) {
    QueryBody term = (QueryBody) visit(ctx.queryTerm());

    Optional<OrderBy> orderBy = Optional.empty();
    if (ctx.ORDER() != null) {
      orderBy =
          Optional.of(new OrderBy(getLocation(ctx.ORDER()), visit(ctx.sortItem(), SortItem.class)));
    }

    Optional<Offset> offset = Optional.empty();
    if (ctx.OFFSET() != null) {
      Expression rowCount;
      if (ctx.offset.INTEGER_VALUE() != null) {
        rowCount = new LongLiteral(getLocation(ctx.offset.INTEGER_VALUE()), ctx.offset.getText());
      } else {
        rowCount = new Parameter(getLocation(ctx.offset.QUESTION_MARK()), parameterPosition);
        parameterPosition++;
      }
      offset = Optional.of(new Offset(getLocation(ctx.OFFSET()), rowCount));
    }

    Optional<Node> limit = Optional.empty();
    if (ctx.LIMIT() != null) {
      if (ctx.limit == null) {
        throw new IllegalStateException("Missing LIMIT value");
      }
      Expression rowCount;
      if (ctx.limit.ALL() != null) {
        rowCount = new AllRows(getLocation(ctx.limit.ALL()));
      } else if (ctx.limit.rowCount().INTEGER_VALUE() != null) {
        rowCount =
            new LongLiteral(getLocation(ctx.limit.rowCount().INTEGER_VALUE()), ctx.limit.getText());
      } else {
        rowCount =
            new Parameter(getLocation(ctx.limit.rowCount().QUESTION_MARK()), parameterPosition);
        parameterPosition++;
      }

      limit = Optional.of(new Limit(getLocation(ctx.LIMIT()), rowCount));
    }

    if (term instanceof QuerySpecification) {
      // When we have a simple query specification
      // followed by order by, offset, limit or fetch,
      // fold the order by, limit, offset or fetch clauses
      // into the query specification (analyzer/planner
      // expects this structure to resolve references with respect
      // to columns defined in the query specification)
      QuerySpecification query = (QuerySpecification) term;

      return new Query(
          getLocation(ctx),
          Optional.empty(),
          new QuerySpecification(
              getLocation(ctx),
              query.getSelect(),
              query.getFrom(),
              query.getWhere(),
              query.getGroupBy(),
              query.getHaving(),
              orderBy,
              offset,
              limit),
          Optional.empty(),
          Optional.empty(),
          Optional.empty());
    }

    return new Query(getLocation(ctx), Optional.empty(), term, orderBy, offset, limit);
  }

  @Override
  public Node visitQuerySpecification(RelationalSqlParser.QuerySpecificationContext ctx) {
    Optional<Relation> from = Optional.empty();
    List<SelectItem> selectItems = visit(ctx.selectItem(), SelectItem.class);

    List<Relation> relations = visit(ctx.relation(), Relation.class);
    if (!relations.isEmpty()) {
      // synthesize implicit join nodes
      Iterator<Relation> iterator = relations.iterator();
      Relation relation = iterator.next();

      while (iterator.hasNext()) {
        relation = new Join(getLocation(ctx), Join.Type.IMPLICIT, relation, iterator.next());
      }

      from = Optional.of(relation);
    }

    return new QuerySpecification(
        getLocation(ctx),
        new Select(getLocation(ctx.SELECT()), isDistinct(ctx.setQuantifier()), selectItems),
        from,
        visitIfPresent(ctx.where, Expression.class),
        visitIfPresent(ctx.groupBy(), GroupBy.class),
        visitIfPresent(ctx.having, Expression.class),
        Optional.empty(),
        Optional.empty(),
        Optional.empty());
  }

  @Override
  public Node visitSelectSingle(RelationalSqlParser.SelectSingleContext ctx) {
    if (ctx.identifier() != null) {
      return new SingleColumn(
          getLocation(ctx),
          (Expression) visit(ctx.expression()),
          (Identifier) visit(ctx.identifier()));
    } else {
      return new SingleColumn(getLocation(ctx), (Expression) visit(ctx.expression()));
    }
  }

  @Override
  public Node visitSelectAll(RelationalSqlParser.SelectAllContext ctx) {
    List<Identifier> aliases = ImmutableList.of();
    if (ctx.columnAliases() != null) {
      aliases = visit(ctx.columnAliases().identifier(), Identifier.class);
    }

    if (ctx.primaryExpression() != null) {
      return new AllColumns(getLocation(ctx), (Expression) visit(ctx.primaryExpression()), aliases);
    } else {
      return new AllColumns(getLocation(ctx), aliases);
    }
  }

  @Override
  public Node visitGroupBy(RelationalSqlParser.GroupByContext ctx) {
    return new GroupBy(
        getLocation(ctx),
        isDistinct(ctx.setQuantifier()),
        visit(ctx.groupingElement(), GroupingElement.class));
  }

  @Override
  public Node visitSingleGroupingSet(RelationalSqlParser.SingleGroupingSetContext ctx) {
    return new SimpleGroupBy(
        getLocation(ctx), visit(ctx.groupingSet().expression(), Expression.class));
  }

  @Override
  public Node visitRollup(RelationalSqlParser.RollupContext ctx) {
    return new GroupingSets(
        getLocation(ctx),
        ROLLUP,
        ctx.groupingSet().stream()
            .map(groupingSet -> visit(groupingSet.expression(), Expression.class))
            .collect(toList()));
  }

  @Override
  public Node visitCube(RelationalSqlParser.CubeContext ctx) {
    return new GroupingSets(
        getLocation(ctx),
        CUBE,
        ctx.groupingSet().stream()
            .map(groupingSet -> visit(groupingSet.expression(), Expression.class))
            .collect(toList()));
  }

  @Override
  public Node visitMultipleGroupingSets(RelationalSqlParser.MultipleGroupingSetsContext ctx) {
    return new GroupingSets(
        getLocation(ctx),
        EXPLICIT,
        ctx.groupingSet().stream()
            .map(groupingSet -> visit(groupingSet.expression(), Expression.class))
            .collect(toList()));
  }

  @Override
  public Node visitSetOperation(RelationalSqlParser.SetOperationContext ctx) {
    QueryBody left = (QueryBody) visit(ctx.left);
    QueryBody right = (QueryBody) visit(ctx.right);

    boolean distinct = ctx.setQuantifier() == null || ctx.setQuantifier().DISTINCT() != null;

    switch (ctx.operator.getType()) {
      case RelationalSqlLexer.UNION:
        return new Union(getLocation(ctx.UNION()), ImmutableList.of(left, right), distinct);
      case RelationalSqlLexer.INTERSECT:
        return new Intersect(getLocation(ctx.INTERSECT()), ImmutableList.of(left, right), distinct);
      case RelationalSqlLexer.EXCEPT:
        return new Except(getLocation(ctx.EXCEPT()), left, right, distinct);
      default:
        throw new IllegalArgumentException("Unsupported set operation: " + ctx.operator.getText());
    }
  }

  @Override
  public Node visitProperty(RelationalSqlParser.PropertyContext ctx) {
    NodeLocation location = getLocation(ctx);
    Identifier name = (Identifier) visit(ctx.identifier());
    RelationalSqlParser.PropertyValueContext valueContext = ctx.propertyValue();
    if (valueContext instanceof RelationalSqlParser.DefaultPropertyValueContext) {
      return new Property(location, name);
    }
    Expression value =
        (Expression)
            visit(((RelationalSqlParser.NonDefaultPropertyValueContext) valueContext).expression());
    return new Property(location, name, value);
  }

  @Override
  public Node visitTable(RelationalSqlParser.TableContext ctx) {
    return new Table(getLocation(ctx), getQualifiedName(ctx.qualifiedName()));
  }

  @Override
  public Node visitInlineTable(RelationalSqlParser.InlineTableContext ctx) {
    return new Values(getLocation(ctx), visit(ctx.expression(), Expression.class));
  }

  @Override
  public Node visitSubquery(RelationalSqlParser.SubqueryContext ctx) {
    return new TableSubquery(getLocation(ctx), (Query) visit(ctx.queryNoWith()));
  }

  @Override
  public Node visitSortItem(RelationalSqlParser.SortItemContext ctx) {
    return new SortItem(
        getLocation(ctx),
        (Expression) visit(ctx.expression()),
        Optional.ofNullable(ctx.ordering)
            .map(AstBuilder::getOrderingType)
            .orElse(SortItem.Ordering.ASCENDING),
        Optional.ofNullable(ctx.nullOrdering)
            .map(AstBuilder::getNullOrderingType)
            .orElse(SortItem.NullOrdering.UNDEFINED));
  }

  @Override
  public Node visitUnquotedIdentifier(RelationalSqlParser.UnquotedIdentifierContext ctx) {
    return new Identifier(getLocation(ctx), ctx.getText(), false);
  }

  @Override
  public Node visitQuotedIdentifier(RelationalSqlParser.QuotedIdentifierContext ctx) {
    String token = ctx.getText();
    String identifier = token.substring(1, token.length() - 1).replace("\"\"", "\"");

    return new Identifier(getLocation(ctx), identifier, true);
  }

  @Override
  public Node visitTimenGrouping(RelationalSqlParser.TimenGroupingContext ctx) {
    return super.visitTimenGrouping(ctx);
  }

  @Override
  public Node visitVariationGrouping(RelationalSqlParser.VariationGroupingContext ctx) {
    return super.visitVariationGrouping(ctx);
  }

  @Override
  public Node visitConditionGrouping(RelationalSqlParser.ConditionGroupingContext ctx) {
    return super.visitConditionGrouping(ctx);
  }

  @Override
  public Node visitSessionGrouping(RelationalSqlParser.SessionGroupingContext ctx) {
    return super.visitSessionGrouping(ctx);
  }

  @Override
  public Node visitCountGrouping(RelationalSqlParser.CountGroupingContext ctx) {
    return super.visitCountGrouping(ctx);
  }

  @Override
  public Node visitLeftClosedRightOpen(RelationalSqlParser.LeftClosedRightOpenContext ctx) {
    return super.visitLeftClosedRightOpen(ctx);
  }

  @Override
  public Node visitLeftOpenRightClosed(RelationalSqlParser.LeftOpenRightClosedContext ctx) {
    return super.visitLeftOpenRightClosed(ctx);
  }

  @Override
  public Node visitTimeValue(RelationalSqlParser.TimeValueContext ctx) {
    return super.visitTimeValue(ctx);
  }

  @Override
  public Node visitDateExpression(RelationalSqlParser.DateExpressionContext ctx) {
    return super.visitDateExpression(ctx);
  }

  @Override
  public Node visitDatetimeLiteral(RelationalSqlParser.DatetimeLiteralContext ctx) {
    return super.visitDatetimeLiteral(ctx);
  }

  @Override
  public Node visitKeepExpression(RelationalSqlParser.KeepExpressionContext ctx) {
    return super.visitKeepExpression(ctx);
  }

  // ***************** boolean expressions ******************
  @Override
  public Node visitLogicalNot(RelationalSqlParser.LogicalNotContext ctx) {
    return new NotExpression(getLocation(ctx), (Expression) visit(ctx.booleanExpression()));
  }

  @Override
  public Node visitOr(RelationalSqlParser.OrContext ctx) {
    List<ParserRuleContext> terms =
        flatten(
            ctx,
            element -> {
              if (element instanceof RelationalSqlParser.OrContext) {
                RelationalSqlParser.OrContext or = (RelationalSqlParser.OrContext) element;
                return Optional.of(or.booleanExpression());
              }

              return Optional.empty();
            });

    return new LogicalExpression(
        getLocation(ctx), LogicalExpression.Operator.OR, visit(terms, Expression.class));
  }

  @Override
  public Node visitAnd(RelationalSqlParser.AndContext ctx) {
    List<ParserRuleContext> terms =
        flatten(
            ctx,
            element -> {
              if (element instanceof RelationalSqlParser.AndContext) {
                RelationalSqlParser.AndContext and = (RelationalSqlParser.AndContext) element;
                return Optional.of(and.booleanExpression());
              }

              return Optional.empty();
            });

    return new LogicalExpression(
        getLocation(ctx), LogicalExpression.Operator.AND, visit(terms, Expression.class));
  }

  private static List<ParserRuleContext> flatten(
      ParserRuleContext root,
      Function<ParserRuleContext, Optional<List<? extends ParserRuleContext>>> extractChildren) {
    List<ParserRuleContext> result = new ArrayList<>();
    Deque<ParserRuleContext> pending = new ArrayDeque<>();
    pending.push(root);

    while (!pending.isEmpty()) {
      ParserRuleContext next = pending.pop();

      Optional<List<? extends ParserRuleContext>> children = extractChildren.apply(next);
      if (!children.isPresent()) {
        result.add(next);
      } else {
        for (int i = children.get().size() - 1; i >= 0; i--) {
          pending.push(children.get().get(i));
        }
      }
    }

    return result;
  }

  // *************** from clause *****************
  @Override
  public Node visitJoinRelation(RelationalSqlParser.JoinRelationContext ctx) {
    Relation left = (Relation) visit(ctx.left);
    Relation right;

    if (ctx.CROSS() != null) {
      right = (Relation) visit(ctx.right);
      return new Join(getLocation(ctx), Join.Type.CROSS, left, right);
    }

    JoinCriteria criteria;
    if (ctx.NATURAL() != null) {
      right = (Relation) visit(ctx.right);
      criteria = new NaturalJoin();
    } else {
      right = (Relation) visit(ctx.rightRelation);
      if (ctx.joinCriteria().ON() != null) {
        criteria = new JoinOn((Expression) visit(ctx.joinCriteria().booleanExpression()));
      } else if (ctx.joinCriteria().USING() != null) {
        criteria = new JoinUsing(visit(ctx.joinCriteria().identifier(), Identifier.class));
      } else {
        throw new IllegalArgumentException("Unsupported join criteria");
      }
    }

    Join.Type joinType;
    if (ctx.joinType().LEFT() != null) {
      joinType = Join.Type.LEFT;
    } else if (ctx.joinType().RIGHT() != null) {
      joinType = Join.Type.RIGHT;
    } else if (ctx.joinType().FULL() != null) {
      joinType = Join.Type.FULL;
    } else {
      joinType = Join.Type.INNER;
    }

    return new Join(getLocation(ctx), joinType, left, right, criteria);
  }

  @Override
  public Node visitAliasedRelation(RelationalSqlParser.AliasedRelationContext ctx) {
    Relation child = (Relation) visit(ctx.relationPrimary());

    if (ctx.identifier() == null) {
      return child;
    }

    List<Identifier> aliases = null;
    if (ctx.columnAliases() != null) {
      aliases = visit(ctx.columnAliases().identifier(), Identifier.class);
    }

    return new AliasedRelation(
        getLocation(ctx), child, (Identifier) visit(ctx.identifier()), aliases);
  }

  @Override
  public Node visitTableName(RelationalSqlParser.TableNameContext ctx) {
    return new Table(getLocation(ctx), getQualifiedName(ctx.qualifiedName()));
  }

  @Override
  public Node visitSubqueryRelation(RelationalSqlParser.SubqueryRelationContext ctx) {
    return new TableSubquery(getLocation(ctx), (Query) visit(ctx.query()));
  }

  @Override
  public Node visitParenthesizedRelation(RelationalSqlParser.ParenthesizedRelationContext ctx) {
    return visit(ctx.relation());
  }

  // ********************* predicates *******************

  @Override
  public Node visitPredicated(RelationalSqlParser.PredicatedContext ctx) {
    if (ctx.predicate() != null) {
      return visit(ctx.predicate());
    }

    return visit(ctx.valueExpression);
  }

  @Override
  public Node visitComparison(RelationalSqlParser.ComparisonContext ctx) {
    return new ComparisonExpression(
        getLocation(ctx.comparisonOperator()),
        getComparisonOperator(((TerminalNode) ctx.comparisonOperator().getChild(0)).getSymbol()),
        (Expression) visit(ctx.value),
        (Expression) visit(ctx.right));
  }

  @Override
  public Node visitQuantifiedComparison(RelationalSqlParser.QuantifiedComparisonContext ctx) {
    return new QuantifiedComparisonExpression(
        getLocation(ctx.comparisonOperator()),
        getComparisonOperator(((TerminalNode) ctx.comparisonOperator().getChild(0)).getSymbol()),
        getComparisonQuantifier(
            ((TerminalNode) ctx.comparisonQuantifier().getChild(0)).getSymbol()),
        (Expression) visit(ctx.value),
        new SubqueryExpression(getLocation(ctx.query()), (Query) visit(ctx.query())));
  }

  @Override
  public Node visitBetween(RelationalSqlParser.BetweenContext ctx) {
    Expression expression =
        new BetweenPredicate(
            getLocation(ctx),
            (Expression) visit(ctx.value),
            (Expression) visit(ctx.lower),
            (Expression) visit(ctx.upper));

    if (ctx.NOT() != null) {
      expression = new NotExpression(getLocation(ctx), expression);
    }

    return expression;
  }

  @Override
  public Node visitInList(RelationalSqlParser.InListContext ctx) {
    Expression result =
        new InPredicate(
            getLocation(ctx),
            (Expression) visit(ctx.value),
            new InListExpression(getLocation(ctx), visit(ctx.expression(), Expression.class)));

    if (ctx.NOT() != null) {
      result = new NotExpression(getLocation(ctx), result);
    }

    return result;
  }

  @Override
  public Node visitInSubquery(RelationalSqlParser.InSubqueryContext ctx) {
    Expression result =
        new InPredicate(
            getLocation(ctx),
            (Expression) visit(ctx.value),
            new SubqueryExpression(getLocation(ctx), (Query) visit(ctx.query())));

    if (ctx.NOT() != null) {
      result = new NotExpression(getLocation(ctx), result);
    }

    return result;
  }

  @Override
  public Node visitLike(RelationalSqlParser.LikeContext ctx) {
    Expression result;
    if (ctx.escape != null) {
      result =
          new LikePredicate(
              getLocation(ctx),
              (Expression) visit(ctx.value),
              (Expression) visit(ctx.pattern),
              (Expression) visit(ctx.escape));
    } else {
      result =
          new LikePredicate(
              getLocation(ctx), (Expression) visit(ctx.value), (Expression) visit(ctx.pattern));
    }

    if (ctx.NOT() != null) {
      result = new NotExpression(getLocation(ctx), result);
    }

    return result;
  }

  @Override
  public Node visitNullPredicate(RelationalSqlParser.NullPredicateContext ctx) {
    Expression child = (Expression) visit(ctx.value);

    if (ctx.NOT() == null) {
      return new IsNullPredicate(getLocation(ctx), child);
    }

    return new IsNotNullPredicate(getLocation(ctx), child);
  }

  @Override
  public Node visitDistinctFrom(RelationalSqlParser.DistinctFromContext ctx) {
    Expression expression =
        new ComparisonExpression(
            getLocation(ctx),
            ComparisonExpression.Operator.IS_DISTINCT_FROM,
            (Expression) visit(ctx.value),
            (Expression) visit(ctx.right));

    if (ctx.NOT() != null) {
      expression = new NotExpression(getLocation(ctx), expression);
    }

    return expression;
  }

  @Override
  public Node visitExists(RelationalSqlParser.ExistsContext ctx) {
    return new ExistsPredicate(
        getLocation(ctx), new SubqueryExpression(getLocation(ctx), (Query) visit(ctx.query())));
  }

  // ************** value expressions **************
  @Override
  public Node visitArithmeticUnary(RelationalSqlParser.ArithmeticUnaryContext ctx) {
    Expression child = (Expression) visit(ctx.valueExpression());

    switch (ctx.operator.getType()) {
      case RelationalSqlLexer.MINUS:
        return ArithmeticUnaryExpression.negative(getLocation(ctx), child);
      case RelationalSqlLexer.PLUS:
        return ArithmeticUnaryExpression.positive(getLocation(ctx), child);
      default:
        throw new UnsupportedOperationException("Unsupported sign: " + ctx.operator.getText());
    }
  }

  @Override
  public Node visitArithmeticBinary(RelationalSqlParser.ArithmeticBinaryContext ctx) {
    return new ArithmeticBinaryExpression(
        getLocation(ctx.operator),
        getArithmeticBinaryOperator(ctx.operator),
        (Expression) visit(ctx.left),
        (Expression) visit(ctx.right));
  }

  @Override
  public Node visitConcatenation(RelationalSqlParser.ConcatenationContext ctx) {
    return new FunctionCall(
        getLocation(ctx.CONCAT()),
        QualifiedName.of("concat"),
        ImmutableList.of((Expression) visit(ctx.left), (Expression) visit(ctx.right)));
  }

  // ********************* primary expressions **********************
  @Override
  public Node visitParenthesizedExpression(RelationalSqlParser.ParenthesizedExpressionContext ctx) {
    return visit(ctx.expression());
  }

  @Override
  public Node visitRowConstructor(RelationalSqlParser.RowConstructorContext context) {
    return new Row(getLocation(context), visit(context.expression(), Expression.class));
  }

  @Override
  public Node visitCast(RelationalSqlParser.CastContext ctx) {
    return new Cast(
        getLocation(ctx), (Expression) visit(ctx.expression()), (DataType) visit(ctx.type()));
  }

  @Override
  public Node visitSpecialDateTimeFunction(RelationalSqlParser.SpecialDateTimeFunctionContext ctx) {
    CurrentTime.Function function = getDateTimeFunctionType(ctx.name);
    return new CurrentTime(getLocation(ctx), function);
  }

  @Override
  public Node visitTrim(RelationalSqlParser.TrimContext ctx) {
    if (ctx.FROM() != null && ctx.trimsSpecification() == null && ctx.trimChar == null) {
      throw parseError(
          "The 'trim' function must have specification, char or both arguments when it takes FROM",
          ctx);
    }

    Trim.Specification specification =
        ctx.trimsSpecification() == null
            ? Trim.Specification.BOTH
            : toTrimSpecification((Token) ctx.trimsSpecification().getChild(0).getPayload());
    if (ctx.trimChar != null) {
      return new Trim(
          getLocation(ctx),
          specification,
          (Expression) visit(ctx.trimSource),
          (Expression) visit(ctx.trimChar));
    } else {
      return new Trim(getLocation(ctx), specification, (Expression) visit(ctx.trimSource));
    }
  }

  private static Trim.Specification toTrimSpecification(Token token) {
    switch (token.getType()) {
      case RelationalSqlLexer.BOTH:
        return Trim.Specification.BOTH;
      case RelationalSqlLexer.LEADING:
        return Trim.Specification.LEADING;
      case RelationalSqlLexer.TRAILING:
        return Trim.Specification.TRAILING;
      default:
        throw new IllegalArgumentException("Unsupported trim specification: " + token.getText());
    }
  }

  @Override
  public Node visitSubstring(RelationalSqlParser.SubstringContext ctx) {
    return new FunctionCall(
        getLocation(ctx),
        QualifiedName.of("substr"),
        visit(ctx.valueExpression(), Expression.class));
  }

  @Override
  public Node visitCurrentDatabase(RelationalSqlParser.CurrentDatabaseContext ctx) {
    return new CurrentDatabase(getLocation(ctx));
  }

  @Override
  public Node visitCurrentUser(RelationalSqlParser.CurrentUserContext ctx) {
    return new CurrentUser(getLocation(ctx));
  }

  @Override
  public Node visitSubqueryExpression(RelationalSqlParser.SubqueryExpressionContext ctx) {
    return new SubqueryExpression(getLocation(ctx), (Query) visit(ctx.query()));
  }

  @Override
  public Node visitDereference(RelationalSqlParser.DereferenceContext ctx) {
    return new DereferenceExpression(
        getLocation(ctx), (Expression) visit(ctx.base), (Identifier) visit(ctx.fieldName));
  }

  @Override
  public Node visitColumnReference(RelationalSqlParser.ColumnReferenceContext ctx) {
    return visit(ctx.identifier());
  }

  @Override
  public Node visitSimpleCase(RelationalSqlParser.SimpleCaseContext ctx) {
    if (ctx.elseExpression != null) {
      return new SimpleCaseExpression(
          getLocation(ctx),
          (Expression) visit(ctx.operand),
          visit(ctx.whenClause(), WhenClause.class),
          (Expression) visit(ctx.elseExpression));
    } else {
      return new SimpleCaseExpression(
          getLocation(ctx),
          (Expression) visit(ctx.operand),
          visit(ctx.whenClause(), WhenClause.class));
    }
  }

  @Override
  public Node visitSearchedCase(RelationalSqlParser.SearchedCaseContext ctx) {
    if (ctx.elseExpression != null) {
      return new SearchedCaseExpression(
          getLocation(ctx),
          visit(ctx.whenClause(), WhenClause.class),
          (Expression) visit(ctx.elseExpression));
    } else {
      return new SearchedCaseExpression(
          getLocation(ctx), visit(ctx.whenClause(), WhenClause.class));
    }
  }

  @Override
  public Node visitWhenClause(RelationalSqlParser.WhenClauseContext ctx) {
    return new WhenClause(
        getLocation(ctx), (Expression) visit(ctx.condition), (Expression) visit(ctx.result));
  }

  @Override
  public Node visitFunctionCall(RelationalSqlParser.FunctionCallContext ctx) {

    QualifiedName name = getQualifiedName(ctx.qualifiedName());

    boolean distinct = isDistinct(ctx.setQuantifier());

    if (name.toString().equalsIgnoreCase("if")) {
      check(
          ctx.expression().size() == 2 || ctx.expression().size() == 3,
          "Invalid number of arguments for 'if' function",
          ctx);
      check(!distinct, "DISTINCT not valid for 'if' function", ctx);

      Expression elseExpression = null;
      if (ctx.expression().size() == 3) {
        elseExpression = (Expression) visit(ctx.expression(2));
      }

      return new IfExpression(
          getLocation(ctx),
          (Expression) visit(ctx.expression(0)),
          (Expression) visit(ctx.expression(1)),
          elseExpression);
    }

    if (name.toString().equalsIgnoreCase("nullif")) {
      check(ctx.expression().size() == 2, "Invalid number of arguments for 'nullif' function", ctx);
      check(!distinct, "DISTINCT not valid for 'nullif' function", ctx);

      return new NullIfExpression(
          getLocation(ctx),
          (Expression) visit(ctx.expression(0)),
          (Expression) visit(ctx.expression(1)));
    }

    if (name.toString().equalsIgnoreCase("coalesce")) {
      check(
          ctx.expression().size() >= 2,
          "The 'coalesce' function must have at least two arguments",
          ctx);
      check(!distinct, "DISTINCT not valid for 'coalesce' function", ctx);

      return new CoalesceExpression(getLocation(ctx), visit(ctx.expression(), Expression.class));
    }

    List<Expression> arguments = visit(ctx.expression(), Expression.class);
    if (ctx.label != null) {
      arguments =
          ImmutableList.of(
              new DereferenceExpression(getLocation(ctx.label), (Identifier) visit(ctx.label)));
    }

    return new FunctionCall(getLocation(ctx), name, distinct, arguments);
  }

  // ************** literals **************

  @Override
  public Node visitNullLiteral(RelationalSqlParser.NullLiteralContext ctx) {
    return new NullLiteral(getLocation(ctx));
  }

  @Override
  public Node visitBasicStringLiteral(RelationalSqlParser.BasicStringLiteralContext ctx) {
    return new StringLiteral(getLocation(ctx), unquote(ctx.STRING().getText()));
  }

  @Override
  public Node visitUnicodeStringLiteral(RelationalSqlParser.UnicodeStringLiteralContext ctx) {
    return new StringLiteral(getLocation(ctx), decodeUnicodeLiteral(ctx));
  }

  @Override
  public Node visitBinaryLiteral(RelationalSqlParser.BinaryLiteralContext ctx) {
    String raw = ctx.BINARY_LITERAL().getText();
    return new BinaryLiteral(getLocation(ctx), unquote(raw.substring(1)));
  }

  @Override
  public Node visitDecimalLiteral(RelationalSqlParser.DecimalLiteralContext ctx) {
    return new DoubleLiteral(getLocation(ctx), ctx.getText());
  }

  @Override
  public Node visitDoubleLiteral(RelationalSqlParser.DoubleLiteralContext ctx) {
    return new DoubleLiteral(getLocation(ctx), ctx.getText());
  }

  @Override
  public Node visitIntegerLiteral(RelationalSqlParser.IntegerLiteralContext ctx) {
    return new LongLiteral(getLocation(ctx), ctx.getText());
  }

  @Override
  public Node visitBooleanLiteral(RelationalSqlParser.BooleanLiteralContext ctx) {
    return new BooleanLiteral(getLocation(ctx), ctx.getText());
  }

  @Override
  public Node visitParameter(RelationalSqlParser.ParameterContext ctx) {
    Parameter parameter = new Parameter(getLocation(ctx), parameterPosition);
    parameterPosition++;
    return parameter;
  }

  @Override
  public Node visitIdentifierOrString(RelationalSqlParser.IdentifierOrStringContext ctx) {
    String s = null;
    if (ctx.identifier() != null) {
      return visit(ctx.identifier());
    } else if (ctx.string() != null) {
      s = ((StringLiteral) visit(ctx.string())).getValue();
    }

    return new Identifier(getLocation(ctx), s);
  }

  @Override
  public Node visitIntervalField(RelationalSqlParser.IntervalFieldContext ctx) {
    return super.visitIntervalField(ctx);
  }

  @Override
  public Node visitTimeDuration(RelationalSqlParser.TimeDurationContext ctx) {
    return super.visitTimeDuration(ctx);
  }

  // ***************** arguments *****************
  @Override
  public Node visitGenericType(RelationalSqlParser.GenericTypeContext ctx) {
    List<DataTypeParameter> parameters =
        ctx.typeParameter().stream()
            .map(this::visit)
            .map(DataTypeParameter.class::cast)
            .collect(toImmutableList());

    return new GenericDataType(getLocation(ctx), (Identifier) visit(ctx.identifier()), parameters);
  }

  @Override
  public Node visitTypeParameter(RelationalSqlParser.TypeParameterContext ctx) {
    if (ctx.INTEGER_VALUE() != null) {
      return new NumericParameter(getLocation(ctx), ctx.getText());
    }

    return new TypeParameter((DataType) visit(ctx.type()));
  }

  // ***************** helpers *****************

  private enum UnicodeDecodeState {
    EMPTY,
    ESCAPED,
    UNICODE_SEQUENCE
  }

  private static String decodeUnicodeLiteral(
      RelationalSqlParser.UnicodeStringLiteralContext context) {
    char escape;
    if (context.UESCAPE() != null) {
      String escapeString = unquote(context.STRING().getText());
      check(!escapeString.isEmpty(), "Empty Unicode escape character", context);
      check(
          escapeString.length() == 1, "Invalid Unicode escape character: " + escapeString, context);
      escape = escapeString.charAt(0);
      check(
          isValidUnicodeEscape(escape),
          "Invalid Unicode escape character: " + escapeString,
          context);
    } else {
      escape = '\\';
    }

    String rawContent = unquote(context.UNICODE_STRING().getText().substring(2));
    StringBuilder unicodeStringBuilder = new StringBuilder();
    StringBuilder escapedCharacterBuilder = new StringBuilder();
    int charactersNeeded = 0;
    UnicodeDecodeState state = UnicodeDecodeState.EMPTY;
    for (int i = 0; i < rawContent.length(); i++) {
      char ch = rawContent.charAt(i);
      switch (state) {
        case EMPTY:
          if (ch == escape) {
            state = UnicodeDecodeState.ESCAPED;
          } else {
            unicodeStringBuilder.append(ch);
          }
          break;
        case ESCAPED:
          if (ch == escape) {
            unicodeStringBuilder.append(escape);
            state = UnicodeDecodeState.EMPTY;
          } else if (ch == '+') {
            state = UnicodeDecodeState.UNICODE_SEQUENCE;
            charactersNeeded = 6;
          } else if (isHexDigit(ch)) {
            state = UnicodeDecodeState.UNICODE_SEQUENCE;
            charactersNeeded = 4;
            escapedCharacterBuilder.append(ch);
          } else {
            throw parseError("Invalid hexadecimal digit: " + ch, context);
          }
          break;
        case UNICODE_SEQUENCE:
          check(isHexDigit(ch), "Incomplete escape sequence: " + escapedCharacterBuilder, context);
          escapedCharacterBuilder.append(ch);
          if (charactersNeeded == escapedCharacterBuilder.length()) {
            String currentEscapedCode = escapedCharacterBuilder.toString();
            escapedCharacterBuilder.setLength(0);
            int codePoint = Integer.parseInt(currentEscapedCode, 16);
            check(
                Character.isValidCodePoint(codePoint),
                "Invalid escaped character: " + currentEscapedCode,
                context);
            if (Character.isSupplementaryCodePoint(codePoint)) {
              unicodeStringBuilder.appendCodePoint(codePoint);
            } else {
              char currentCodePoint = (char) codePoint;
              if (Character.isSurrogate(currentCodePoint)) {
                throw parseError(
                    String.format(
                        "Invalid escaped character: %s. Escaped character is a surrogate. Use '\\+123456' instead.",
                        currentEscapedCode),
                    context);
              }
              unicodeStringBuilder.append(currentCodePoint);
            }
            state = UnicodeDecodeState.EMPTY;
            charactersNeeded = -1;
          } else {
            check(
                charactersNeeded > escapedCharacterBuilder.length(),
                "Unexpected escape sequence length: " + escapedCharacterBuilder.length(),
                context);
          }
          break;
        default:
          throw new UnsupportedOperationException();
      }
    }

    check(
        state == UnicodeDecodeState.EMPTY,
        "Incomplete escape sequence: " + escapedCharacterBuilder.toString(),
        context);
    return unicodeStringBuilder.toString();
  }

  private <T> Optional<T> visitIfPresent(ParserRuleContext context, Class<T> clazz) {
    return Optional.ofNullable(context).map(this::visit).map(clazz::cast);
  }

  private <T> List<T> visit(List<? extends ParserRuleContext> contexts, Class<T> clazz) {
    return contexts.stream().map(this::visit).map(clazz::cast).collect(toList());
  }

  private static String unquote(String value) {
    return value.substring(1, value.length() - 1).replace("''", "'");
  }

  private QualifiedName getQualifiedName(RelationalSqlParser.QualifiedNameContext context) {
    return QualifiedName.of(visit(context.identifier(), Identifier.class));
  }

  private static boolean isDistinct(RelationalSqlParser.SetQuantifierContext setQuantifier) {
    return setQuantifier != null && setQuantifier.DISTINCT() != null;
  }

  private static boolean isHexDigit(char c) {
    return ((c >= '0') && (c <= '9')) || ((c >= 'A') && (c <= 'F')) || ((c >= 'a') && (c <= 'f'));
  }

  private static boolean isValidUnicodeEscape(char c) {
    return c < 0x7F && c > 0x20 && !isHexDigit(c) && c != '"' && c != '+' && c != '\'';
  }

  private static Optional<String> getTextIfPresent(ParserRuleContext context) {
    return Optional.ofNullable(context).map(ParseTree::getText);
  }

  private Optional<Identifier> getIdentifierIfPresent(ParserRuleContext context) {
    return Optional.ofNullable(context).map(c -> (Identifier) visit(c));
  }

  private static ColumnDefinition.ColumnCategory getColumnCategory(Token category) {
    if (category == null) {
      return ColumnDefinition.ColumnCategory.MEASUREMENT;
    }
    switch (category.getType()) {
      case RelationalSqlLexer.ID:
        return ColumnDefinition.ColumnCategory.ID;
      case RelationalSqlLexer.ATTRIBUTE:
        return ColumnDefinition.ColumnCategory.ATTRIBUTE;
      case RelationalSqlLexer.TIME:
        return ColumnDefinition.ColumnCategory.TIME;
      case RelationalSqlLexer.MEASUREMENT:
        return ColumnDefinition.ColumnCategory.MEASUREMENT;
      default:
        throw new UnsupportedOperationException(
            "Unsupported ColumnCategory: " + category.getText());
    }
  }

  private static ArithmeticBinaryExpression.Operator getArithmeticBinaryOperator(Token operator) {
    switch (operator.getType()) {
      case RelationalSqlLexer.PLUS:
        return ArithmeticBinaryExpression.Operator.ADD;
      case RelationalSqlLexer.MINUS:
        return ArithmeticBinaryExpression.Operator.SUBTRACT;
      case RelationalSqlLexer.ASTERISK:
        return ArithmeticBinaryExpression.Operator.MULTIPLY;
      case RelationalSqlLexer.SLASH:
        return ArithmeticBinaryExpression.Operator.DIVIDE;
      case RelationalSqlLexer.PERCENT:
        return ArithmeticBinaryExpression.Operator.MODULUS;
      default:
        throw new UnsupportedOperationException("Unsupported operator: " + operator.getText());
    }
  }

  private static ComparisonExpression.Operator getComparisonOperator(Token symbol) {
    switch (symbol.getType()) {
      case RelationalSqlLexer.EQ:
        return ComparisonExpression.Operator.EQUAL;
      case RelationalSqlLexer.NEQ:
        return ComparisonExpression.Operator.NOT_EQUAL;
      case RelationalSqlLexer.LT:
        return ComparisonExpression.Operator.LESS_THAN;
      case RelationalSqlLexer.LTE:
        return ComparisonExpression.Operator.LESS_THAN_OR_EQUAL;
      case RelationalSqlLexer.GT:
        return ComparisonExpression.Operator.GREATER_THAN;
      case RelationalSqlLexer.GTE:
        return ComparisonExpression.Operator.GREATER_THAN_OR_EQUAL;
      default:
        throw new IllegalArgumentException("Unsupported operator: " + symbol.getText());
    }
  }

  private static CurrentTime.Function getDateTimeFunctionType(Token token) {
    switch (token.getType()) {
      case RelationalSqlLexer.CURRENT_DATE:
        return CurrentTime.Function.DATE;
      case RelationalSqlLexer.CURRENT_TIME:
        return CurrentTime.Function.TIME;
      case RelationalSqlLexer.CURRENT_TIMESTAMP:
      case RelationalSqlLexer.NOW:
        return CurrentTime.Function.TIMESTAMP;
      case RelationalSqlLexer.LOCALTIME:
        return CurrentTime.Function.LOCALTIME;
      case RelationalSqlLexer.LOCALTIMESTAMP:
        return CurrentTime.Function.LOCALTIMESTAMP;
      default:
        throw new IllegalArgumentException("Unsupported special function: " + token.getText());
    }
  }

  private static SortItem.NullOrdering getNullOrderingType(Token token) {
    switch (token.getType()) {
      case RelationalSqlLexer.FIRST:
        return SortItem.NullOrdering.FIRST;
      case RelationalSqlLexer.LAST:
        return SortItem.NullOrdering.LAST;
      default:
        throw new IllegalArgumentException("Unsupported ordering: " + token.getText());
    }
  }

  private static SortItem.Ordering getOrderingType(Token token) {
    switch (token.getType()) {
      case RelationalSqlLexer.ASC:
        return SortItem.Ordering.ASCENDING;
      case RelationalSqlLexer.DESC:
        return SortItem.Ordering.DESCENDING;
      default:
        throw new IllegalArgumentException("Unsupported ordering: " + token.getText());
    }
  }

  private static QuantifiedComparisonExpression.Quantifier getComparisonQuantifier(Token symbol) {
    switch (symbol.getType()) {
      case RelationalSqlLexer.ALL:
        return QuantifiedComparisonExpression.Quantifier.ALL;
      case RelationalSqlLexer.ANY:
        return QuantifiedComparisonExpression.Quantifier.ANY;
      case RelationalSqlLexer.SOME:
        return QuantifiedComparisonExpression.Quantifier.SOME;
      default:
        throw new IllegalArgumentException("Unsupported quantifier: " + symbol.getText());
    }
  }

  private List<Identifier> getIdentifiers(List<RelationalSqlParser.IdentifierContext> identifiers) {
    return identifiers.stream().map(context -> (Identifier) visit(context)).collect(toList());
  }

  private static void check(boolean condition, String message, ParserRuleContext context) {
    if (!condition) {
      throw parseError(message, context);
    }
  }

  private NodeLocation getLocation(TerminalNode terminalNode) {
    requireNonNull(terminalNode, "terminalNode is null");
    return getLocation(terminalNode.getSymbol());
  }

  private NodeLocation getLocation(ParserRuleContext parserRuleContext) {
    requireNonNull(parserRuleContext, "parserRuleContext is null");
    return getLocation(parserRuleContext.getStart());
  }

  private NodeLocation getLocation(Token token) {
    requireNonNull(token, "token is null");
    return baseLocation != null
        ? new NodeLocation(
            token.getLine() + baseLocation.getLineNumber() - 1,
            token.getCharPositionInLine()
                + 1
                + (token.getLine() == 1 ? baseLocation.getColumnNumber() : 0))
        : new NodeLocation(token.getLine(), token.getCharPositionInLine() + 1);
  }

  private static ParsingException parseError(String message, ParserRuleContext context) {
    return new ParsingException(
        message,
        null,
        context.getStart().getLine(),
        context.getStart().getCharPositionInLine() + 1);
  }

  private static void validateArgumentAlias(Identifier alias, ParserRuleContext context) {
    check(
        alias.isDelimited() || !alias.getValue().equalsIgnoreCase("COPARTITION"),
        "The word \"COPARTITION\" is ambiguous in this context. "
            + "To alias an argument, precede the alias with \"AS\". "
            + "To specify co-partitioning, change the argument order so that the last argument cannot be aliased.",
        context);
  }
}
