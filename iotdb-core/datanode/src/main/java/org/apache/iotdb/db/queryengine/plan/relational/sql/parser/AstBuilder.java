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

package org.apache.iotdb.db.queryengine.plan.relational.sql.parser;

import org.apache.iotdb.common.rpc.thrift.TConsensusGroupType;
import org.apache.iotdb.commons.exception.IllegalPathException;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.commons.schema.cache.CacheClearOptions;
import org.apache.iotdb.commons.schema.table.TsTable;
import org.apache.iotdb.commons.schema.table.column.TsTableColumnCategory;
import org.apache.iotdb.commons.schema.table.column.TsTableColumnSchema;
import org.apache.iotdb.commons.udf.builtin.relational.TableBuiltinScalarFunction;
import org.apache.iotdb.commons.utils.CommonDateTimeUtils;
import org.apache.iotdb.commons.utils.PathUtils;
import org.apache.iotdb.db.exception.query.QueryProcessException;
import org.apache.iotdb.db.exception.sql.SemanticException;
import org.apache.iotdb.db.protocol.session.IClientSession;
import org.apache.iotdb.db.queryengine.plan.expression.leaf.TimestampOperand;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.AddColumn;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.AliasedRelation;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.AllColumns;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.AllRows;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.AlterDB;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.AlterPipe;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.ArithmeticBinaryExpression;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.ArithmeticUnaryExpression;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.BetweenPredicate;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.BinaryLiteral;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.BooleanLiteral;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.Cast;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.ClearCache;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.CoalesceExpression;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.ColumnDefinition;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.ComparisonExpression;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.CountDevice;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.CreateDB;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.CreateFunction;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.CreateIndex;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.CreatePipe;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.CreatePipePlugin;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.CreateTable;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.CreateTopic;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.CurrentDatabase;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.CurrentTime;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.CurrentUser;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.DataType;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.DataTypeParameter;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.Delete;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.DeleteDevice;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.DereferenceExpression;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.DescribeTable;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.DoubleLiteral;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.DropColumn;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.DropDB;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.DropFunction;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.DropIndex;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.DropPipe;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.DropPipePlugin;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.DropTable;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.DropTopic;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.Except;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.ExistsPredicate;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.Explain;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.ExplainAnalyze;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.Expression;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.Fill;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.Flush;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.FunctionCall;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.GenericDataType;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.GroupBy;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.GroupingElement;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.GroupingSets;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.Identifier;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.IfExpression;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.InListExpression;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.InPredicate;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.InsertRows;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.Intersect;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.IsNotNullPredicate;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.IsNullPredicate;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.Join;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.JoinCriteria;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.JoinOn;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.JoinUsing;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.KillQuery;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.LikePredicate;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.Limit;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.Literal;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.LoadTsFile;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.LogicalExpression;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.LongLiteral;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.NaturalJoin;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.Node;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.NodeLocation;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.NotExpression;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.NullIfExpression;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.NullLiteral;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.NumericParameter;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.Offset;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.OrderBy;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.Parameter;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.Property;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.QualifiedName;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.QuantifiedComparisonExpression;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.Query;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.QueryBody;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.QuerySpecification;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.Relation;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.RenameColumn;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.RenameTable;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.Row;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.SearchedCaseExpression;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.Select;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.SelectItem;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.SetConfiguration;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.SetProperties;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.ShowAINodes;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.ShowCluster;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.ShowClusterId;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.ShowConfigNodes;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.ShowCurrentDatabase;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.ShowCurrentSqlDialect;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.ShowCurrentTimestamp;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.ShowCurrentUser;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.ShowDB;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.ShowDataNodes;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.ShowDevice;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.ShowFunctions;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.ShowIndex;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.ShowPipePlugins;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.ShowPipes;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.ShowRegions;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.ShowStatement;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.ShowSubscriptions;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.ShowTables;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.ShowTopics;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.ShowVariables;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.ShowVersion;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.SimpleCaseExpression;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.SimpleGroupBy;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.SingleColumn;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.SortItem;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.StartPipe;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.Statement;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.StopPipe;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.StringLiteral;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.SubqueryExpression;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.Table;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.TableExpressionType;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.TableSubquery;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.Trim;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.TypeParameter;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.Union;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.Update;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.UpdateAssignment;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.Use;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.Values;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.WhenClause;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.With;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.WithQuery;
import org.apache.iotdb.db.queryengine.plan.relational.sql.util.AstUtil;
import org.apache.iotdb.db.queryengine.plan.statement.StatementType;
import org.apache.iotdb.db.queryengine.plan.statement.crud.InsertRowStatement;
import org.apache.iotdb.db.queryengine.plan.statement.crud.InsertRowsStatement;
import org.apache.iotdb.db.queryengine.plan.statement.sys.FlushStatement;
import org.apache.iotdb.db.queryengine.plan.statement.sys.SetConfigurationStatement;
import org.apache.iotdb.db.relational.grammar.sql.RelationalSqlBaseVisitor;
import org.apache.iotdb.db.relational.grammar.sql.RelationalSqlLexer;
import org.apache.iotdb.db.relational.grammar.sql.RelationalSqlParser;
import org.apache.iotdb.db.schemaengine.table.DataNodeTableCache;
import org.apache.iotdb.db.storageengine.load.config.LoadTsFileConfigurator;
import org.apache.iotdb.db.utils.DateTimeUtils;
import org.apache.iotdb.db.utils.TimestampPrecisionUtils;

import com.google.common.collect.ImmutableList;
import org.antlr.v4.runtime.ParserRuleContext;
import org.antlr.v4.runtime.Token;
import org.antlr.v4.runtime.tree.ParseTree;
import org.antlr.v4.runtime.tree.TerminalNode;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.exception.write.NoTableException;
import org.apache.tsfile.utils.TimeDuration;
import org.apache.tsfile.write.schema.MeasurementSchema;

import javax.annotation.Nullable;

import java.net.URI;
import java.net.URISyntaxException;
import java.time.ZoneId;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toList;
import static org.apache.iotdb.commons.schema.table.InformationSchemaTable.QUERIES;
import static org.apache.iotdb.commons.schema.table.TsTable.TIME_COLUMN_NAME;
import static org.apache.iotdb.commons.schema.table.column.TsTableColumnCategory.ATTRIBUTE;
import static org.apache.iotdb.commons.schema.table.column.TsTableColumnCategory.ID;
import static org.apache.iotdb.commons.schema.table.column.TsTableColumnCategory.MEASUREMENT;
import static org.apache.iotdb.commons.schema.table.column.TsTableColumnCategory.TIME;
import static org.apache.iotdb.commons.udf.builtin.relational.TableBuiltinScalarFunction.DATE_BIN;
import static org.apache.iotdb.db.queryengine.plan.execution.config.TableConfigTaskVisitor.DATABASE_NOT_SPECIFIED;
import static org.apache.iotdb.db.queryengine.plan.parser.ASTVisitor.parseDateTimeFormat;
import static org.apache.iotdb.db.queryengine.plan.relational.sql.ast.GroupingSets.Type.CUBE;
import static org.apache.iotdb.db.queryengine.plan.relational.sql.ast.GroupingSets.Type.EXPLICIT;
import static org.apache.iotdb.db.queryengine.plan.relational.sql.ast.GroupingSets.Type.ROLLUP;
import static org.apache.iotdb.db.queryengine.plan.relational.sql.ast.QualifiedName.mapIdentifier;
import static org.apache.iotdb.db.utils.constant.SqlConstant.FIRST_AGGREGATION;
import static org.apache.iotdb.db.utils.constant.SqlConstant.FIRST_BY_AGGREGATION;
import static org.apache.iotdb.db.utils.constant.SqlConstant.LAST_AGGREGATION;
import static org.apache.iotdb.db.utils.constant.SqlConstant.LAST_BY_AGGREGATION;

public class AstBuilder extends RelationalSqlBaseVisitor<Node> {

  private int parameterPosition;

  @Nullable private final NodeLocation baseLocation;

  private final ZoneId zoneId;

  private final IClientSession clientSession;

  AstBuilder(@Nullable NodeLocation baseLocation, ZoneId zoneId, IClientSession clientSession) {
    this.baseLocation = baseLocation;
    this.zoneId = zoneId;
    this.clientSession = clientSession;
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
    return new Use(getLocation(ctx), lowerIdentifier((Identifier) visit(ctx.database)));
  }

  private static Identifier lowerIdentifier(Identifier identifier) {
    if (identifier.getLocation().isPresent()) {
      return new Identifier(
          identifier.getLocation().get(), mapIdentifier(identifier), identifier.isDelimited());
    } else {
      return new Identifier(mapIdentifier(identifier), identifier.isDelimited());
    }
  }

  @Override
  public Node visitShowDatabasesStatement(
      final RelationalSqlParser.ShowDatabasesStatementContext ctx) {
    return new ShowDB(getLocation(ctx), Objects.nonNull(ctx.DETAILS()));
  }

  @Override
  public Node visitCreateDbStatement(final RelationalSqlParser.CreateDbStatementContext ctx) {
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
  public Node visitAlterDbStatement(final RelationalSqlParser.AlterDbStatementContext ctx) {
    List<Property> properties = ImmutableList.of();
    if (ctx.propertyAssignments() != null) {
      properties = visit(ctx.propertyAssignments().property(), Property.class);
    }

    return new AlterDB(
        getLocation(ctx),
        ctx.EXISTS() != null,
        ((Identifier) visit(ctx.database)).getValue(),
        properties);
  }

  @Override
  public Node visitDropDbStatement(final RelationalSqlParser.DropDbStatementContext ctx) {
    return new DropDB(
        getLocation(ctx), lowerIdentifier((Identifier) visit(ctx.database)), ctx.EXISTS() != null);
  }

  @Override
  public Node visitCreateTableStatement(final RelationalSqlParser.CreateTableStatementContext ctx) {
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
  public Node visitColumnDefinition(final RelationalSqlParser.ColumnDefinitionContext ctx) {
    return new ColumnDefinition(
        getLocation(ctx),
        lowerIdentifier((Identifier) visit(ctx.identifier())),
        Objects.nonNull(ctx.type()) ? (DataType) visit(ctx.type()) : null,
        getColumnCategory(ctx.columnCategory),
        ctx.charsetName() == null
            ? null
            : ((Identifier) visit(ctx.charsetName().identifier())).getValue());
  }

  @Override
  public Node visitDropTableStatement(final RelationalSqlParser.DropTableStatementContext ctx) {
    return new DropTable(
        getLocation(ctx), getQualifiedName(ctx.qualifiedName()), ctx.EXISTS() != null);
  }

  @Override
  public Node visitShowTableStatement(final RelationalSqlParser.ShowTableStatementContext ctx) {
    return Objects.nonNull(ctx.database)
        ? new ShowTables(
            getLocation(ctx),
            lowerIdentifier((Identifier) visit(ctx.database)),
            Objects.nonNull(ctx.DETAILS()))
        : new ShowTables(getLocation(ctx), Objects.nonNull(ctx.DETAILS()));
  }

  @Override
  public Node visitDescTableStatement(final RelationalSqlParser.DescTableStatementContext ctx) {
    return new DescribeTable(
        getLocation(ctx), getQualifiedName(ctx.table), Objects.nonNull(ctx.DETAILS()));
  }

  @Override
  public Node visitRenameTable(final RelationalSqlParser.RenameTableContext ctx) {
    return new RenameTable(
        getLocation(ctx),
        getQualifiedName(ctx.from),
        lowerIdentifier((Identifier) visit(ctx.to)),
        Objects.nonNull(ctx.EXISTS()));
  }

  @Override
  public Node visitAddColumn(final RelationalSqlParser.AddColumnContext ctx) {
    return new AddColumn(
        getLocation(ctx),
        getQualifiedName(ctx.tableName),
        (ColumnDefinition) visit(ctx.column),
        ctx.EXISTS().size() == (Objects.nonNull(ctx.NOT()) ? 2 : 1),
        Objects.nonNull(ctx.NOT()));
  }

  @Override
  public Node visitRenameColumn(final RelationalSqlParser.RenameColumnContext ctx) {
    return new RenameColumn(
        getLocation(ctx),
        getQualifiedName(ctx.tableName),
        (Identifier) visit(ctx.from),
        (Identifier) visit(ctx.to),
        ctx.EXISTS().stream()
            .anyMatch(
                node ->
                    node.getSymbol().getTokenIndex() < ctx.COLUMN().getSymbol().getTokenIndex()),
        ctx.EXISTS().stream()
            .anyMatch(
                node ->
                    node.getSymbol().getTokenIndex() > ctx.COLUMN().getSymbol().getTokenIndex()));
  }

  @Override
  public Node visitDropColumn(final RelationalSqlParser.DropColumnContext ctx) {
    return new DropColumn(
        getLocation(ctx),
        getQualifiedName(ctx.tableName),
        lowerIdentifier((Identifier) visit(ctx.column)),
        ctx.EXISTS().stream()
            .anyMatch(
                node ->
                    node.getSymbol().getTokenIndex() < ctx.COLUMN().getSymbol().getTokenIndex()),
        ctx.EXISTS().stream()
            .anyMatch(
                node ->
                    node.getSymbol().getTokenIndex() > ctx.COLUMN().getSymbol().getTokenIndex()));
  }

  @Override
  public Node visitSetTableProperties(final RelationalSqlParser.SetTablePropertiesContext ctx) {
    List<Property> properties = ImmutableList.of();
    if (ctx.propertyAssignments() != null) {
      properties = visit(ctx.propertyAssignments().property(), Property.class);
    }
    return new SetProperties(
        getLocation(ctx),
        SetProperties.Type.TABLE,
        getQualifiedName(ctx.qualifiedName()),
        properties,
        Objects.nonNull(ctx.EXISTS()));
  }

  @Override
  public Node visitCreateIndexStatement(RelationalSqlParser.CreateIndexStatementContext ctx) {
    return new CreateIndex(
        getLocation(ctx),
        getQualifiedName(ctx.tableName),
        lowerIdentifier((Identifier) visit(ctx.indexName)),
        visit(ctx.identifierList().identifier(), Identifier.class));
  }

  @Override
  public Node visitDropIndexStatement(RelationalSqlParser.DropIndexStatementContext ctx) {
    return new DropIndex(
        getLocation(ctx),
        getQualifiedName(ctx.tableName),
        lowerIdentifier((Identifier) visit(ctx.indexName)));
  }

  @Override
  public Node visitShowIndexStatement(RelationalSqlParser.ShowIndexStatementContext ctx) {
    return new ShowIndex(getLocation(ctx), getQualifiedName(ctx.tableName));
  }

  @Override
  public Node visitInsertStatement(RelationalSqlParser.InsertStatementContext ctx) {
    QualifiedName qualifiedName = getQualifiedName(ctx.tableName);
    String tableName = qualifiedName.getSuffix();
    String databaseName =
        qualifiedName
            .getPrefix()
            .map(QualifiedName::toString)
            .orElse(clientSession.getDatabaseName());
    if (databaseName == null) {
      throw new SemanticException(DATABASE_NOT_SPECIFIED);
    }
    tableName = tableName.toLowerCase();
    databaseName = databaseName.toLowerCase();

    Query query = (Query) visit(ctx.query());
    if (ctx.columnAliases() != null) {
      List<Identifier> identifiers = visit(ctx.columnAliases().identifier(), Identifier.class);
      if (query.getQueryBody() instanceof Values) {
        return visitInsertValues(
            databaseName, tableName, identifiers, ((Values) query.getQueryBody()));
      } else {
        throw new SemanticException("Only support insert by values.");
      }
    } else {
      if (query.getQueryBody() instanceof Values) {
        TsTable table = DataNodeTableCache.getInstance().getTable(databaseName, tableName);
        if (table == null) {
          throw new SemanticException(new NoTableException(tableName));
        }
        return visitInsertValues(databaseName, table, ((Values) query.getQueryBody()));
      } else {
        throw new SemanticException("Only support insert by values.");
      }
    }
  }

  private Node visitInsertValues(String databaseName, TsTable table, Values queryBody) {
    List<Expression> rows = queryBody.getRows();
    List<InsertRowStatement> rowStatements =
        rows.stream()
            .map(
                r -> {
                  List<Expression> expressions;
                  if (r instanceof Row) {
                    expressions = ((Row) r).getItems();
                  } else if (r instanceof Literal) {
                    expressions = Collections.singletonList(r);
                  } else {
                    throw new SemanticException("unexpected expression: " + r);
                  }
                  return toInsertRowStatement(expressions, table, databaseName);
                })
            .collect(toList());

    InsertRowsStatement insertRowsStatement = new InsertRowsStatement();
    insertRowsStatement.setInsertRowStatementList(rowStatements);
    insertRowsStatement.setWriteToTable(true);
    return new InsertRows(insertRowsStatement, null);
  }

  private Node visitInsertValues(
      String databaseName, String tableName, List<Identifier> identifiers, Values queryBody) {
    List<String> columnNames = identifiers.stream().map(Identifier::getValue).collect(toList());
    int timeColumnIndex = -1;
    for (int i = 0; i < columnNames.size(); i++) {
      if (TIME_COLUMN_NAME.equalsIgnoreCase(columnNames.get(i))) {
        if (timeColumnIndex == -1) {
          timeColumnIndex = i;
        } else {
          throw new SemanticException("One row should only have one time value");
        }
      }
    }
    if (timeColumnIndex != -1) {
      columnNames.remove(timeColumnIndex);
    }

    String[] columnNameArray = columnNames.toArray(new String[0]);

    List<Expression> rows = queryBody.getRows();
    if (timeColumnIndex == -1 && rows.size() > 1) {
      throw new SemanticException("need timestamps when insert multi rows");
    }
    int finalTimeColumnIndex = timeColumnIndex;
    List<InsertRowStatement> rowStatements =
        rows.stream()
            .map(
                r -> {
                  List<Expression> expressions;
                  if (r instanceof Row) {
                    expressions = ((Row) r).getItems();
                  } else if (r instanceof Literal) {
                    expressions = Collections.singletonList(r);
                  } else {
                    throw new SemanticException("unexpected expression: " + r);
                  }
                  return toInsertRowStatement(
                      expressions, finalTimeColumnIndex, columnNameArray, tableName, databaseName);
                })
            .collect(toList());

    InsertRowsStatement insertRowsStatement = new InsertRowsStatement();
    insertRowsStatement.setInsertRowStatementList(rowStatements);
    insertRowsStatement.setWriteToTable(true);
    return new InsertRows(insertRowsStatement, null);
  }

  private InsertRowStatement toInsertRowStatement(
      List<Expression> expressions, TsTable table, String databaseName) {
    InsertRowStatement insertRowStatement = new InsertRowStatement();
    insertRowStatement.setWriteToTable(true);
    insertRowStatement.setDevicePath(new PartialPath(new String[] {table.getTableName()}));

    List<TsTableColumnSchema> columnList = table.getColumnList();
    if (expressions.size() != columnList.size()) {
      throw new SemanticException(
          "expressions and columns do not match, expressions size: "
              + expressions.size()
              + ", columns size: "
              + columnList.size());
    }

    String[] nonTimeColumnNames = new String[columnList.size() - 1];
    Object[] nonTimeValues = new Object[columnList.size() - 1];
    TsTableColumnCategory[] nonTimeColumnCategories =
        new TsTableColumnCategory[columnList.size() - 1];
    MeasurementSchema[] columnSchemas = new MeasurementSchema[columnList.size() - 1];
    TSDataType[] dataTypes = new TSDataType[columnList.size() - 1];
    int nonTimeColumnIndex = 0;
    long timestamp = -1;
    for (int i = 0; i < columnList.size(); i++) {
      TsTableColumnSchema columnSchema = columnList.get(i);
      Expression expression = expressions.get(i);

      if (columnSchema.getColumnCategory().equals(TIME)) {
        if (expression instanceof LongLiteral) {
          timestamp = ((LongLiteral) expression).getParsedValue();
        } else {
          timestamp =
              parseDateTimeFormat(
                  ((StringLiteral) expression).getValue(),
                  CommonDateTimeUtils.currentTime(),
                  zoneId);
        }
      } else {
        Object value = AstUtil.expressionToTsValue(expression);
        nonTimeValues[nonTimeColumnIndex] = value;
        nonTimeColumnNames[nonTimeColumnIndex] = columnSchema.getColumnName();
        dataTypes[nonTimeColumnIndex] = columnSchema.getDataType();
        nonTimeColumnCategories[nonTimeColumnIndex] = columnSchema.getColumnCategory();
        columnSchemas[nonTimeColumnIndex] =
            new MeasurementSchema(columnSchema.getColumnName(), columnSchema.getDataType());
        nonTimeColumnIndex++;
      }
    }

    TimestampPrecisionUtils.checkTimestampPrecision(timestamp);
    insertRowStatement.setTime(timestamp);
    insertRowStatement.setMeasurements(nonTimeColumnNames);
    insertRowStatement.setDataTypes(dataTypes);
    insertRowStatement.setMeasurementSchemas(columnSchemas);
    insertRowStatement.setValues(nonTimeValues);
    insertRowStatement.setColumnCategories(nonTimeColumnCategories);
    insertRowStatement.setNeedInferType(false);
    insertRowStatement.setDatabaseName(databaseName);

    try {
      insertRowStatement.transferType(zoneId);
    } catch (QueryProcessException e) {
      throw new SemanticException(e);
    }
    return insertRowStatement;
  }

  private InsertRowStatement toInsertRowStatement(
      List<Expression> expressions,
      int timeColumnIndex,
      String[] nonTimeColumnNames,
      String tableName,
      String databaseName) {
    InsertRowStatement insertRowStatement = new InsertRowStatement();
    insertRowStatement.setWriteToTable(true);
    insertRowStatement.setDevicePath(new PartialPath(new String[] {tableName}));
    long timestamp;
    int nonTimeColCnt;
    if (timeColumnIndex == -1) {
      timestamp = CommonDateTimeUtils.currentTime();
      nonTimeColCnt = expressions.size();
    } else {
      Expression timeExpression = expressions.get(timeColumnIndex);
      if (timeExpression instanceof LongLiteral) {
        timestamp = ((LongLiteral) timeExpression).getParsedValue();
      } else {
        timestamp =
            parseDateTimeFormat(
                ((StringLiteral) timeExpression).getValue(),
                CommonDateTimeUtils.currentTime(),
                zoneId);
      }
      nonTimeColCnt = expressions.size() - 1;
    }

    if (nonTimeColCnt != nonTimeColumnNames.length) {
      throw new SemanticException(
          String.format(
              "Inconsistent numbers of non-time column names and values: %d-%d",
              nonTimeColumnNames.length, nonTimeColCnt));
    }

    TimestampPrecisionUtils.checkTimestampPrecision(timestamp);
    insertRowStatement.setTime(timestamp);
    insertRowStatement.setMeasurements(nonTimeColumnNames);

    Object[] values = new Object[nonTimeColumnNames.length];
    int valuePos = 0;
    for (int i = 0; i < expressions.size(); i++) {
      if (i != timeColumnIndex) {
        values[valuePos++] = AstUtil.expressionToTsValue(expressions.get(i));
      }
    }

    insertRowStatement.setValues(values);
    insertRowStatement.setNeedInferType(true);
    insertRowStatement.setDatabaseName(databaseName);
    return insertRowStatement;
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
  public Node visitUpdateStatement(final RelationalSqlParser.UpdateStatementContext ctx) {
    return new Update(
        getLocation(ctx),
        new Table(getLocation(ctx), getQualifiedName(ctx.qualifiedName())),
        visit(ctx.updateAssignment(), UpdateAssignment.class),
        Objects.nonNull(ctx.booleanExpression())
            ? (Expression) visit(ctx.booleanExpression())
            : null);
  }

  @Override
  public Node visitDeleteDeviceStatement(
      final RelationalSqlParser.DeleteDeviceStatementContext ctx) {
    return new DeleteDevice(
        getLocation(ctx),
        new Table(getLocation(ctx), getQualifiedName(ctx.qualifiedName())),
        Objects.nonNull(ctx.booleanExpression())
            ? (Expression) visit(ctx.booleanExpression())
            : null);
  }

  @Override
  public Node visitUpdateAssignment(RelationalSqlParser.UpdateAssignmentContext ctx) {
    return new UpdateAssignment(
        (Identifier) visit(ctx.identifier()), (Expression) visit(ctx.expression()));
  }

  @Override
  public Node visitCreateFunctionStatement(RelationalSqlParser.CreateFunctionStatementContext ctx) {
    final String udfName = ((Identifier) visit(ctx.udfName)).getValue();
    final String className = ((Identifier) visit(ctx.className)).getValue();
    if (ctx.uriClause() == null) {
      return new CreateFunction(getLocation(ctx), udfName, className);
    } else {
      String uriString = parseAndValidateURI(ctx.uriClause());
      return new CreateFunction(getLocation(ctx), udfName, className, uriString);
    }
  }

  @Override
  public Node visitUriClause(RelationalSqlParser.UriClauseContext ctx) {
    return super.visitUriClause(ctx);
  }

  @Override
  public Node visitDropFunctionStatement(RelationalSqlParser.DropFunctionStatementContext ctx) {
    final String udfName = ((Identifier) visit(ctx.udfName)).getValue();
    return new DropFunction(getLocation(ctx), udfName);
  }

  @Override
  public Node visitShowFunctionsStatement(RelationalSqlParser.ShowFunctionsStatementContext ctx) {
    return new ShowFunctions();
  }

  @Override
  public Node visitLoadTsFileStatement(RelationalSqlParser.LoadTsFileStatementContext ctx) {
    final Map<String, String> withAttributes =
        ctx.loadFileWithAttributesClause() != null
            ? parseLoadFileWithAttributeClauses(
                ctx.loadFileWithAttributesClause().loadFileWithAttributeClause())
            : new HashMap<>();

    withAttributes.forEach(LoadTsFileConfigurator::validateParameters);
    return new LoadTsFile(
        getLocation(ctx), ((StringLiteral) visit(ctx.fileName)).getValue(), withAttributes);
  }

  private Map<String, String> parseLoadFileWithAttributeClauses(
      List<RelationalSqlParser.LoadFileWithAttributeClauseContext> contexts) {
    final Map<String, String> withAttributesMap = new HashMap<>();
    for (RelationalSqlParser.LoadFileWithAttributeClauseContext context : contexts) {
      withAttributesMap.put(
          ((StringLiteral) visit(context.loadFileWithKey)).getValue(),
          ((StringLiteral) visit(context.loadFileWithValue)).getValue());
    }
    return withAttributesMap;
  }

  @Override
  public Node visitCreatePipeStatement(RelationalSqlParser.CreatePipeStatementContext ctx) {
    final String pipeName = ((Identifier) visit(ctx.identifier())).getValue();
    final boolean hasIfNotExistsCondition =
        ctx.IF() != null && ctx.NOT() != null && ctx.EXISTS() != null;

    final Map<String, String> extractorAttributes =
        ctx.extractorAttributesClause() != null
            ? parseExtractorAttributesClause(
                ctx.extractorAttributesClause().extractorAttributeClause())
            : new HashMap<>(); // DO NOT USE Collections.emptyMap() here
    final Map<String, String> processorAttributes =
        ctx.processorAttributesClause() != null
            ? parseProcessorAttributesClause(
                ctx.processorAttributesClause().processorAttributeClause())
            : new HashMap<>(); // DO NOT USE Collections.emptyMap() here
    final Map<String, String> connectorAttributes =
        ctx.connectorAttributesClause() != null
            ? parseConnectorAttributesClause(
                ctx.connectorAttributesClause().connectorAttributeClause())
            : parseConnectorAttributesClause(
                ctx.connectorAttributesWithoutWithSinkClause().connectorAttributeClause());

    return new CreatePipe(
        pipeName,
        hasIfNotExistsCondition,
        extractorAttributes,
        processorAttributes,
        connectorAttributes);
  }

  private Map<String, String> parseExtractorAttributesClause(
      List<RelationalSqlParser.ExtractorAttributeClauseContext> contexts) {
    final Map<String, String> extractorMap = new HashMap<>();
    for (RelationalSqlParser.ExtractorAttributeClauseContext context : contexts) {
      extractorMap.put(
          ((StringLiteral) visit(context.extractorKey)).getValue(),
          ((StringLiteral) visit(context.extractorValue)).getValue());
    }
    return extractorMap;
  }

  private Map<String, String> parseProcessorAttributesClause(
      List<RelationalSqlParser.ProcessorAttributeClauseContext> contexts) {
    final Map<String, String> processorMap = new HashMap<>();
    for (RelationalSqlParser.ProcessorAttributeClauseContext context : contexts) {
      processorMap.put(
          ((StringLiteral) visit(context.processorKey)).getValue(),
          ((StringLiteral) visit(context.processorValue)).getValue());
    }
    return processorMap;
  }

  private Map<String, String> parseConnectorAttributesClause(
      List<RelationalSqlParser.ConnectorAttributeClauseContext> contexts) {
    final Map<String, String> connectorMap = new HashMap<>();
    for (RelationalSqlParser.ConnectorAttributeClauseContext context : contexts) {
      connectorMap.put(
          ((StringLiteral) visit(context.connectorKey)).getValue(),
          ((StringLiteral) visit(context.connectorValue)).getValue());
    }
    return connectorMap;
  }

  @Override
  public Node visitAlterPipeStatement(RelationalSqlParser.AlterPipeStatementContext ctx) {
    final String pipeName = ((Identifier) visit(ctx.identifier())).getValue();
    final boolean hasIfExistsCondition = ctx.IF() != null && ctx.EXISTS() != null;

    final Map<String, String> extractorAttributes;
    final boolean isReplaceAllExtractorAttributes;
    if (ctx.alterExtractorAttributesClause() != null) {
      extractorAttributes =
          parseExtractorAttributesClause(
              ctx.alterExtractorAttributesClause().extractorAttributeClause());
      isReplaceAllExtractorAttributes =
          Objects.nonNull(ctx.alterExtractorAttributesClause().REPLACE());
    } else {
      extractorAttributes = new HashMap<>(); // DO NOT USE Collections.emptyMap() here
      isReplaceAllExtractorAttributes = false;
    }

    final Map<String, String> processorAttributes;
    final boolean isReplaceAllProcessorAttributes;
    if (ctx.alterProcessorAttributesClause() != null) {
      processorAttributes =
          parseProcessorAttributesClause(
              ctx.alterProcessorAttributesClause().processorAttributeClause());
      isReplaceAllProcessorAttributes =
          Objects.nonNull(ctx.alterProcessorAttributesClause().REPLACE());
    } else {
      processorAttributes = new HashMap<>(); // DO NOT USE Collections.emptyMap() here
      isReplaceAllProcessorAttributes = false;
    }

    final Map<String, String> connectorAttributes;
    final boolean isReplaceAllConnectorAttributes;
    if (ctx.alterConnectorAttributesClause() != null) {
      connectorAttributes =
          parseConnectorAttributesClause(
              ctx.alterConnectorAttributesClause().connectorAttributeClause());
      isReplaceAllConnectorAttributes =
          Objects.nonNull(ctx.alterConnectorAttributesClause().REPLACE());
    } else {
      connectorAttributes = new HashMap<>(); // DO NOT USE Collections.emptyMap() here
      isReplaceAllConnectorAttributes = false;
    }

    return new AlterPipe(
        pipeName,
        hasIfExistsCondition,
        extractorAttributes,
        processorAttributes,
        connectorAttributes,
        isReplaceAllExtractorAttributes,
        isReplaceAllProcessorAttributes,
        isReplaceAllConnectorAttributes);
  }

  @Override
  public Node visitDropPipeStatement(RelationalSqlParser.DropPipeStatementContext ctx) {
    final String pipeName = ((Identifier) visit(ctx.identifier())).getValue();
    final boolean hasIfExistsCondition = ctx.IF() != null && ctx.EXISTS() != null;
    return new DropPipe(pipeName, hasIfExistsCondition);
  }

  @Override
  public Node visitStartPipeStatement(RelationalSqlParser.StartPipeStatementContext ctx) {
    return new StartPipe(((Identifier) visit(ctx.identifier())).getValue());
  }

  @Override
  public Node visitStopPipeStatement(RelationalSqlParser.StopPipeStatementContext ctx) {
    return new StopPipe(((Identifier) visit(ctx.identifier())).getValue());
  }

  @Override
  public Node visitShowPipesStatement(RelationalSqlParser.ShowPipesStatementContext ctx) {
    final String pipeName =
        getIdentifierIfPresent(ctx.identifier()).map(Identifier::getValue).orElse(null);
    final boolean hasWhereClause = ctx.WHERE() != null;
    return new ShowPipes(pipeName, hasWhereClause);
  }

  @Override
  public Node visitCreatePipePluginStatement(
      RelationalSqlParser.CreatePipePluginStatementContext ctx) {
    final String pluginName = ((Identifier) visit(ctx.identifier())).getValue();
    final boolean hasIfNotExistsCondition =
        ctx.IF() != null && ctx.NOT() != null && ctx.EXISTS() != null;
    final String className = ((StringLiteral) visit(ctx.className)).getValue();
    final String uriString = parseAndValidateURI(ctx.uriClause());
    return new CreatePipePlugin(pluginName, hasIfNotExistsCondition, className, uriString);
  }

  private String parseAndValidateURI(RelationalSqlParser.UriClauseContext ctx) {
    final String uriString =
        ctx.uri.identifier() != null
            ? ((Identifier) visit(ctx.uri.identifier())).getValue()
            : ((StringLiteral) visit(ctx.uri.string())).getValue();
    try {
      new URI(uriString);
    } catch (URISyntaxException e) {
      throw new SemanticException(String.format("Invalid URI: %s", uriString));
    }
    return uriString;
  }

  @Override
  public Node visitDropPipePluginStatement(RelationalSqlParser.DropPipePluginStatementContext ctx) {
    final String pluginName = ((Identifier) visit(ctx.identifier())).getValue();
    final boolean hasIfExistsCondition = ctx.IF() != null && ctx.EXISTS() != null;
    return new DropPipePlugin(pluginName, hasIfExistsCondition);
  }

  @Override
  public Node visitShowPipePluginsStatement(
      RelationalSqlParser.ShowPipePluginsStatementContext ctx) {
    return new ShowPipePlugins();
  }

  @Override
  public Node visitCreateTopicStatement(RelationalSqlParser.CreateTopicStatementContext ctx) {
    final String topicName = ((Identifier) visit(ctx.identifier())).getValue();
    final boolean hasIfNotExistsCondition =
        ctx.IF() != null && ctx.NOT() != null && ctx.EXISTS() != null;

    final Map<String, String> topicAttributes =
        ctx.topicAttributesClause() != null
            ? parseTopicAttributesClause(ctx.topicAttributesClause().topicAttributeClause())
            : new HashMap<>(); // DO NOT USE Collections.emptyMap() here

    return new CreateTopic(topicName, hasIfNotExistsCondition, topicAttributes);
  }

  private Map<String, String> parseTopicAttributesClause(
      List<RelationalSqlParser.TopicAttributeClauseContext> contexts) {
    final Map<String, String> tppicMap = new HashMap<>();
    for (RelationalSqlParser.TopicAttributeClauseContext context : contexts) {
      tppicMap.put(
          ((StringLiteral) visit(context.topicKey)).getValue(),
          ((StringLiteral) visit(context.topicValue)).getValue());
    }
    return tppicMap;
  }

  @Override
  public Node visitDropTopicStatement(RelationalSqlParser.DropTopicStatementContext ctx) {
    final String topicName = ((Identifier) visit(ctx.identifier())).getValue();
    final boolean hasIfExistsCondition = ctx.IF() != null && ctx.EXISTS() != null;
    return new DropTopic(topicName, hasIfExistsCondition);
  }

  @Override
  public Node visitShowTopicsStatement(RelationalSqlParser.ShowTopicsStatementContext ctx) {
    final String topicName =
        getIdentifierIfPresent(ctx.identifier()).map(Identifier::getValue).orElse(null);
    return new ShowTopics(topicName);
  }

  @Override
  public Node visitShowSubscriptionsStatement(
      RelationalSqlParser.ShowSubscriptionsStatementContext ctx) {
    final String topicName =
        getIdentifierIfPresent(ctx.identifier()).map(Identifier::getValue).orElse(null);
    return new ShowSubscriptions(topicName);
  }

  @Override
  public Node visitShowDevicesStatement(final RelationalSqlParser.ShowDevicesStatementContext ctx) {
    return new ShowDevice(
        getLocation(ctx),
        new Table(getLocation(ctx), getQualifiedName(ctx.qualifiedName())),
        visitIfPresent(ctx.where, Expression.class).orElse(null),
        visitIfPresent(ctx.limitOffsetClause().offset, Offset.class).orElse(null),
        visitIfPresent(ctx.limitOffsetClause().limit, Node.class).orElse(null));
  }

  @Override
  public Node visitCountDevicesStatement(
      final RelationalSqlParser.CountDevicesStatementContext ctx) {
    return new CountDevice(
        getLocation(ctx),
        new Table(getLocation(ctx), getQualifiedName(ctx.qualifiedName())),
        visitIfPresent(ctx.where, Expression.class).orElse(null));
  }

  @Override
  public Node visitShowClusterStatement(RelationalSqlParser.ShowClusterStatementContext ctx) {
    boolean details = ctx.DETAILS() != null;
    return new ShowCluster(details);
  }

  @Override
  public Node visitShowRegionsStatement(RelationalSqlParser.ShowRegionsStatementContext ctx) {
    TConsensusGroupType regionType = null;
    if (ctx.DATA() != null) {
      regionType = TConsensusGroupType.DataRegion;
    } else if (ctx.SCHEMA() != null) {
      regionType = TConsensusGroupType.SchemaRegion;
    }
    List<PartialPath> databases = null;
    if (ctx.identifier() != null) {
      try {
        // When using the table model, only single level databases are allowed to be used.
        // Therefore, the "root." prefix is omitted from the query syntax, but we need to
        // add it back before querying the server.
        databases =
            Collections.singletonList(new PartialPath("root." + ctx.identifier().getText()));
      } catch (IllegalPathException e) {
        throw new RuntimeException(e);
      }
    }
    // TODO: This will be left untouched for now, well add filtering later on.
    List<Integer> nodeIds = null;
    return new ShowRegions(regionType, databases, nodeIds);
  }

  @Override
  public Node visitShowDataNodesStatement(RelationalSqlParser.ShowDataNodesStatementContext ctx) {
    return new ShowDataNodes();
  }

  @Override
  public Node visitShowConfigNodesStatement(
      RelationalSqlParser.ShowConfigNodesStatementContext ctx) {
    return new ShowConfigNodes();
  }

  @Override
  public Node visitShowAINodesStatement(RelationalSqlParser.ShowAINodesStatementContext ctx) {
    return new ShowAINodes();
  }

  @Override
  public Node visitShowClusterIdStatement(RelationalSqlParser.ShowClusterIdStatementContext ctx) {
    return new ShowClusterId(getLocation(ctx));
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
  public Node visitFlushStatement(RelationalSqlParser.FlushStatementContext ctx) {
    FlushStatement flushStatement = new FlushStatement(StatementType.FLUSH);
    List<PartialPath> storageGroups = null;
    if (ctx.booleanValue() != null) {
      flushStatement.setSeq(Boolean.parseBoolean(ctx.booleanValue().getText()));
    }
    flushStatement.setOnCluster(
        ctx.localOrClusterMode() == null || ctx.localOrClusterMode().LOCAL() == null);
    if (ctx.identifier() != null) {
      storageGroups = new ArrayList<>();
      List<Identifier> identifiers = getIdentifiers(ctx.identifier());
      for (Identifier identifier : identifiers) {
        try {
          storageGroups.add(new PartialPath(PathUtils.qualifyDatabaseName(identifier.getValue())));
        } catch (IllegalPathException e) {
          throw new RuntimeException(e);
        }
      }
    }
    flushStatement.setStorageGroups(storageGroups);
    return new Flush(flushStatement, null);
  }

  @Override
  public Node visitClearCacheStatement(final RelationalSqlParser.ClearCacheStatementContext ctx) {
    final Set<CacheClearOptions> options;
    final RelationalSqlParser.ClearCacheOptionsContext context = ctx.clearCacheOptions();

    if (Objects.isNull(context)) {
      options = Collections.singleton(CacheClearOptions.DEFAULT);
    } else if (context.ATTRIBUTE() != null) {
      options = Collections.singleton(CacheClearOptions.TABLE_ATTRIBUTE);
    } else if (context.QUERY() != null) {
      options = Collections.singleton(CacheClearOptions.QUERY);
    } else {
      options =
          new HashSet<>(
              Arrays.asList(
                  CacheClearOptions.TABLE_ATTRIBUTE,
                  CacheClearOptions.TREE_SCHEMA,
                  CacheClearOptions.QUERY));
    }
    return new ClearCache(
        Objects.isNull(ctx.localOrClusterMode())
            || Objects.nonNull(ctx.localOrClusterMode().CLUSTER()),
        options);
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
    return new ShowVersion(getLocation(ctx));
  }

  @Override
  public Node visitShowCurrentSqlDialectStatement(
      RelationalSqlParser.ShowCurrentSqlDialectStatementContext ctx) {
    return new ShowCurrentSqlDialect(getLocation(ctx));
  }

  @Override
  public Node visitShowCurrentDatabaseStatement(
      RelationalSqlParser.ShowCurrentDatabaseStatementContext ctx) {
    return new ShowCurrentDatabase(getLocation(ctx));
  }

  @Override
  public Node visitShowCurrentUserStatement(
      RelationalSqlParser.ShowCurrentUserStatementContext ctx) {
    return new ShowCurrentUser(getLocation(ctx));
  }

  @Override
  public Node visitShowVariablesStatement(RelationalSqlParser.ShowVariablesStatementContext ctx) {
    return new ShowVariables(getLocation(ctx));
  }

  @Override
  public Node visitShowCurrentTimestampStatement(
      RelationalSqlParser.ShowCurrentTimestampStatementContext ctx) {
    return new ShowCurrentTimestamp(getLocation(ctx));
  }

  @Override
  public Node visitShowQueriesStatement(RelationalSqlParser.ShowQueriesStatementContext ctx) {
    Optional<OrderBy> orderBy = Optional.empty();
    if (ctx.ORDER() != null) {
      orderBy =
          Optional.of(new OrderBy(getLocation(ctx.ORDER()), visit(ctx.sortItem(), SortItem.class)));
    }

    Optional<Offset> offset = Optional.empty();
    if (ctx.limitOffsetClause().OFFSET() != null) {
      offset = visitIfPresent(ctx.limitOffsetClause().offset, Offset.class);
    }

    Optional<Node> limit = Optional.empty();
    if (ctx.limitOffsetClause().LIMIT() != null) {
      if (ctx.limitOffsetClause().limit == null) {
        throw new IllegalStateException("Missing LIMIT value");
      }
      limit = visitIfPresent(ctx.limitOffsetClause().limit, Node.class);
    }

    return new ShowStatement(
        getLocation(ctx),
        QUERIES.getSchemaTableName(),
        visitIfPresent(ctx.where, Expression.class),
        orderBy,
        offset,
        limit);
  }

  @Override
  public Node visitKillQueryStatement(RelationalSqlParser.KillQueryStatementContext ctx) {
    if (ctx.queryId == null) {
      return new KillQuery(null, getLocation(ctx));
    }
    return new KillQuery(((StringLiteral) visit(ctx.queryId)).getValue(), getLocation(ctx));
  }

  @Override
  public Node visitLoadConfigurationStatement(
      RelationalSqlParser.LoadConfigurationStatementContext ctx) {
    return super.visitLoadConfigurationStatement(ctx);
  }

  @Override
  public Node visitSetConfigurationStatement(
      RelationalSqlParser.SetConfigurationStatementContext ctx) {
    SetConfigurationStatement setConfigurationStatement =
        new SetConfigurationStatement(StatementType.SET_CONFIGURATION);
    int nodeId =
        Integer.parseInt(ctx.INTEGER_VALUE() == null ? "-1" : ctx.INTEGER_VALUE().getText());
    Map<String, String> configItems = new HashMap<>();
    List<Property> properties = ImmutableList.of();
    if (ctx.propertyAssignments() != null) {
      properties = visit(ctx.propertyAssignments().property(), Property.class);
    }
    for (Property property : properties) {
      String key = property.getName().getValue();
      Expression propertyValue = property.getNonDefaultValue();
      if (!propertyValue.getExpressionType().equals(TableExpressionType.STRING_LITERAL)) {
        throw new IllegalArgumentException(
            propertyValue.getExpressionType() + " is not supported for 'set configuration'");
      }
      String value = ((StringLiteral) propertyValue).getValue();
      configItems.put(key.trim(), value.trim());
    }
    setConfigurationStatement.setNodeId(nodeId);
    setConfigurationStatement.setConfigItems(configItems);
    return new SetConfiguration(setConfigurationStatement, null);
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
    return new Explain(getLocation(ctx), (Statement) visit(ctx.query()));
  }

  @Override
  public Node visitExplainAnalyze(RelationalSqlParser.ExplainAnalyzeContext ctx) {
    return new ExplainAnalyze(
        getLocation(ctx), ctx.VERBOSE() != null, (Statement) visit(ctx.query()));
  }

  // ********************** query expressions ********************
  @Override
  public Node visitQuery(RelationalSqlParser.QueryContext ctx) {
    Query body = (Query) visit(ctx.queryNoWith());

    return new Query(
        getLocation(ctx),
        visitIfPresent(ctx.with(), With.class),
        body.getQueryBody(),
        body.getFill(),
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

    Optional<Fill> fill = Optional.empty();
    if (ctx.fillClause() != null) {
      fill = visitIfPresent(ctx.fillClause().fillMethod(), Fill.class);
    }

    Optional<OrderBy> orderBy = Optional.empty();
    if (ctx.ORDER() != null) {
      orderBy =
          Optional.of(new OrderBy(getLocation(ctx.ORDER()), visit(ctx.sortItem(), SortItem.class)));
    }

    Optional<Offset> offset = Optional.empty();
    if (ctx.limitOffsetClause().OFFSET() != null) {
      offset = visitIfPresent(ctx.limitOffsetClause().offset, Offset.class);
    }

    Optional<Node> limit = Optional.empty();
    if (ctx.limitOffsetClause().LIMIT() != null) {
      if (ctx.limitOffsetClause().limit == null) {
        throw new IllegalStateException("Missing LIMIT value");
      }
      limit = visitIfPresent(ctx.limitOffsetClause().limit, Node.class);
    }

    if (term instanceof QuerySpecification) {
      // When we have a simple query specification
      // followed by order by, offset, limit or fetch,
      // fold the order by, limit, offset or fetch clauses
      // into the query specification (analyzer/planner
      // expects this structure to resolve references with respect
      // to columns defined in the query specification)
      final QuerySpecification query = (QuerySpecification) term;

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
              fill,
              orderBy,
              offset,
              limit),
          Optional.empty(),
          Optional.empty(),
          Optional.empty(),
          Optional.empty());
    }

    return new Query(getLocation(ctx), Optional.empty(), term, fill, orderBy, offset, limit);
  }

  @Override
  public Node visitPreviousFill(RelationalSqlParser.PreviousFillContext ctx) {
    TimeDuration timeBound = null;
    LongLiteral timeColumn = null;
    List<LongLiteral> fillGroupingElements = null;
    if (ctx.timeBoundClause() != null) {
      timeBound =
          DateTimeUtils.constructTimeDuration(ctx.timeBoundClause().timeDuration().getText());

      if (timeBound.monthDuration != 0 && timeBound.nonMonthDuration != 0) {
        throw new SemanticException(
            "Simultaneous setting of monthly and non-monthly intervals is not supported.");
      }
    }

    if (ctx.timeColumnClause() != null) {
      timeColumn =
          new LongLiteral(
              getLocation(ctx.timeColumnClause().INTEGER_VALUE()),
              ctx.timeColumnClause().INTEGER_VALUE().getText());
    }

    if (ctx.fillGroupClause() != null) {
      fillGroupingElements =
          ctx.fillGroupClause().INTEGER_VALUE().stream()
              .map(index -> new LongLiteral(getLocation(index), index.getText()))
              .collect(toList());
    }

    if (timeColumn != null && (timeBound == null && fillGroupingElements == null)) {
      throw new SemanticException(
          "Don't need to specify TIME_COLUMN while either TIME_BOUND or FILL_GROUP parameter is not specified");
    }
    return new Fill(getLocation(ctx), timeBound, timeColumn, fillGroupingElements);
  }

  @Override
  public Node visitLinearFill(RelationalSqlParser.LinearFillContext ctx) {
    LongLiteral timeColumn = null;
    List<LongLiteral> fillGroupingElements = null;
    if (ctx.timeColumnClause() != null) {
      timeColumn =
          new LongLiteral(
              getLocation(ctx.timeColumnClause().INTEGER_VALUE()),
              ctx.timeColumnClause().INTEGER_VALUE().getText());
    }
    if (ctx.fillGroupClause() != null) {
      fillGroupingElements =
          ctx.fillGroupClause().INTEGER_VALUE().stream()
              .map(index -> new LongLiteral(getLocation(index), index.getText()))
              .collect(toList());
    }

    return new Fill(getLocation(ctx), timeColumn, fillGroupingElements);
  }

  @Override
  public Node visitValueFill(RelationalSqlParser.ValueFillContext ctx) {
    return new Fill(getLocation(ctx), (Literal) visit(ctx.literalExpression()));
  }

  @Override
  public Node visitLimitRowCount(final RelationalSqlParser.LimitRowCountContext ctx) {
    final Expression rowCount;
    if (ctx.ALL() != null) {
      rowCount = new AllRows(getLocation(ctx.ALL()));
    } else if (ctx.rowCount().INTEGER_VALUE() != null) {
      rowCount = new LongLiteral(getLocation(ctx.rowCount().INTEGER_VALUE()), ctx.getText());
    } else {
      rowCount = new Parameter(getLocation(ctx.rowCount().QUESTION_MARK()), parameterPosition);
      parameterPosition++;
    }

    return new Limit(getLocation(ctx), rowCount);
  }

  @Override
  public Node visitRowCount(final RelationalSqlParser.RowCountContext ctx) {
    final Expression rowCount;
    if (ctx.INTEGER_VALUE() != null) {
      rowCount = new LongLiteral(getLocation(ctx.INTEGER_VALUE()), ctx.getText());
    } else {
      rowCount = new Parameter(getLocation(ctx.QUESTION_MARK()), parameterPosition);
      parameterPosition++;
    }
    return new Offset(getLocation(ctx), rowCount);
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
        QualifiedName.of(TableBuiltinScalarFunction.CONCAT.getFunctionName()),
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
    boolean isTryCast = ctx.TRY_CAST() != null;
    return new Cast(
        getLocation(ctx),
        (Expression) visit(ctx.expression()),
        (DataType) visit(ctx.type()),
        isTryCast);
  }

  @Override
  public Node visitSpecialDateTimeFunction(RelationalSqlParser.SpecialDateTimeFunctionContext ctx) {
    CurrentTime.Function function = getDateTimeFunctionType(ctx.name);
    return new CurrentTime(getLocation(ctx), function);
  }

  @Override
  public Node visitDateTimeExpression(RelationalSqlParser.DateTimeExpressionContext ctx) {
    return new LongLiteral(
        getLocation(ctx),
        String.valueOf(
            parseDateExpression(ctx.dateExpression(), CommonDateTimeUtils.currentTime())));
  }

  private Long parseDateExpression(
      RelationalSqlParser.DateExpressionContext ctx, long currentTime) {
    long time;
    time = parseDateTimeFormat(ctx.getChild(0).getText(), currentTime, zoneId);
    for (int i = 1; i < ctx.getChildCount(); i = i + 2) {
      if ("+".equals(ctx.getChild(i).getText())) {
        time += DateTimeUtils.convertDurationStrToLong(time, ctx.getChild(i + 1).getText(), false);
      } else {
        time -= DateTimeUtils.convertDurationStrToLong(time, ctx.getChild(i + 1).getText(), false);
      }
    }
    return time;
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
        QualifiedName.of("substring"),
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

    // Syntactic sugar: first(s1) => first(s1,time), first_by(s1,s2) => first_by(s1,s2,time)
    // So do last and last_by.
    if (name.toString().equalsIgnoreCase(FIRST_AGGREGATION)
        || name.toString().equalsIgnoreCase(LAST_AGGREGATION)) {
      if (arguments.size() == 1) {
        arguments.add(
            new Identifier(
                TimestampOperand.TIMESTAMP_EXPRESSION_STRING.toLowerCase(Locale.ENGLISH)));
      } else if (arguments.size() == 2) {
        check(
            arguments
                .get(1)
                .toString()
                .equalsIgnoreCase(TimestampOperand.TIMESTAMP_EXPRESSION_STRING),
            "The second argument of 'first' or 'last' function must be 'time'",
            ctx);
      }
    } else if (name.toString().equalsIgnoreCase(FIRST_BY_AGGREGATION)
        || name.toString().equalsIgnoreCase(LAST_BY_AGGREGATION)) {
      if (arguments.size() == 2) {
        arguments.add(
            new Identifier(
                TimestampOperand.TIMESTAMP_EXPRESSION_STRING.toLowerCase(Locale.ENGLISH)));
      } else if (arguments.size() == 3) {
        check(
            arguments
                .get(2)
                .toString()
                .equalsIgnoreCase(TimestampOperand.TIMESTAMP_EXPRESSION_STRING),
            "The third argument of 'first_by' or 'last_by' function must be 'time'",
            ctx);
      }
    }

    return new FunctionCall(getLocation(ctx), name, distinct, arguments);
  }

  @Override
  public Node visitDateBinGapFill(RelationalSqlParser.DateBinGapFillContext ctx) {
    TimeDuration timeDuration = DateTimeUtils.constructTimeDuration(ctx.timeDuration().getText());

    if (timeDuration.monthDuration != 0 && timeDuration.nonMonthDuration != 0) {
      throw new SemanticException(
          "Simultaneous setting of monthly and non-monthly intervals is not supported.");
    }

    LongLiteral monthDuration =
        new LongLiteral(
            getLocation(ctx.timeDuration()), String.valueOf(timeDuration.monthDuration));
    LongLiteral nonMonthDuration =
        new LongLiteral(
            getLocation(ctx.timeDuration()), String.valueOf(timeDuration.nonMonthDuration));
    LongLiteral origin =
        ctx.timeValue() == null
            ? new LongLiteral("0")
            : new LongLiteral(
                getLocation(ctx.timeValue()),
                String.valueOf(parseTimeValue(ctx.timeValue(), CommonDateTimeUtils.currentTime())));

    List<Expression> arguments =
        Arrays.asList(
            monthDuration,
            nonMonthDuration,
            (Expression) visit(ctx.valueExpression()),
            origin,
            new BooleanLiteral("true"));
    return new FunctionCall(
        getLocation(ctx), QualifiedName.of(DATE_BIN.getFunctionName()), arguments);
  }

  @Override
  public Node visitDateBin(RelationalSqlParser.DateBinContext ctx) {
    TimeDuration timeDuration = DateTimeUtils.constructTimeDuration(ctx.timeDuration().getText());

    if (timeDuration.monthDuration != 0 && timeDuration.nonMonthDuration != 0) {
      throw new SemanticException(
          "Simultaneous setting of monthly and non-monthly intervals is not supported.");
    }

    LongLiteral monthDuration =
        new LongLiteral(
            getLocation(ctx.timeDuration()), String.valueOf(timeDuration.monthDuration));
    LongLiteral nonMonthDuration =
        new LongLiteral(
            getLocation(ctx.timeDuration()), String.valueOf(timeDuration.nonMonthDuration));
    LongLiteral origin =
        ctx.timeValue() == null
            ? new LongLiteral("0")
            : new LongLiteral(
                getLocation(ctx.timeValue()),
                String.valueOf(parseTimeValue(ctx.timeValue(), CommonDateTimeUtils.currentTime())));

    List<Expression> arguments =
        Arrays.asList(
            monthDuration, nonMonthDuration, (Expression) visit(ctx.valueExpression()), origin);
    return new FunctionCall(
        getLocation(ctx), QualifiedName.of(DATE_BIN.getFunctionName()), arguments);
  }

  private long parseTimeValue(RelationalSqlParser.TimeValueContext ctx, long currentTime) {
    if (ctx.INTEGER_VALUE() != null) {
      try {
        if (ctx.MINUS() != null) {
          return -Long.parseLong(ctx.INTEGER_VALUE().getText());
        }
        return Long.parseLong(ctx.INTEGER_VALUE().getText());
      } catch (NumberFormatException e) {
        throw new SemanticException(
            String.format("Can not parse %s to long value", ctx.INTEGER_VALUE().getText()));
      }
    } else {
      return parseDateExpression(ctx.dateExpression(), currentTime);
    }
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
  public Node visitDatetimeLiteral(RelationalSqlParser.DatetimeLiteralContext ctx) {
    return new LongLiteral(
        getLocation(ctx),
        String.valueOf(
            parseDateTimeFormat(
                ctx.getChild(0).getText(), CommonDateTimeUtils.currentTime(), zoneId)));
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

  private static TsTableColumnCategory getColumnCategory(Token category) {
    if (category == null) {
      return MEASUREMENT;
    }
    switch (category.getType()) {
      case RelationalSqlLexer.ID:
        return ID;
      case RelationalSqlLexer.ATTRIBUTE:
        return ATTRIBUTE;
      case RelationalSqlLexer.TIME:
        return TIME;
      case RelationalSqlLexer.MEASUREMENT:
        return MEASUREMENT;
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
