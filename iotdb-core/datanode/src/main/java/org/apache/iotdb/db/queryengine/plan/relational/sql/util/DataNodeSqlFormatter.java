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

package org.apache.iotdb.db.queryengine.plan.relational.sql.util;

import org.apache.iotdb.db.node_commons.plan.relational.sql.ast.AliasedRelation;
import org.apache.iotdb.db.node_commons.plan.relational.sql.ast.Expression;
import org.apache.iotdb.db.node_commons.plan.relational.sql.ast.Identifier;
import org.apache.iotdb.db.node_commons.plan.relational.sql.ast.Node;
import org.apache.iotdb.db.node_commons.plan.relational.sql.ast.Relation;
import org.apache.iotdb.db.node_commons.plan.relational.sql.ast.TableSubquery;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.AddColumn;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.AlterDB;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.AlterPipe;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.AstVisitor;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.ColumnDefinition;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.CopyTo;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.CreateDB;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.CreateFunction;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.CreatePipe;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.CreatePipePlugin;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.CreateTable;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.CreateTopic;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.CreateView;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.Delete;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.DropColumn;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.DropDB;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.DropFunction;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.DropPipe;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.DropPipePlugin;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.DropSubscription;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.DropTable;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.DropTopic;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.Except;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.Explain;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.ExplainAnalyze;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.Insert;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.Intersect;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.LoadTsFile;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.PatternRecognitionRelation;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.Property;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.QuerySpecification;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.RelationalAuthorStatement;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.RenameColumn;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.RenameTable;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.RowPattern;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.Select;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.SetColumnComment;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.SetProperties;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.SetTableComment;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.ShowClusterId;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.ShowCurrentDatabase;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.ShowCurrentSqlDialect;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.ShowCurrentTimestamp;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.ShowCurrentUser;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.ShowDB;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.ShowFunctions;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.ShowPipePlugins;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.ShowPipes;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.ShowSubscriptions;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.ShowTables;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.ShowTopics;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.ShowVariables;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.ShowVersion;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.StartPipe;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.StopPipe;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.Table;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.TableFunctionArgument;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.TableFunctionInvocation;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.TableFunctionTableArgument;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.Union;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.Update;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.UpdateAssignment;

import com.google.common.base.Joiner;

import java.util.Iterator;
import java.util.List;
import java.util.Objects;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static java.util.stream.Collectors.joining;
import static org.apache.iotdb.db.node_commons.plan.relational.sql.util.ExpressionFormatter.formatOrderBy;
import static org.apache.iotdb.db.queryengine.plan.relational.sql.util.RowPatternFormatter.formatPattern;

public class DataNodeSqlFormatter extends CommonQuerySqlFormatter
    implements AstVisitor<Void, Integer> {

  public DataNodeSqlFormatter(StringBuilder builder) {
    super(builder);
  }

  @Override
  public Void visitRowPattern(RowPattern node, Integer indent) {
    checkArgument(indent == 0, "visitRowPattern should only be called at root");
    builder.append(formatPattern(node));
    return null;
  }

  @Override
  public Void visitQuerySpecification(QuerySpecification node, Integer indent) {
    process(node.getSelect(), indent);

    node.getFrom()
        .ifPresent(
            from -> {
              append(indent, "FROM");
              builder.append('\n');
              append(indent, "  ");
              process(from, indent);
            });

    builder.append('\n');

    node.getWhere()
        .ifPresent(
            where -> append(indent, "WHERE " + SqlFormatter.formatExpression(where)).append('\n'));

    node.getGroupBy()
        .ifPresent(
            groupBy ->
                append(
                        indent,
                        "GROUP BY "
                            + (groupBy.isDistinct() ? " DISTINCT " : "")
                            + org.apache.iotdb.db.node_commons.plan.relational.sql.util
                                .ExpressionFormatter.formatGroupBy(groupBy.getGroupingElements()))
                    .append('\n'));

    node.getHaving()
        .ifPresent(
            having ->
                append(indent, "HAVING " + SqlFormatter.formatExpression(having)).append('\n'));

    node.getOrderBy().ifPresent(orderBy -> process(orderBy, indent));
    node.getOffset().ifPresent(offset -> process(offset, indent));
    node.getLimit().ifPresent(limit -> process(limit, indent));
    return null;
  }

  @Override
  public Void visitSelect(Select node, Integer indent) {
    append(indent, "SELECT");
    if (node.isDistinct()) {
      builder.append(" DISTINCT");
    }

    if (node.getSelectItems().size() > 1) {
      boolean first = true;
      for (org.apache.iotdb.db.node_commons.plan.relational.sql.ast.SelectItem item :
          node.getSelectItems()) {
        builder.append("\n").append(indentString(indent)).append(first ? "  " : ", ");
        process(item, indent);
        first = false;
      }
    } else {
      builder.append(' ');
      process(com.google.common.collect.Iterables.getOnlyElement(node.getSelectItems()), indent);
    }

    builder.append('\n');

    return null;
  }

  @Override
  public Void visitTable(Table node, Integer indent) {
    builder.append(SqlFormatter.formatName(node.getName()));
    return null;
  }

  @Override
  public Void visitPatternRecognitionRelation(PatternRecognitionRelation node, Integer indent) {
    processRelationSuffix(node.getInput(), indent);

    builder.append(" MATCH_RECOGNIZE (\n");
    if (!node.getPartitionBy().isEmpty()) {
      append(indent + 1, "PARTITION BY ")
          .append(
              node.getPartitionBy().stream()
                  .map(
                      org.apache.iotdb.db.node_commons.plan.relational.sql.util.ExpressionFormatter
                          ::formatExpression)
                  .collect(joining(", ")))
          .append("\n");
    }
    if (node.getOrderBy().isPresent()) {
      process(node.getOrderBy().get(), indent + 1);
    }
    if (!node.getMeasures().isEmpty()) {
      append(indent + 1, "MEASURES");
      formatDefinitionList(
          node.getMeasures().stream()
              .map(
                  measure ->
                      SqlFormatter.formatExpression(measure.getExpression())
                          + " AS "
                          + SqlFormatter.formatExpression(measure.getName()))
              .collect(com.google.common.collect.ImmutableList.toImmutableList()),
          indent + 2);
    }
    if (node.getRowsPerMatch().isPresent()) {
      String rowsPerMatch;
      switch (node.getRowsPerMatch().get()) {
        case ONE:
          rowsPerMatch = "ONE ROW PER MATCH";
          break;
        case ALL_SHOW_EMPTY:
          rowsPerMatch = "ALL ROWS PER MATCH SHOW EMPTY MATCHES";
          break;
        case ALL_OMIT_EMPTY:
          rowsPerMatch = "ALL ROWS PER MATCH OMIT EMPTY MATCHES";
          break;
        case ALL_WITH_UNMATCHED:
          rowsPerMatch = "ALL ROWS PER MATCH WITH UNMATCHED ROWS";
          break;
        default:
          throw new IllegalStateException(
              "unexpected rowsPerMatch: " + node.getRowsPerMatch().get());
      }
      append(indent + 1, rowsPerMatch).append("\n");
    }
    if (node.getAfterMatchSkipTo().isPresent()) {
      String skipTo;
      switch (node.getAfterMatchSkipTo().get().getPosition()) {
        case PAST_LAST:
          skipTo = "AFTER MATCH SKIP PAST LAST ROW";
          break;
        case NEXT:
          skipTo = "AFTER MATCH SKIP TO NEXT ROW";
          break;
        case LAST:
          checkState(
              node.getAfterMatchSkipTo().get().getIdentifier().isPresent(),
              "missing identifier in AFTER MATCH SKIP TO LAST");
          skipTo =
              "AFTER MATCH SKIP TO LAST "
                  + SqlFormatter.formatExpression(
                      node.getAfterMatchSkipTo().get().getIdentifier().get());
          break;
        case FIRST:
          checkState(
              node.getAfterMatchSkipTo().get().getIdentifier().isPresent(),
              "missing identifier in AFTER MATCH SKIP TO FIRST");
          skipTo =
              "AFTER MATCH SKIP TO FIRST "
                  + SqlFormatter.formatExpression(
                      node.getAfterMatchSkipTo().get().getIdentifier().get());
          break;
        default:
          throw new IllegalStateException("unexpected skipTo: " + node.getAfterMatchSkipTo().get());
      }
      append(indent + 1, skipTo).append("\n");
    }
    append(indent + 1, "PATTERN (").append(formatPattern(node.getPattern())).append(")\n");
    if (!node.getSubsets().isEmpty()) {
      append(indent + 1, "SUBSET");
      formatDefinitionList(
          node.getSubsets().stream()
              .map(
                  subset ->
                      SqlFormatter.formatExpression(subset.getName())
                          + " = "
                          + subset.getIdentifiers().stream()
                              .map(
                                  org.apache.iotdb.db.node_commons.plan.relational.sql.util
                                          .ExpressionFormatter
                                      ::formatExpression)
                              .collect(joining(", ", "(", ")")))
              .collect(com.google.common.collect.ImmutableList.toImmutableList()),
          indent + 2);
    }
    append(indent + 1, "DEFINE");
    formatDefinitionList(
        node.getVariableDefinitions().stream()
            .map(
                variable ->
                    SqlFormatter.formatExpression(variable.getName())
                        + " AS "
                        + SqlFormatter.formatExpression(variable.getExpression()))
            .collect(com.google.common.collect.ImmutableList.toImmutableList()),
        indent + 2);

    builder.append(")");

    return null;
  }

  @Override
  public Void visitUnion(Union node, Integer indent) {
    Iterator<Relation> relations = node.getRelations().iterator();

    while (relations.hasNext()) {
      processRelation(relations.next(), indent);

      if (relations.hasNext()) {
        builder.append("UNION ");
        if (!node.isDistinct()) {
          builder.append("ALL ");
        }
      }
    }

    return null;
  }

  @Override
  public Void visitExcept(Except node, Integer indent) {
    processRelation(node.getLeft(), indent);

    builder.append("EXCEPT ");
    if (!node.isDistinct()) {
      builder.append("ALL ");
    }

    processRelation(node.getRight(), indent);

    return null;
  }

  @Override
  public Void visitIntersect(Intersect node, Integer indent) {
    Iterator<Relation> relations = node.getRelations().iterator();

    while (relations.hasNext()) {
      processRelation(relations.next(), indent);

      if (relations.hasNext()) {
        builder.append("INTERSECT ");
        if (!node.isDistinct()) {
          builder.append("ALL ");
        }
      }
    }

    return null;
  }

  @Override
  public Void visitExplain(Explain node, Integer indent) {
    builder.append("EXPLAIN ");
    builder.append("\n");
    process(node.getStatement(), indent);
    return null;
  }

  @Override
  public Void visitCopyTo(CopyTo node, Integer context) {
    builder.append("COPY\n");
    builder.append("(\n");
    process(node.getQueryStatement(), context);
    builder.append("\n) ");
    builder.append("TO ");
    builder.append('\'');
    builder.append(node.getTargetFileName());
    builder.append('\'');
    builder.append("\n");
    builder.append(node.getOptions().toString());
    return null;
  }

  @Override
  public Void visitExplainAnalyze(ExplainAnalyze node, Integer indent) {
    builder.append("EXPLAIN ANALYZE");
    if (node.isVerbose()) {
      builder.append(" VERBOSE");
    }
    builder.append("\n");
    process(node.getStatement(), indent);
    return null;
  }

  @Override
  public Void visitShowDB(ShowDB node, Integer indent) {
    builder.append("SHOW DATABASE");
    return null;
  }

  @Override
  public Void visitShowTables(ShowTables node, Integer indent) {
    builder.append("SHOW TABLES");

    if (node.isDetails()) {
      builder.append(" DETAILS");
    }

    node.getDbName().ifPresent(db -> builder.append(" FROM ").append(SqlFormatter.formatName(db)));

    return null;
  }

  @Override
  public Void visitShowFunctions(ShowFunctions node, Integer indent) {
    builder.append("SHOW FUNCTIONS");
    return null;
  }

  @Override
  public Void visitShowCurrentSqlDialect(ShowCurrentSqlDialect node, Integer context) {
    builder.append(node.toString());
    return null;
  }

  @Override
  public Void visitShowCurrentDatabase(ShowCurrentDatabase node, Integer context) {
    builder.append(node.toString());
    return null;
  }

  @Override
  public Void visitShowCurrentUser(ShowCurrentUser node, Integer context) {
    builder.append(node.toString());
    return null;
  }

  @Override
  public Void visitShowVersion(ShowVersion node, Integer context) {
    builder.append(node.toString());
    return null;
  }

  @Override
  public Void visitShowVariables(ShowVariables node, Integer context) {
    builder.append(node.toString());
    return null;
  }

  @Override
  public Void visitShowClusterId(ShowClusterId node, Integer context) {
    builder.append(node.toString());
    return null;
  }

  @Override
  public Void visitShowCurrentTimestamp(ShowCurrentTimestamp node, Integer context) {
    builder.append(node.toString());
    return null;
  }

  @Override
  public Void visitDelete(Delete node, Integer indent) {
    builder.append("DELETE FROM ").append(SqlFormatter.formatName(node.getTable().getName()));
    node.getWhere()
        .ifPresent(where -> builder.append(" WHERE ").append(SqlFormatter.formatExpression(where)));
    return null;
  }

  @Override
  public Void visitCreateDB(CreateDB node, Integer indent) {
    builder.append("CREATE DATABASE ");
    if (node.exists()) {
      builder.append("IF NOT EXISTS ");
    }
    builder.append(node.getDbName()).append(" ");
    builder.append(formatPropertiesMultiLine(node.getProperties()));
    return null;
  }

  @Override
  public Void visitAlterDB(AlterDB node, Integer indent) {
    builder.append("ALTER DATABASE ");
    if (node.exists()) {
      builder.append("IF EXISTS ");
    }
    builder.append(node.getDbName()).append(" ");
    builder.append(formatPropertiesMultiLine(node.getProperties()));
    return null;
  }

  @Override
  public Void visitDropDB(DropDB node, Integer indent) {
    builder.append("DROP DATABASE ");
    if (node.isExists()) {
      builder.append("IF EXISTS ");
    }
    builder.append(SqlFormatter.formatName(node.getDbName()));
    return null;
  }

  @Override
  public Void visitCreateTable(CreateTable node, Integer indent) {
    builder.append("CREATE TABLE ");
    if (node.isIfNotExists()) {
      builder.append("IF NOT EXISTS ");
    }
    String tableName = SqlFormatter.formatName(node.getName());
    builder.append(tableName).append(" (\n");

    String elementIndent = indentString(indent + 1);
    String columnList =
        node.getElements().stream()
            .map(
                element -> {
                  if (element != null) {
                    return elementIndent + formatColumnDefinition(element);
                  }
                  throw new UnsupportedOperationException("unknown table element: " + element);
                })
            .collect(joining(",\n"));
    builder.append(columnList);
    builder.append("\n").append(")");

    node.getCharsetName().ifPresent(charset -> builder.append(" CHARSET ").append(charset));

    if (Objects.nonNull(node.getComment())) {
      builder.append(" COMMENT '").append(node.getComment()).append("'");
    }

    builder.append(formatPropertiesMultiLine(node.getProperties()));
    return null;
  }

  @Override
  public Void visitCreateView(CreateView node, Integer indent) {
    builder.append("CREATE ");
    if (node.isReplace()) {
      builder.append("OR REPLACE ");
    }
    builder.append("VIEW ");
    String tableName = SqlFormatter.formatName(node.getName());
    builder.append(tableName).append(" (\n");

    String elementIndent = indentString(indent + 1);
    String columnList =
        node.getElements().stream()
            .map(
                element -> {
                  if (element != null) {
                    return elementIndent + formatColumnDefinition(element);
                  }
                  throw new UnsupportedOperationException("unknown table element: " + element);
                })
            .collect(joining(",\n"));
    builder.append(columnList);
    builder.append("\n").append(")");

    if (Objects.nonNull(node.getComment())) {
      builder.append(" COMMENT '").append(node.getComment()).append("'");
    }

    if (node.isRestrict()) {
      builder.append(" RESTRICT");
    }

    builder.append(formatPropertiesMultiLine(node.getProperties()));
    builder.append(" AS ").append(node.getPrefixPath().toString());

    return null;
  }

  @Override
  public Void visitDropTable(DropTable node, Integer indent) {
    builder.append("DROP");
    builder.append(node.isView() ? " VIEW " : " TABLE ");
    if (node.isExists()) {
      builder.append("IF EXISTS ");
    }
    builder.append(SqlFormatter.formatName(node.getTableName()));

    return null;
  }

  @Override
  public Void visitRenameTable(RenameTable node, Integer indent) {
    builder.append("ALTER");
    builder.append(node.isView() ? " VIEW " : " TABLE ");
    if (node.tableIfExists()) {
      builder.append("IF EXISTS ");
    }

    builder
        .append(SqlFormatter.formatName(node.getSource()))
        .append(" RENAME TO ")
        .append(SqlFormatter.formatName(node.getTarget()));

    return null;
  }

  @Override
  public Void visitSetProperties(SetProperties node, Integer context) {
    SetProperties.Type type = node.getType();
    builder.append("ALTER ");
    switch (type) {
      case TABLE:
        builder.append("TABLE ");
      case MATERIALIZED_VIEW:
        builder.append("MATERIALIZED VIEW ");
      case TREE_VIEW:
        builder.append("VIEW ");
    }
    if (node.ifExists()) {
      builder.append("IF EXISTS ");
    }

    builder
        .append(SqlFormatter.formatName(node.getName()))
        .append(" SET PROPERTIES ")
        .append(joinProperties(node.getProperties()));

    return null;
  }

  @Override
  public Void visitRenameColumn(RenameColumn node, Integer indent) {
    builder.append("ALTER");
    builder.append(node.isView() ? " VIEW " : " TABLE ");
    if (node.tableIfExists()) {
      builder.append("IF EXISTS ");
    }

    builder.append(SqlFormatter.formatName(node.getTable())).append("RENAME COLUMN ");
    if (node.columnIfExists()) {
      builder.append("IF EXISTS ");
    }

    builder
        .append(SqlFormatter.formatName(node.getSource()))
        .append(" TO ")
        .append(SqlFormatter.formatName(node.getTarget()));

    return null;
  }

  @Override
  public Void visitDropColumn(DropColumn node, Integer indent) {
    builder.append("ALTER");
    builder.append(node.isView() ? " VIEW " : " TABLE ");
    if (node.tableIfExists()) {
      builder.append("IF EXISTS ");
    }

    builder.append(SqlFormatter.formatName(node.getTable())).append("DROP COLUMN ");
    if (node.columnIfExists()) {
      builder.append("IF EXISTS ");
    }

    builder.append(SqlFormatter.formatName(node.getField()));

    return null;
  }

  @Override
  public Void visitAddColumn(AddColumn node, Integer indent) {
    builder.append("ALTER");
    builder.append(node.isView() ? " VIEW " : " TABLE ");
    if (node.tableIfExists()) {
      builder.append("IF EXISTS ");
    }

    builder.append(SqlFormatter.formatName(node.getTableName())).append("ADD COLUMN ");
    if (node.columnIfNotExists()) {
      builder.append("IF NOT EXISTS ");
    }

    builder.append(formatColumnDefinition(node.getColumn()));

    return null;
  }

  @Override
  public Void visitSetTableComment(SetTableComment node, Integer indent) {
    builder
        .append("COMMENT ON")
        .append(node.isView() ? " VIEW " : " TABLE ")
        .append(SqlFormatter.formatName(node.getTableName()))
        .append(" IS ")
        .append(node.getComment());

    return null;
  }

  @Override
  public Void visitSetColumnComment(SetColumnComment node, Integer indent) {
    builder
        .append("COMMENT ON COLUMN ")
        .append(SqlFormatter.formatName(node.getTable()))
        .append(".")
        .append(SqlFormatter.formatName(node.getField()))
        .append(" IS ")
        .append(node.getComment());
    return null;
  }

  @Override
  public Void visitInsert(Insert node, Integer indent) {
    builder.append("INSERT INTO ").append(SqlFormatter.formatName(node.getTarget()));

    node.getColumns()
        .ifPresent(
            columns -> builder.append(" (").append(Joiner.on(", ").join(columns)).append(")"));

    builder.append("\n");

    process(node.getQuery(), indent);

    return null;
  }

  @Override
  public Void visitUpdate(Update node, Integer indent) {
    builder
        .append("UPDATE ")
        .append(SqlFormatter.formatName(node.getTable().getName()))
        .append(" SET");
    int setCounter = node.getAssignments().size() - 1;
    for (UpdateAssignment assignment : node.getAssignments()) {
      builder
          .append("\n")
          .append(indentString(indent + 1))
          .append(((Identifier) assignment.getName()).getValue())
          .append(" = ")
          .append(SqlFormatter.formatExpression(assignment.getValue()));
      if (setCounter > 0) {
        builder.append(",");
      }
      setCounter--;
    }
    node.getWhere()
        .ifPresent(
            where ->
                builder
                    .append("\n")
                    .append(indentString(indent))
                    .append("WHERE ")
                    .append(SqlFormatter.formatExpression(where)));
    return null;
  }

  @Override
  public Void visitCreateFunction(CreateFunction node, Integer indent) {
    builder
        .append("CREATE FUNCTION ")
        .append(node.getUdfName())
        .append(" AS ")
        .append(node.getClassName());
    node.getUriString().ifPresent(uri -> builder.append(" USING URI ").append(uri));
    return null;
  }

  @Override
  public Void visitDropFunction(DropFunction node, Integer indent) {
    builder.append("DROP FUNCTION ");
    builder.append(node.getUdfName());
    return null;
  }

  @Override
  public Void visitLoadTsFile(LoadTsFile node, Integer indent) {
    builder.append("LOAD ");
    builder.append("\"" + node.getFilePath() + "\"");
    builder.append(" \n");

    if (!node.getLoadAttributes().isEmpty()) {
      builder
          .append("WITH (")
          .append("\n")
          .append(
              node.getLoadAttributes().entrySet().stream()
                  .map(
                      entry ->
                          indentString(1)
                              + "\""
                              + entry.getKey()
                              + "\" = \""
                              + entry.getValue()
                              + "\"")
                  .collect(joining(", " + "\n")))
          .append(")\n");
    }
    return null;
  }

  @Override
  public Void visitCreatePipe(CreatePipe node, Integer context) {
    builder.append("CREATE PIPE ");
    if (node.hasIfNotExistsCondition()) {
      builder.append("IF NOT EXISTS ");
    }
    builder.append(node.getPipeName());
    builder.append(" \n");

    if (!node.getSourceAttributes().isEmpty()) {
      builder
          .append("WITH SOURCE (")
          .append("\n")
          .append(
              node.getSourceAttributes().entrySet().stream()
                  .map(
                      entry ->
                          indentString(1)
                              + "\""
                              + entry.getKey()
                              + "\" = \""
                              + entry.getValue()
                              + "\"")
                  .collect(joining(", " + "\n")))
          .append(")\n");
    }

    if (!node.getProcessorAttributes().isEmpty()) {
      builder
          .append("WITH PROCESSOR (")
          .append("\n")
          .append(
              node.getProcessorAttributes().entrySet().stream()
                  .map(
                      entry ->
                          indentString(1)
                              + "\""
                              + entry.getKey()
                              + "\" = \""
                              + entry.getValue()
                              + "\"")
                  .collect(joining(", " + "\n")))
          .append(")\n");
    }

    if (!node.getSinkAttributes().isEmpty()) {
      builder
          .append("WITH SINK (")
          .append("\n")
          .append(
              node.getSinkAttributes().entrySet().stream()
                  .map(
                      entry ->
                          indentString(1)
                              + "\""
                              + entry.getKey()
                              + "\" = \""
                              + entry.getValue()
                              + "\"")
                  .collect(joining(", " + "\n")))
          .append(")");
    }

    return null;
  }

  @Override
  public Void visitAlterPipe(AlterPipe node, Integer context) {
    builder.append("ALTER PIPE ");
    if (node.hasIfExistsCondition()) {
      builder.append("IF EXISTS ");
    }
    builder.append(node.getPipeName());
    builder.append(" \n");

    builder
        .append(node.isReplaceAllExtractorAttributes() ? "REPLACE" : "MODIFY")
        .append(" SOURCE (")
        .append("\n")
        .append(
            node.getExtractorAttributes().entrySet().stream()
                .map(
                    entry ->
                        indentString(1)
                            + "\""
                            + entry.getKey()
                            + "\" = \""
                            + entry.getValue()
                            + "\"")
                .collect(joining(", " + "\n")))
        .append(")\n");

    builder
        .append(node.isReplaceAllProcessorAttributes() ? "REPLACE" : "MODIFY")
        .append(" PROCESSOR (")
        .append("\n")
        .append(
            node.getProcessorAttributes().entrySet().stream()
                .map(
                    entry ->
                        indentString(1)
                            + "\""
                            + entry.getKey()
                            + "\" = \""
                            + entry.getValue()
                            + "\"")
                .collect(joining(", " + "\n")))
        .append(")\n");

    builder
        .append(node.isReplaceAllConnectorAttributes() ? "REPLACE" : "MODIFY")
        .append(" SINK (")
        .append("\n")
        .append(
            node.getConnectorAttributes().entrySet().stream()
                .map(
                    entry ->
                        indentString(1)
                            + "\""
                            + entry.getKey()
                            + "\" = \""
                            + entry.getValue()
                            + "\"")
                .collect(joining(", " + "\n")))
        .append(")");

    return null;
  }

  @Override
  public Void visitDropPipe(DropPipe node, Integer context) {
    builder.append("DROP PIPE ");
    if (node.hasIfExistsCondition()) {
      builder.append("IF EXISTS ");
    }
    builder.append(node.getPipeName());

    return null;
  }

  @Override
  public Void visitStartPipe(StartPipe node, Integer context) {
    builder.append("START PIPE ").append(node.getPipeName());
    return null;
  }

  @Override
  public Void visitStopPipe(StopPipe node, Integer context) {
    builder.append("STOP PIPE ").append(node.getPipeName());
    return null;
  }

  @Override
  public Void visitShowPipes(ShowPipes node, Integer context) {
    builder.append("SHOW PIPES");
    return null;
  }

  @Override
  public Void visitCreatePipePlugin(CreatePipePlugin node, Integer context) {
    builder.append("CREATE PIPEPLUGIN ");
    if (node.hasIfNotExistsCondition()) {
      builder.append("IF NOT EXISTS ");
    }
    builder.append(node.getPluginName());
    builder.append("\n");

    builder.append("AS \"");
    builder.append(node.getClassName());
    builder.append("\"\n");

    builder.append("USING URI \"");
    builder.append(node.getUriString());
    builder.append("\"");

    return null;
  }

  @Override
  public Void visitDropPipePlugin(DropPipePlugin node, Integer context) {
    builder.append("DROP PIPEPLUGIN ");
    if (node.hasIfExistsCondition()) {
      builder.append("IF EXISTS ");
    }
    builder.append(node.getPluginName());

    return null;
  }

  @Override
  public Void visitShowPipePlugins(ShowPipePlugins node, Integer context) {
    builder.append("SHOW PIPEPLUGINS");
    return null;
  }

  @Override
  public Void visitCreateTopic(CreateTopic node, Integer context) {
    builder.append("CREATE TOPIC ");
    if (node.hasIfNotExistsCondition()) {
      builder.append("IF NOT EXISTS ");
    }
    builder.append(node.getTopicName());
    builder.append(" \n");

    if (!node.getTopicAttributes().isEmpty()) {
      builder
          .append("WITH (")
          .append("\n")
          .append(
              node.getTopicAttributes().entrySet().stream()
                  .map(
                      entry ->
                          indentString(1)
                              + "\""
                              + entry.getKey()
                              + "\" = \""
                              + entry.getValue()
                              + "\"")
                  .collect(joining(", " + "\n")))
          .append(")\n");
    }

    return null;
  }

  @Override
  public Void visitDropTopic(DropTopic node, Integer context) {
    builder.append("DROP TOPIC ");
    if (node.hasIfExistsCondition()) {
      builder.append("IF EXISTS ");
    }
    builder.append(node.getTopicName());

    return null;
  }

  @Override
  public Void visitShowTopics(ShowTopics node, Integer context) {
    if (Objects.isNull(node.getTopicName())) {
      builder.append("SHOW TOPICS");
    } else {
      builder.append("SHOW TOPIC ").append(node.getTopicName());
    }

    return null;
  }

  @Override
  public Void visitShowSubscriptions(ShowSubscriptions node, Integer context) {
    if (Objects.isNull(node.getTopicName())) {
      builder.append("SHOW SUBSCRIPTIONS");
    } else {
      builder.append("SHOW SUBSCRIPTIONS ON ").append(node.getTopicName());
    }

    return null;
  }

  @Override
  public Void visitDropSubscription(DropSubscription node, Integer context) {
    builder.append("DROP SUBSCRIPTION ");
    if (node.hasIfExistsCondition()) {
      builder.append("IF EXISTS ");
    }
    builder.append(node.getSubscriptionId());

    return null;
  }

  @Override
  public Void visitRelationalAuthorPlan(RelationalAuthorStatement node, Integer context) {
    switch (node.getAuthorType()) {
      case GRANT_USER_ANY:
        builder.append(
            "GRANT "
                + node.getPrivilegesString()
                + " ON ANY"
                + " TO USER "
                + node.getUserName()
                + (node.isGrantOption() ? " WITH GRANT OPTION" : ""));
        break;
      case GRANT_USER_ALL:
        builder.append(
            "GRANT ALL TO USER "
                + node.getUserName()
                + (node.isGrantOption() ? " WITH GRANT OPTION" : ""));
        break;
      case GRANT_USER_DB:
        builder.append(
            "GRANT "
                + node.getPrivilegesString()
                + " ON DATABASE "
                + node.getDatabase()
                + " TO USER "
                + node.getUserName()
                + (node.isGrantOption() ? " WITH GRANT OPTION" : ""));
        break;
      case GRANT_USER_SYS:
        builder.append(
            "GRANT "
                + node.getPrivilegesString()
                + " TO USER "
                + node.getUserName()
                + (node.isGrantOption() ? " WITH GRANT OPTION" : ""));
        break;
      case GRANT_USER_TB:
        builder.append(
            "GRANT "
                + node.getPrivilegesString()
                + " ON TABLE "
                + node.getDatabase()
                + "."
                + node.getTableName()
                + " TO USER "
                + node.getUserName()
                + (node.isGrantOption() ? " WITH GRANT OPTION" : ""));
        break;
      case GRANT_ROLE_ANY:
        builder.append(
            "GRANT "
                + node.getPrivilegesString()
                + " ON ANY"
                + " TO ROLE "
                + node.getRoleName()
                + (node.isGrantOption() ? " WITH GRANT OPTION" : ""));
        break;
      case GRANT_ROLE_ALL:
        builder.append(
            "GRANT ALL TO ROLE "
                + node.getRoleName()
                + (node.isGrantOption() ? " WITH GRANT OPTION" : ""));
        break;
      case GRANT_ROLE_DB:
        builder.append(
            "GRANT "
                + node.getPrivilegesString()
                + " ON DATABASE "
                + node.getDatabase()
                + " TO ROLE "
                + node.getRoleName()
                + (node.isGrantOption() ? " WITH GRANT OPTION" : ""));
        break;
      case GRANT_ROLE_SYS:
        builder.append(
            "GRANT "
                + node.getPrivilegesString()
                + " TO ROLE "
                + node.getRoleName()
                + (node.isGrantOption() ? " WITH GRANT OPTION" : ""));
        break;
      case GRANT_ROLE_TB:
        builder.append(
            "GRANT "
                + node.getPrivilegesString()
                + " ON TABLE "
                + node.getDatabase()
                + "."
                + node.getTableName()
                + " TO ROLE "
                + node.getRoleName()
                + (node.isGrantOption() ? " WITH GRANT OPTION" : ""));
        break;
      case REVOKE_USER_ANY:
        builder.append(
            "REVOKE "
                + (node.isGrantOption() ? "GRANT OPTION FOR " : "")
                + node.getPrivilegesString()
                + " ON ANY FROM USER "
                + node.getUserName());
        break;
      case REVOKE_USER_ALL:
        builder.append(
            "REVOKE"
                + (node.isGrantOption() ? "GRANT OPTION FOR ALL" : "ALL")
                + " FROM USER "
                + node.getUserName());
        break;
      case REVOKE_USER_DB:
        builder.append(
            "REVOKE "
                + (node.isGrantOption() ? "GRANT OPTION FOR " : "")
                + node.getPrivilegesString()
                + " ON DATABASE "
                + node.getDatabase()
                + " FROM USER "
                + node.getUserName());
        break;
      case REVOKE_USER_SYS:
        builder.append(
            "REVOKE "
                + (node.isGrantOption() ? "GRANT OPTION FOR " : "")
                + node.getPrivilegesString()
                + "FROM USER "
                + node.getUserName());
        break;
      case REVOKE_USER_TB:
        builder.append(
            "REVOKE "
                + (node.isGrantOption() ? "GRANT OPTION FOR " : "")
                + node.getPrivilegesString()
                + " ON TABLE "
                + node.getDatabase()
                + "."
                + node.getTableName()
                + " FROM USER "
                + node.getUserName());
        break;
      case REVOKE_ROLE_ANY:
        builder.append(
            "REVOKE "
                + (node.isGrantOption() ? "GRANT OPTION FOR " : "")
                + node.getPrivilegesString()
                + " ON ANY FROM ROLE "
                + node.getRoleName());
        break;
      case REVOKE_ROLE_ALL:
        builder.append(
            "REVOKE "
                + (node.isGrantOption() ? "GRANT OPTION FOR ALL" : "ALL")
                + " FROM ROLE "
                + node.getRoleName());
        break;
      case REVOKE_ROLE_DB:
        builder.append(
            "REVOKE "
                + (node.isGrantOption() ? "GRANT OPTION FOR " : "")
                + node.getPrivilegesString()
                + " ON DATABASE "
                + node.getDatabase()
                + " FROM ROLE "
                + node.getRoleName());
        break;
      case REVOKE_ROLE_SYS:
        builder.append(
            "REVOKE "
                + (node.isGrantOption() ? "GRANT OPTION FOR " : "")
                + node.getPrivilegesString()
                + " FROM ROLE "
                + node.getRoleName());
        break;
      case REVOKE_ROLE_TB:
        builder.append(
            "REVOKE "
                + (node.isGrantOption() ? "GRANT OPTION FOR " : "")
                + node.getPrivilegesString()
                + " ON TABLE "
                + node.getDatabase()
                + "."
                + node.getTableName()
                + " FROM ROLE "
                + node.getRoleName());
        break;
      case GRANT_USER_ROLE:
        builder.append("GRANT ROLE " + node.getRoleName() + " TO " + node.getUserName());
        break;
      case REVOKE_USER_ROLE:
        builder.append("REVOKE ROLE " + node.getRoleName() + " FROM " + node.getUserName());
        break;
      case CREATE_USER:
        builder.append("CREATE USER " + node.getUserName());
        break;
      case CREATE_ROLE:
        builder.append("CREATE ROLE " + node.getRoleName());
        break;
      case UPDATE_USER:
        builder.append("ALTER USER " + node.getUserName() + " SET PASSWORD");
        break;
      case LIST_USER:
        builder.append("LIST USER ");
        break;
      case LIST_ROLE:
        builder.append("LIST ROLE ");
        break;
      case LIST_USER_PRIV:
        builder.append("LIST PRIVILEGES OF USER " + node.getUserName());
        break;
      case LIST_ROLE_PRIV:
        builder.append("LIST PRIVILEGES OF ROLE " + node.getRoleName());
        break;
      case DROP_ROLE:
        builder.append("DROP ROLE " + node.getRoleName());
        break;
      case DROP_USER:
        builder.append("DROP USER " + node.getUserName());
        break;
      default:
        break;
    }
    return null;
  }

  @Override
  public Void visitTableFunctionInvocation(TableFunctionInvocation node, Integer indent) {
    append(indent, "TABLE(");
    appendTableFunctionInvocation(node, indent + 1);
    builder.append(")");
    return null;
  }

  @Override
  public Void visitTableArgument(TableFunctionTableArgument node, Integer indent) {
    Relation relation = node.getTable();
    Node unaliased =
        relation instanceof AliasedRelation ? ((AliasedRelation) relation).getRelation() : relation;
    if (unaliased instanceof TableSubquery) {
      unaliased = ((TableSubquery) unaliased).getQuery();
    }
    builder.append("TABLE(");
    process(unaliased, indent);
    builder.append(")");
    if (relation instanceof AliasedRelation) {
      AliasedRelation aliasedRelation = (AliasedRelation) relation;
      builder.append(" AS ").append(SqlFormatter.formatName(aliasedRelation.getAlias()));
      appendAliasColumns(builder, aliasedRelation.getColumnNames());
    }
    if (node.getPartitionBy().isPresent()) {
      builder.append("\n");
      append(indent, "PARTITION BY ")
          .append(
              node.getPartitionBy().get().stream()
                  .map(SqlFormatter::formatExpression)
                  .collect(joining(", ")));
    }
    node.getOrderBy()
        .ifPresent(
            orderBy -> {
              builder.append("\n");
              append(indent, formatOrderBy(orderBy));
            });

    return null;
  }

  @Override
  protected boolean needsParenthesesForRelationSuffix(Relation relation) {
    return super.needsParenthesesForRelationSuffix(relation)
        || relation instanceof PatternRecognitionRelation;
  }

  @Override
  protected boolean isSimpleTableRelation(Relation relation) {
    return relation instanceof Table;
  }

  @Override
  protected String formatSimpleTableRelationName(Relation relation) {
    return SqlFormatter.formatName(((Table) relation).getName());
  }

  private String formatPropertiesMultiLine(List<Property> properties) {
    if (properties.isEmpty()) {
      return "";
    }

    String propertyList =
        properties.stream()
            .map(
                element ->
                    SqlFormatter.INDENT
                        + SqlFormatter.formatName(element.getName())
                        + " = "
                        + (element.isSetToDefault()
                            ? "DEFAULT"
                            : SqlFormatter.formatExpression(element.getNonDefaultValue())))
            .collect(joining(",\n"));

    return "\nWITH (\n" + propertyList + "\n)";
  }

  private String formatColumnDefinition(ColumnDefinition column) {
    StringBuilder stringBuilder =
        new StringBuilder()
            .append(SqlFormatter.formatName(column.getName()))
            .append(" ")
            .append(column.getType())
            .append(" ")
            .append(column.getColumnCategory());

    column.getCharsetName().ifPresent(charset -> stringBuilder.append(" CHARSET ").append(charset));

    if (Objects.nonNull(column.getComment())) {
      stringBuilder.append(" COMMENT '").append(column.getComment()).append("'");
    }
    return stringBuilder.toString();
  }

  private String joinProperties(List<Property> properties) {
    return properties.stream()
        .map(
            element ->
                SqlFormatter.formatName(element.getName())
                    + " = "
                    + (element.isSetToDefault()
                        ? "DEFAULT"
                        : SqlFormatter.formatExpression(element.getNonDefaultValue())))
        .collect(joining(", "));
  }

  private void appendTableFunctionInvocation(TableFunctionInvocation node, Integer indent) {
    builder.append(SqlFormatter.formatName(node.getName())).append("(\n");
    appendTableFunctionArguments(node.getArguments(), indent + 1);
    builder.append(")");
  }

  private void appendTableFunctionArguments(List<TableFunctionArgument> arguments, int indent) {
    for (int i = 0; i < arguments.size(); i++) {
      TableFunctionArgument argument = arguments.get(i);
      if (argument.getName().isPresent()) {
        append(indent, SqlFormatter.formatName(argument.getName().get()));
        builder.append(" => ");
      } else {
        append(indent, "");
      }
      Node value = argument.getValue();
      if (value instanceof Expression) {
        builder.append(SqlFormatter.formatExpression((Expression) value));
      } else {
        process(value, indent + 1);
      }
      if (i < arguments.size() - 1) {
        builder.append(",\n");
      }
    }
  }
}
