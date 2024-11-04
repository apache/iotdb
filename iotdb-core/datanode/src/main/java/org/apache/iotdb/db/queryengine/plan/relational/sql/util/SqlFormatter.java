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

import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.AddColumn;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.AliasedRelation;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.AllColumns;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.AlterPipe;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.AstVisitor;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.ColumnDefinition;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.CreateDB;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.CreateFunction;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.CreatePipe;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.CreatePipePlugin;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.CreateTable;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.Delete;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.DropColumn;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.DropDB;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.DropFunction;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.DropPipe;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.DropPipePlugin;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.DropTable;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.Except;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.Explain;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.ExplainAnalyze;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.Expression;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.Fill;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.Identifier;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.Insert;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.Intersect;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.Join;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.JoinCriteria;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.JoinOn;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.JoinUsing;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.Limit;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.LoadTsFile;
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
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.SetProperties;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.ShowClusterId;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.ShowCurrentDatabase;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.ShowCurrentSqlDialect;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.ShowCurrentTimestamp;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.ShowCurrentUser;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.ShowDB;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.ShowFunctions;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.ShowPipePlugins;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.ShowPipes;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.ShowTables;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.ShowVariables;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.ShowVersion;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.SingleColumn;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.StartPipe;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.StopPipe;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.Table;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.TableSubquery;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.Union;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.Update;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.UpdateAssignment;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.Values;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.WithQuery;
import org.apache.iotdb.db.queryengine.plan.statement.component.FillPolicy;

import com.google.common.base.Joiner;
import com.google.common.base.Strings;
import com.google.errorprone.annotations.CanIgnoreReturnValue;

import java.util.Iterator;
import java.util.List;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.Iterables.getOnlyElement;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.joining;
import static org.apache.iotdb.db.queryengine.plan.relational.sql.util.ExpressionFormatter.formatGroupBy;
import static org.apache.iotdb.db.queryengine.plan.relational.sql.util.ExpressionFormatter.formatOrderBy;

public final class SqlFormatter {

  private static final String INDENT = "   ";

  private SqlFormatter() {}

  public static String formatSql(Node root) {
    StringBuilder builder = new StringBuilder();
    new Formatter(builder).process(root, 0);
    return builder.toString();
  }

  private static String formatName(Identifier identifier) {
    return ExpressionFormatter.formatExpression(identifier);
  }

  public static String formatName(QualifiedName name) {
    return name.getOriginalParts().stream().map(SqlFormatter::formatName).collect(joining("."));
  }

  private static String formatExpression(Expression expression) {
    return ExpressionFormatter.formatExpression(expression);
  }

  private static class Formatter extends AstVisitor<Void, Integer> {
    private static class SqlBuilder {
      private final StringBuilder builder;

      public SqlBuilder(StringBuilder builder) {
        this.builder = requireNonNull(builder, "builder is null");
      }

      @CanIgnoreReturnValue
      public SqlBuilder append(CharSequence value) {
        builder.append(value);
        return this;
      }

      @CanIgnoreReturnValue
      public SqlBuilder append(char c) {
        builder.append(c);
        return this;
      }
    }

    private final SqlBuilder builder;

    public Formatter(StringBuilder builder) {
      this.builder = new SqlBuilder(builder);
    }

    @Override
    protected Void visitNode(Node node, Integer indent) {
      throw new UnsupportedOperationException("not yet implemented: " + node);
    }

    @Override
    protected Void visitExpression(Expression node, Integer indent) {
      checkArgument(indent == 0, "visitExpression should only be called at root");
      builder.append(formatExpression(node));
      return null;
    }

    @Override
    protected Void visitQuery(Query node, Integer indent) {

      node.getWith()
          .ifPresent(
              with -> {
                append(indent, "WITH");
                if (with.isRecursive()) {
                  builder.append(" RECURSIVE");
                }
                builder.append("\n  ");
                Iterator<WithQuery> queries = with.getQueries().iterator();
                while (queries.hasNext()) {
                  WithQuery query = queries.next();
                  append(indent, formatName(query.getName()));
                  query
                      .getColumnNames()
                      .ifPresent(columnNames -> appendAliasColumns(builder, columnNames));
                  builder.append(" AS ");
                  process(new TableSubquery(query.getQuery()), indent);
                  builder.append('\n');
                  if (queries.hasNext()) {
                    builder.append(", ");
                  }
                }
              });

      processRelation(node.getQueryBody(), indent);
      node.getOrderBy().ifPresent(orderBy -> process(orderBy, indent));
      node.getOffset().ifPresent(offset -> process(offset, indent));
      node.getLimit().ifPresent(limit -> process(limit, indent));
      return null;
    }

    @Override
    protected Void visitQuerySpecification(QuerySpecification node, Integer indent) {
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
          .ifPresent(where -> append(indent, "WHERE " + formatExpression(where)).append('\n'));

      node.getGroupBy()
          .ifPresent(
              groupBy ->
                  append(
                          indent,
                          "GROUP BY "
                              + (groupBy.isDistinct() ? " DISTINCT " : "")
                              + formatGroupBy(groupBy.getGroupingElements()))
                      .append('\n'));

      node.getHaving()
          .ifPresent(having -> append(indent, "HAVING " + formatExpression(having)).append('\n'));

      node.getOrderBy().ifPresent(orderBy -> process(orderBy, indent));
      node.getOffset().ifPresent(offset -> process(offset, indent));
      node.getLimit().ifPresent(limit -> process(limit, indent));
      return null;
    }

    @Override
    protected Void visitFill(Fill node, Integer indent) {
      append(indent, "FILL METHOD ").append(node.getFillMethod().name());

      if (node.getFillMethod() == FillPolicy.CONSTANT) {
        builder.append(formatExpression(node.getFillValue().get()));
      } else if (node.getFillMethod() == FillPolicy.LINEAR) {
        node.getTimeColumnIndex()
            .ifPresent(index -> builder.append(" TIME_COLUMN ").append(String.valueOf(index)));
        node.getFillGroupingElements()
            .ifPresent(
                elements ->
                    builder
                        .append(" FILL_GROUP ")
                        .append(
                            elements.stream()
                                .map(SqlFormatter::formatExpression)
                                .collect(joining(", "))));
      } else if (node.getFillMethod() == FillPolicy.PREVIOUS) {
        node.getTimeBound()
            .ifPresent(timeBound -> builder.append(" TIME_BOUND ").append(timeBound.toString()));
        node.getTimeColumnIndex()
            .ifPresent(index -> builder.append(" TIME_COLUMN ").append(String.valueOf(index)));
        node.getFillGroupingElements()
            .ifPresent(
                elements ->
                    builder
                        .append(" FILL_GROUP ")
                        .append(
                            elements.stream()
                                .map(SqlFormatter::formatExpression)
                                .collect(joining(", "))));
      } else {
        throw new IllegalArgumentException("Unknown fill method: " + node.getFillMethod());
      }
      return null;
    }

    @Override
    protected Void visitOrderBy(OrderBy node, Integer indent) {
      append(indent, formatOrderBy(node)).append('\n');
      return null;
    }

    @Override
    protected Void visitOffset(Offset node, Integer indent) {
      append(indent, "OFFSET ").append(formatExpression(node.getRowCount())).append(" ROWS\n");
      return null;
    }

    @Override
    protected Void visitLimit(Limit node, Integer indent) {
      append(indent, "LIMIT ").append(formatExpression(node.getRowCount())).append('\n');
      return null;
    }

    @Override
    protected Void visitSelect(Select node, Integer indent) {
      append(indent, "SELECT");
      if (node.isDistinct()) {
        builder.append(" DISTINCT");
      }

      if (node.getSelectItems().size() > 1) {
        boolean first = true;
        for (SelectItem item : node.getSelectItems()) {
          builder.append("\n").append(indentString(indent)).append(first ? "  " : ", ");

          process(item, indent);
          first = false;
        }
      } else {
        builder.append(' ');
        process(getOnlyElement(node.getSelectItems()), indent);
      }

      builder.append('\n');

      return null;
    }

    @Override
    protected Void visitSingleColumn(SingleColumn node, Integer indent) {
      builder.append(formatExpression(node.getExpression()));
      node.getAlias().ifPresent(alias -> builder.append(' ').append(formatName(alias)));

      return null;
    }

    @Override
    protected Void visitAllColumns(AllColumns node, Integer indent) {
      node.getTarget().ifPresent(value -> builder.append(formatExpression(value)).append("."));
      builder.append("*");

      if (!node.getAliases().isEmpty()) {
        builder
            .append(" AS (")
            .append(
                Joiner.on(", ")
                    .join(
                        node.getAliases().stream()
                            .map(SqlFormatter::formatName)
                            .collect(toImmutableList())))
            .append(")");
      }

      return null;
    }

    @Override
    protected Void visitTable(Table node, Integer indent) {
      builder.append(formatName(node.getName()));
      return null;
    }

    @Override
    protected Void visitJoin(Join node, Integer indent) {
      JoinCriteria criteria = node.getCriteria().orElse(null);
      String type = node.getType().toString();
      if (criteria instanceof NaturalJoin) {
        type = "NATURAL " + type;
      }

      if (node.getType() != Join.Type.IMPLICIT) {
        builder.append('(');
      }
      process(node.getLeft(), indent);

      builder.append('\n');
      if (node.getType() == Join.Type.IMPLICIT) {
        append(indent, ", ");
      } else {
        append(indent, type).append(" JOIN ");
      }

      process(node.getRight(), indent);

      if (node.getType() != Join.Type.CROSS && node.getType() != Join.Type.IMPLICIT) {
        if (criteria instanceof JoinUsing) {
          JoinUsing using = (JoinUsing) criteria;
          builder.append(" USING (").append(Joiner.on(", ").join(using.getColumns())).append(")");
        } else if (criteria instanceof JoinOn) {
          JoinOn on = (JoinOn) criteria;
          builder.append(" ON ").append(formatExpression(on.getExpression()));
        } else if (!(criteria instanceof NaturalJoin)) {
          throw new UnsupportedOperationException("unknown join criteria: " + criteria);
        }
      }

      if (node.getType() != Join.Type.IMPLICIT) {
        builder.append(")");
      }

      return null;
    }

    @Override
    protected Void visitAliasedRelation(AliasedRelation node, Integer indent) {
      builder.append("( ");
      process(node, indent + 1);
      append(indent, ")");

      builder.append(' ').append(formatName(node.getAlias()));
      appendAliasColumns(builder, node.getColumnNames());

      return null;
    }

    @Override
    protected Void visitValues(Values node, Integer indent) {
      builder.append(" VALUES ");

      boolean first = true;
      for (Expression row : node.getRows()) {
        builder.append("\n").append(indentString(indent)).append(first ? "  " : ", ");

        builder.append(formatExpression(row));
        first = false;
      }
      builder.append('\n');

      return null;
    }

    @Override
    protected Void visitTableSubquery(TableSubquery node, Integer indent) {
      builder.append('(').append('\n');

      process(node.getQuery(), indent + 1);

      append(indent, ") ");

      return null;
    }

    @Override
    protected Void visitUnion(Union node, Integer indent) {
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
    protected Void visitExcept(Except node, Integer indent) {
      processRelation(node.getLeft(), indent);

      builder.append("EXCEPT ");
      if (!node.isDistinct()) {
        builder.append("ALL ");
      }

      processRelation(node.getRight(), indent);

      return null;
    }

    @Override
    protected Void visitIntersect(Intersect node, Integer indent) {
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
    protected Void visitExplain(Explain node, Integer indent) {
      builder.append("EXPLAIN ");

      builder.append("\n");

      process(node.getStatement(), indent);

      return null;
    }

    @Override
    protected Void visitExplainAnalyze(ExplainAnalyze node, Integer indent) {
      builder.append("EXPLAIN ANALYZE");
      if (node.isVerbose()) {
        builder.append(" VERBOSE");
      }
      builder.append("\n");

      process(node.getStatement(), indent);

      return null;
    }

    @Override
    protected Void visitShowDB(ShowDB node, Integer indent) {
      builder.append("SHOW DATABASE");

      return null;
    }

    @Override
    protected Void visitShowTables(final ShowTables node, final Integer indent) {
      builder.append("SHOW TABLES");

      if (node.isDetails()) {
        builder.append(" DETAILS");
      }

      node.getDbName().ifPresent(db -> builder.append(" FROM ").append(formatName(db)));

      return null;
    }

    @Override
    protected Void visitShowFunctions(ShowFunctions node, Integer indent) {
      builder.append("SHOW FUNCTIONS");

      return null;
    }

    @Override
    protected Void visitShowCurrentSqlDialect(ShowCurrentSqlDialect node, Integer context) {
      builder.append(node.toString());
      return null;
    }

    @Override
    protected Void visitShowCurrentDatabase(ShowCurrentDatabase node, Integer context) {
      builder.append(node.toString());
      return null;
    }

    @Override
    protected Void visitShowCurrentUser(ShowCurrentUser node, Integer context) {
      builder.append(node.toString());
      return null;
    }

    @Override
    protected Void visitShowVersion(ShowVersion node, Integer context) {
      builder.append(node.toString());
      return null;
    }

    @Override
    protected Void visitShowVariables(ShowVariables node, Integer context) {
      builder.append(node.toString());
      return null;
    }

    @Override
    protected Void visitShowClusterId(ShowClusterId node, Integer context) {
      builder.append(node.toString());
      return null;
    }

    @Override
    protected Void visitShowCurrentTimestamp(ShowCurrentTimestamp node, Integer context) {
      builder.append(node.toString());
      return null;
    }

    @Override
    protected Void visitDelete(Delete node, Integer indent) {
      builder.append("DELETE FROM ").append(formatName(node.getTable().getName()));

      node.getWhere().ifPresent(where -> builder.append(" WHERE ").append(formatExpression(where)));

      return null;
    }

    @Override
    protected Void visitCreateDB(CreateDB node, Integer indent) {
      builder.append("CREATE DATABASE ");
      if (node.isSetIfNotExists()) {
        builder.append("IF NOT EXISTS ");
      }
      builder.append(node.getDbName()).append(" ");

      builder.append(formatPropertiesMultiLine(node.getProperties()));

      return null;
    }

    @Override
    protected Void visitDropDB(DropDB node, Integer indent) {
      builder.append("DROP DATABASE ");
      if (node.isExists()) {
        builder.append("IF EXISTS ");
      }
      builder.append(formatName(node.getDbName()));
      return null;
    }

    @Override
    protected Void visitCreateTable(final CreateTable node, final Integer indent) {
      builder.append("CREATE TABLE ");
      if (node.isIfNotExists()) {
        builder.append("IF NOT EXISTS ");
      }
      final String tableName = formatName(node.getName());
      builder.append(tableName).append(" (\n");

      final String elementIndent = indentString(indent + 1);
      final String columnList =
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

      builder.append(formatPropertiesMultiLine(node.getProperties()));

      return null;
    }

    private String formatPropertiesMultiLine(List<Property> properties) {
      if (properties.isEmpty()) {
        return "";
      }

      String propertyList =
          properties.stream()
              .map(
                  element ->
                      INDENT
                          + formatName(element.getName())
                          + " = "
                          + (element.isSetToDefault()
                              ? "DEFAULT"
                              : formatExpression(element.getNonDefaultValue())))
              .collect(joining(",\n"));

      return "\nWITH (\n" + propertyList + "\n)";
    }

    private String formatPropertiesSingleLine(List<Property> properties) {
      if (properties.isEmpty()) {
        return "";
      }

      return " WITH ( " + joinProperties(properties) + " )";
    }

    private String formatColumnDefinition(ColumnDefinition column) {
      StringBuilder stringBuilder =
          new StringBuilder()
              .append(formatName(column.getName()))
              .append(" ")
              .append(column.getType())
              .append(" ")
              .append(column.getColumnCategory());

      column
          .getCharsetName()
          .ifPresent(charset -> stringBuilder.append(" CHARSET ").append(charset));
      return stringBuilder.toString();
    }

    @Override
    protected Void visitDropTable(final DropTable node, final Integer indent) {
      builder.append("DROP TABLE ");
      if (node.isExists()) {
        builder.append("IF EXISTS ");
      }
      builder.append(formatName(node.getTableName()));

      return null;
    }

    @Override
    protected Void visitRenameTable(final RenameTable node, final Integer indent) {
      builder.append("ALTER TABLE ");
      if (node.tableIfExists()) {
        builder.append("IF EXISTS ");
      }

      builder
          .append(formatName(node.getSource()))
          .append(" RENAME TO ")
          .append(formatName(node.getTarget()));

      return null;
    }

    @Override
    protected Void visitSetProperties(final SetProperties node, final Integer context) {
      final SetProperties.Type type = node.getType();
      builder.append("ALTER ");
      switch (type) {
        case TABLE:
          builder.append("TABLE ");
        case MATERIALIZED_VIEW:
          builder.append("MATERIALIZED VIEW ");
      }
      if (node.ifExists()) {
        builder.append("IF EXISTS ");
      }

      builder
          .append(formatName(node.getName()))
          .append(" SET PROPERTIES ")
          .append(joinProperties(node.getProperties()));

      return null;
    }

    private String joinProperties(List<Property> properties) {
      return properties.stream()
          .map(
              element ->
                  formatName(element.getName())
                      + " = "
                      + (element.isSetToDefault()
                          ? "DEFAULT"
                          : formatExpression(element.getNonDefaultValue())))
          .collect(joining(", "));
    }

    @Override
    protected Void visitRenameColumn(RenameColumn node, Integer indent) {
      builder.append("ALTER TABLE ");
      if (node.tableIfExists()) {
        builder.append("IF EXISTS ");
      }

      builder.append(formatName(node.getTable())).append(" RENAME COLUMN ");
      if (node.columnIfExists()) {
        builder.append("IF EXISTS ");
      }

      builder
          .append(formatName(node.getSource()))
          .append(" TO ")
          .append(formatName(node.getTarget()));

      return null;
    }

    @Override
    protected Void visitDropColumn(final DropColumn node, final Integer indent) {
      builder.append("ALTER TABLE ");
      if (node.tableIfExists()) {
        builder.append("IF EXISTS ");
      }

      builder.append(formatName(node.getTable())).append(" DROP COLUMN ");
      if (node.columnIfExists()) {
        builder.append("IF NOT EXISTS ");
      }

      builder.append(formatName(node.getField()));

      return null;
    }

    @Override
    protected Void visitAddColumn(final AddColumn node, final Integer indent) {
      builder.append("ALTER TABLE ");
      if (node.tableIfExists()) {
        builder.append("IF EXISTS ");
      }

      builder.append(formatName(node.getTableName())).append(" ADD COLUMN ");
      if (node.columnIfNotExists()) {
        builder.append("IF NOT EXISTS ");
      }

      builder.append(formatColumnDefinition(node.getColumn()));

      return null;
    }

    @Override
    protected Void visitInsert(Insert node, Integer indent) {
      builder.append("INSERT INTO ").append(formatName(node.getTarget()));

      node.getColumns()
          .ifPresent(
              columns -> builder.append(" (").append(Joiner.on(", ").join(columns)).append(")"));

      builder.append("\n");

      process(node.getQuery(), indent);

      return null;
    }

    @Override
    protected Void visitUpdate(Update node, Integer indent) {
      builder.append("UPDATE ").append(formatName(node.getTable().getName())).append(" SET");
      int setCounter = node.getAssignments().size() - 1;
      for (UpdateAssignment assignment : node.getAssignments()) {
        builder
            .append("\n")
            .append(indentString(indent + 1))
            .append(((Identifier) assignment.getName()).getValue())
            .append(" = ")
            .append(formatExpression(assignment.getValue()));
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
                      .append(formatExpression(where)));
      return null;
    }

    @Override
    protected Void visitRow(Row node, Integer indent) {
      builder.append("ROW(");
      boolean firstItem = true;
      for (Expression item : node.getItems()) {
        if (!firstItem) {
          builder.append(", ");
        }
        process(item, indent);
        firstItem = false;
      }
      builder.append(")");
      return null;
    }

    @Override
    protected Void visitCreateFunction(CreateFunction node, Integer indent) {
      builder
          .append("CREATE FUNCTION ")
          .append(node.getUdfName())
          .append(" AS ")
          .append(node.getClassName());
      node.getUriString().ifPresent(uri -> builder.append(" USING URI ").append(uri));
      return null;
    }

    @Override
    protected Void visitDropFunction(DropFunction node, Integer indent) {
      builder.append("DROP FUNCTION ");
      builder.append(node.getUdfName());
      return null;
    }

    @Override
    protected Void visitLoadTsFile(final LoadTsFile node, final Integer indent) {
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
    protected Void visitCreatePipe(CreatePipe node, Integer context) {
      builder.append("CREATE PIPE ");
      if (node.hasIfNotExistsCondition()) {
        builder.append("IF NOT EXISTS ");
      }
      builder.append(node.getPipeName());
      builder.append(" \n");

      if (!node.getExtractorAttributes().isEmpty()) {
        builder
            .append("WITH SOURCE (")
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

      if (!node.getConnectorAttributes().isEmpty()) {
        builder
            .append("WITH SINK (")
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
      }

      return null;
    }

    @Override
    protected Void visitAlterPipe(AlterPipe node, Integer context) {
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
    protected Void visitDropPipe(DropPipe node, Integer context) {
      builder.append("DROP PIPE ");
      if (node.hasIfExistsCondition()) {
        builder.append("IF EXISTS ");
      }
      builder.append(node.getPipeName());

      return null;
    }

    @Override
    protected Void visitStartPipe(StartPipe node, Integer context) {
      builder.append("START PIPE ").append(node.getPipeName());

      return null;
    }

    @Override
    protected Void visitStopPipe(StopPipe node, Integer context) {
      builder.append("STOP PIPE ").append(node.getPipeName());

      return null;
    }

    @Override
    protected Void visitShowPipes(ShowPipes node, Integer context) {
      builder.append("SHOW PIPES");

      return null;
    }

    @Override
    protected Void visitCreatePipePlugin(CreatePipePlugin node, Integer context) {
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
    protected Void visitDropPipePlugin(DropPipePlugin node, Integer context) {
      builder.append("DROP PIPEPLUGIN ");
      if (node.hasIfExistsCondition()) {
        builder.append("IF EXISTS ");
      }
      builder.append(node.getPluginName());

      return null;
    }

    @Override
    protected Void visitShowPipePlugins(ShowPipePlugins node, Integer context) {
      builder.append("SHOW PIPEPLUGINS");

      return null;
    }

    private void appendBeginLabel(Optional<Identifier> label) {
      label.ifPresent(value -> builder.append(formatName(value)).append(": "));
    }

    private void processRelation(Relation relation, Integer indent) {
      // TODO: handle this properly
      if (relation instanceof Table) {
        builder.append("TABLE ").append(formatName(((Table) relation).getName())).append('\n');
      } else {
        process(relation, indent);
      }
    }

    private SqlBuilder append(int indent, String value) {
      return builder.append(indentString(indent)).append(value);
    }

    private static String indentString(int indent) {
      return Strings.repeat(INDENT, indent);
    }

    private void formatDefinitionList(List<String> elements, int indent) {
      if (elements.size() == 1) {
        builder.append(" ").append(getOnlyElement(elements)).append("\n");
      } else {
        builder.append("\n");
        for (int i = 0; i < elements.size() - 1; i++) {
          append(indent, elements.get(i)).append(",\n");
        }
        append(indent, elements.get(elements.size() - 1)).append("\n");
      }
    }

    private void appendAliasColumns(Formatter.SqlBuilder builder, List<Identifier> columns) {
      if ((columns != null) && !columns.isEmpty()) {
        String formattedColumns =
            columns.stream().map(SqlFormatter::formatName).collect(joining(", "));

        builder.append(" (").append(formattedColumns).append(')');
      }
    }
  }
}
