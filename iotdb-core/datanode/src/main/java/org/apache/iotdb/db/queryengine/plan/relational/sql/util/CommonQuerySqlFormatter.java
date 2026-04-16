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
import org.apache.iotdb.db.node_commons.plan.relational.sql.ast.AllColumns;
import org.apache.iotdb.db.node_commons.plan.relational.sql.ast.CommonQueryAstVisitor;
import org.apache.iotdb.db.node_commons.plan.relational.sql.ast.Expression;
import org.apache.iotdb.db.node_commons.plan.relational.sql.ast.Fill;
import org.apache.iotdb.db.node_commons.plan.relational.sql.ast.Identifier;
import org.apache.iotdb.db.node_commons.plan.relational.sql.ast.Join;
import org.apache.iotdb.db.node_commons.plan.relational.sql.ast.JoinCriteria;
import org.apache.iotdb.db.node_commons.plan.relational.sql.ast.Limit;
import org.apache.iotdb.db.node_commons.plan.relational.sql.ast.Node;
import org.apache.iotdb.db.node_commons.plan.relational.sql.ast.Offset;
import org.apache.iotdb.db.node_commons.plan.relational.sql.ast.OrderBy;
import org.apache.iotdb.db.node_commons.plan.relational.sql.ast.Query;
import org.apache.iotdb.db.node_commons.plan.relational.sql.ast.Relation;
import org.apache.iotdb.db.node_commons.plan.relational.sql.ast.Row;
import org.apache.iotdb.db.node_commons.plan.relational.sql.ast.SingleColumn;
import org.apache.iotdb.db.node_commons.plan.relational.sql.ast.TableSubquery;
import org.apache.iotdb.db.node_commons.plan.relational.sql.ast.Values;
import org.apache.iotdb.db.node_commons.plan.relational.sql.ast.WithQuery;
import org.apache.iotdb.db.node_commons.plan.statement.component.FillPolicy;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.JoinOn;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.JoinUsing;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.NaturalJoin;

import com.google.common.base.Joiner;
import com.google.common.base.Strings;

import java.util.Iterator;
import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.Iterables.getOnlyElement;
import static java.util.stream.Collectors.joining;
import static org.apache.iotdb.db.node_commons.plan.relational.sql.util.ExpressionFormatter.formatOrderBy;

public class CommonQuerySqlFormatter implements CommonQueryAstVisitor<Void, Integer> {

  protected final SqlFormatter.SqlBuilder builder;

  public CommonQuerySqlFormatter(StringBuilder builder) {
    this.builder = new SqlFormatter.SqlBuilder(builder);
  }

  @Override
  public Void visitNode(Node node, Integer indent) {
    throw new UnsupportedOperationException("not yet implemented: " + node);
  }

  @Override
  public Void visitExpression(Expression node, Integer indent) {
    checkArgument(indent == 0, "visitExpression should only be called at root");
    builder.append(SqlFormatter.formatExpression(node));
    return null;
  }

  @Override
  public Void visitQuery(Query node, Integer indent) {
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
                append(indent, SqlFormatter.formatName(query.getName()));
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
  public Void visitFill(Fill node, Integer indent) {
    append(indent, "FILL METHOD ").append(node.getFillMethod().name());

    if (node.getFillMethod() == FillPolicy.CONSTANT) {
      builder.append(SqlFormatter.formatExpression(node.getFillValue().get()));
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
  public Void visitOrderBy(OrderBy node, Integer indent) {
    append(indent, formatOrderBy(node)).append('\n');
    return null;
  }

  @Override
  public Void visitOffset(Offset node, Integer indent) {
    append(indent, "OFFSET ")
        .append(SqlFormatter.formatExpression(node.getRowCount()))
        .append(" ROWS\n");
    return null;
  }

  @Override
  public Void visitLimit(Limit node, Integer indent) {
    append(indent, "LIMIT ").append(SqlFormatter.formatExpression(node.getRowCount())).append('\n');
    return null;
  }

  @Override
  public Void visitSingleColumn(SingleColumn node, Integer indent) {
    builder.append(SqlFormatter.formatExpression(node.getExpression()));
    node.getAlias().ifPresent(alias -> builder.append(' ').append(SqlFormatter.formatName(alias)));
    return null;
  }

  @Override
  public Void visitAllColumns(AllColumns node, Integer indent) {
    node.getTarget()
        .ifPresent(value -> builder.append(SqlFormatter.formatExpression(value)).append("."));
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
  public Void visitJoin(Join node, Integer indent) {
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
        builder.append(" ON ").append(SqlFormatter.formatExpression(on.getExpression()));
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
  public Void visitAliasedRelation(AliasedRelation node, Integer indent) {
    processRelationSuffix(node.getRelation(), indent);

    builder.append(' ').append(SqlFormatter.formatName(node.getAlias()));
    appendAliasColumns(builder, node.getColumnNames());

    return null;
  }

  @Override
  public Void visitValues(Values node, Integer indent) {
    builder.append(" VALUES ");

    boolean first = true;
    for (Expression row : node.getRows()) {
      builder.append("\n").append(indentString(indent)).append(first ? "  " : ", ");
      builder.append(SqlFormatter.formatExpression(row));
      first = false;
    }
    builder.append('\n');

    return null;
  }

  @Override
  public Void visitTableSubquery(TableSubquery node, Integer indent) {
    builder.append('(').append('\n');

    process(node.getQuery(), indent + 1);

    append(indent, ") ");

    return null;
  }

  @Override
  public Void visitRow(Row node, Integer indent) {
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

  protected void processRelationSuffix(Relation relation, Integer indent) {
    if (needsParenthesesForRelationSuffix(relation)) {
      builder.append("( ");
      process(relation, indent + 1);
      append(indent, ")");
    } else {
      process(relation, indent);
    }
  }

  protected boolean needsParenthesesForRelationSuffix(Relation relation) {
    return relation instanceof AliasedRelation;
  }

  protected void processRelation(Relation relation, Integer indent) {
    if (isSimpleTableRelation(relation)) {
      builder.append("TABLE ").append(formatSimpleTableRelationName(relation)).append('\n');
    } else {
      process(relation, indent);
    }
  }

  protected boolean isSimpleTableRelation(Relation relation) {
    return false;
  }

  protected String formatSimpleTableRelationName(Relation relation) {
    throw new UnsupportedOperationException("unsupported relation: " + relation);
  }

  protected SqlFormatter.SqlBuilder append(int indent, String value) {
    return builder.append(indentString(indent)).append(value);
  }

  protected static String indentString(int indent) {
    return Strings.repeat(SqlFormatter.INDENT, indent);
  }

  protected void formatDefinitionList(List<String> elements, int indent) {
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

  protected void appendAliasColumns(SqlFormatter.SqlBuilder builder, List<Identifier> columns) {
    if ((columns != null) && !columns.isEmpty()) {
      String formattedColumns =
          columns.stream().map(SqlFormatter::formatName).collect(joining(", "));
      builder.append(" (").append(formattedColumns).append(')');
    }
  }
}
