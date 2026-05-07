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

package org.apache.iotdb.commons.queryengine.plan.relational.sql.util;

import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.AliasedRelation;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.AllColumns;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.CommonQueryAstVisitor;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.Except;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.Expression;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.Fill;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.Identifier;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.Intersect;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.Join;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.JoinCriteria;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.JoinOn;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.JoinUsing;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.Limit;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.NaturalJoin;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.Node;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.Offset;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.OrderBy;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.PatternRecognitionRelation;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.QualifiedName;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.Query;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.QuerySpecification;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.Relation;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.Row;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.RowPattern;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.Select;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.SingleColumn;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.Table;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.TableFunctionArgument;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.TableFunctionInvocation;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.TableFunctionTableArgument;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.TableSubquery;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.Union;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.Values;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.WithQuery;
import org.apache.iotdb.commons.queryengine.plan.statement.component.FillPolicy;

import com.google.common.base.Joiner;
import com.google.common.base.Strings;
import com.google.errorprone.annotations.CanIgnoreReturnValue;

import java.util.Iterator;
import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.Iterables.getOnlyElement;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.joining;
import static org.apache.iotdb.commons.queryengine.plan.relational.sql.util.ExpressionFormatter.formatOrderBy;
import static org.apache.iotdb.commons.queryengine.plan.relational.sql.util.RowPatternFormatter.formatPattern;

public class CommonQuerySqlFormatter implements CommonQueryAstVisitor<Void, Integer> {

  protected static final String INDENT = "   ";
  protected final SqlBuilder builder;

  public CommonQuerySqlFormatter(StringBuilder builder) {
    this.builder = new SqlBuilder(builder);
  }

  protected static String formatName(Identifier identifier) {
    return ExpressionFormatter.formatExpression(identifier);
  }

  public static String formatName(QualifiedName name) {
    return name.getOriginalParts().stream()
        .map(CommonQuerySqlFormatter::formatName)
        .collect(joining("."));
  }

  protected static String formatExpression(Expression expression) {
    return ExpressionFormatter.formatExpression(expression);
  }

  @Override
  public Void visitNode(Node node, Integer indent) {
    throw new UnsupportedOperationException("not yet implemented: " + node);
  }

  @Override
  public Void visitExpression(Expression node, Integer indent) {
    checkArgument(indent == 0, "visitExpression should only be called at root");
    builder.append(formatExpression(node));
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
  public Void visitFill(Fill node, Integer indent) {
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
                              .map(CommonQuerySqlFormatter::formatExpression)
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
                              .map(CommonQuerySqlFormatter::formatExpression)
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
    append(indent, "OFFSET ").append(formatExpression(node.getRowCount())).append(" ROWS\n");
    return null;
  }

  @Override
  public Void visitLimit(Limit node, Integer indent) {
    append(indent, "LIMIT ").append(formatExpression(node.getRowCount())).append('\n');
    return null;
  }

  @Override
  public Void visitSingleColumn(SingleColumn node, Integer indent) {
    builder.append(formatExpression(node.getExpression()));
    node.getAlias().ifPresent(alias -> builder.append(' ').append(formatName(alias)));
    return null;
  }

  @Override
  public Void visitAllColumns(AllColumns node, Integer indent) {
    node.getTarget().ifPresent(value -> builder.append(formatExpression(value)).append("."));
    builder.append("*");

    if (!node.getAliases().isEmpty()) {
      builder
          .append(" AS (")
          .append(
              Joiner.on(", ")
                  .join(
                      node.getAliases().stream()
                          .map(CommonQuerySqlFormatter::formatName)
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
  public Void visitAliasedRelation(AliasedRelation node, Integer indent) {
    processRelationSuffix(node.getRelation(), indent);

    builder.append(' ').append(formatName(node.getAlias()));
    appendAliasColumns(builder, node.getColumnNames());

    return null;
  }

  @Override
  public Void visitValues(Values node, Integer indent) {
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

  protected SqlBuilder append(int indent, String value) {
    return builder.append(indentString(indent)).append(value);
  }

  protected static String indentString(int indent) {
    return Strings.repeat(INDENT, indent);
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

  protected void appendAliasColumns(SqlBuilder builder, List<Identifier> columns) {
    if ((columns != null) && !columns.isEmpty()) {
      String formattedColumns =
          columns.stream().map(CommonQuerySqlFormatter::formatName).collect(joining(", "));
      builder.append(" (").append(formattedColumns).append(')');
    }
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
        .ifPresent(where -> append(indent, "WHERE " + formatExpression(where)).append('\n'));

    node.getGroupBy()
        .ifPresent(
            groupBy ->
                append(
                        indent,
                        "GROUP BY "
                            + (groupBy.isDistinct() ? " DISTINCT " : "")
                            + org.apache.iotdb.commons.queryengine.plan.relational.sql.util
                                .ExpressionFormatter.formatGroupBy(groupBy.getGroupingElements()))
                    .append('\n'));

    node.getHaving()
        .ifPresent(having -> append(indent, "HAVING " + formatExpression(having)).append('\n'));

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
      for (org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.SelectItem item :
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
    builder.append(formatName(node.getName()));
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
      builder.append(" AS ").append(formatName(aliasedRelation.getAlias()));
      appendAliasColumns(builder, aliasedRelation.getColumnNames());
    }
    if (node.getPartitionBy().isPresent()) {
      builder.append("\n");
      append(indent, "PARTITION BY ")
          .append(
              node.getPartitionBy().get().stream()
                  .map(CommonQuerySqlFormatter::formatExpression)
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

  private void appendTableFunctionInvocation(TableFunctionInvocation node, Integer indent) {
    builder.append(formatName(node.getName())).append("(\n");
    appendTableFunctionArguments(node.getArguments(), indent + 1);
    builder.append(")");
  }

  private void appendTableFunctionArguments(List<TableFunctionArgument> arguments, int indent) {
    for (int i = 0; i < arguments.size(); i++) {
      TableFunctionArgument argument = arguments.get(i);
      if (argument.getName().isPresent()) {
        append(indent, formatName(argument.getName().get()));
        builder.append(" => ");
      } else {
        append(indent, "");
      }
      Node value = argument.getValue();
      if (value instanceof Expression) {
        builder.append(formatExpression((Expression) value));
      } else {
        process(value, indent + 1);
      }
      if (i < arguments.size() - 1) {
        builder.append(",\n");
      }
    }
  }

  protected static class SqlBuilder {
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

  @Override
  public Void visitPatternRecognitionRelation(PatternRecognitionRelation node, Integer indent) {
    processRelationSuffix(node.getInput(), indent);

    builder.append(" MATCH_RECOGNIZE (\n");
    if (!node.getPartitionBy().isEmpty()) {
      append(indent + 1, "PARTITION BY ")
          .append(
              node.getPartitionBy().stream()
                  .map(
                      org.apache.iotdb.commons.queryengine.plan.relational.sql.util
                              .ExpressionFormatter
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
                      formatExpression(measure.getExpression())
                          + " AS "
                          + formatExpression(measure.getName()))
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
                  + formatExpression(node.getAfterMatchSkipTo().get().getIdentifier().get());
          break;
        case FIRST:
          checkState(
              node.getAfterMatchSkipTo().get().getIdentifier().isPresent(),
              "missing identifier in AFTER MATCH SKIP TO FIRST");
          skipTo =
              "AFTER MATCH SKIP TO FIRST "
                  + formatExpression(node.getAfterMatchSkipTo().get().getIdentifier().get());
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
                      formatExpression(subset.getName())
                          + " = "
                          + subset.getIdentifiers().stream()
                              .map(
                                  org.apache.iotdb.commons.queryengine.plan.relational.sql.util
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
                    formatExpression(variable.getName())
                        + " AS "
                        + formatExpression(variable.getExpression()))
            .collect(com.google.common.collect.ImmutableList.toImmutableList()),
        indent + 2);

    builder.append(")");

    return null;
  }
}
