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

package org.apache.iotdb.db.queryengine.plan.relational.analyzer.predicate.schema;

import org.apache.iotdb.commons.schema.filter.SchemaFilter;
import org.apache.iotdb.commons.schema.filter.impl.multichildren.AndFilter;
import org.apache.iotdb.commons.schema.filter.impl.multichildren.OrFilter;
import org.apache.iotdb.commons.schema.filter.impl.singlechild.AttributeFilter;
import org.apache.iotdb.commons.schema.filter.impl.singlechild.IdFilter;
import org.apache.iotdb.commons.schema.filter.impl.singlechild.NotFilter;
import org.apache.iotdb.commons.schema.filter.impl.values.ComparisonFilter;
import org.apache.iotdb.commons.schema.filter.impl.values.InFilter;
import org.apache.iotdb.commons.schema.filter.impl.values.LikeFilter;
import org.apache.iotdb.commons.schema.filter.impl.values.PreciseFilter;
import org.apache.iotdb.commons.schema.table.TsTable;
import org.apache.iotdb.commons.schema.table.column.TsTableColumnCategory;
import org.apache.iotdb.commons.schema.table.column.TsTableColumnSchema;
import org.apache.iotdb.db.queryengine.plan.relational.analyzer.predicate.PredicateVisitor;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.BetweenPredicate;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.ComparisonExpression;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.Expression;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.IfExpression;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.InListExpression;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.InPredicate;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.IsNotNullPredicate;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.IsNullPredicate;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.LikePredicate;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.Literal;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.LogicalExpression;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.NotExpression;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.NullIfExpression;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.SearchedCaseExpression;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.SimpleCaseExpression;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.StringLiteral;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.SymbolReference;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;

import static com.google.common.base.Preconditions.checkArgument;
import static org.apache.iotdb.db.queryengine.plan.expression.unary.LikeExpression.getEscapeCharacter;
import static org.apache.iotdb.db.queryengine.plan.relational.analyzer.predicate.PredicatePushIntoScanChecker.isSymbolReference;

/**
 * The {@link ConvertSchemaPredicateToFilterVisitor} will convert a predicate to {@link
 * SchemaFilter}. For the predicates which can not be converted, this will return {@code null}.
 * However, for IdDeterminedPredicate, this visitor shall never return {@code null}.
 */
public class ConvertSchemaPredicateToFilterVisitor
    extends PredicateVisitor<SchemaFilter, ConvertSchemaPredicateToFilterVisitor.Context> {

  @Override
  protected @Nullable SchemaFilter visitInPredicate(final InPredicate node, final Context context) {
    final Expression valueList = node.getValueList();
    checkArgument(valueList instanceof InListExpression);
    final List<Expression> values = ((InListExpression) valueList).getValues();
    for (final Expression value : values) {
      if (!(value instanceof Literal)) {
        return null;
      }
    }

    return wrapIdOrAttributeFilter(
        new InFilter(
            values.stream()
                .map(value -> ((StringLiteral) value).getValue())
                .collect(Collectors.toSet())),
        ((SymbolReference) node.getValue()).getName(),
        context);
  }

  @Override
  protected SchemaFilter visitIsNullPredicate(final IsNullPredicate node, final Context context) {
    return wrapIdOrAttributeFilter(
        new PreciseFilter((String) null), ((SymbolReference) node.getValue()).getName(), context);
  }

  @Override
  protected SchemaFilter visitIsNotNullPredicate(
      final IsNotNullPredicate node, final Context context) {
    return wrapIdOrAttributeFilter(
        new NotFilter(new PreciseFilter((String) null)),
        ((SymbolReference) node.getValue()).getName(),
        context);
  }

  @Override
  protected @Nullable SchemaFilter visitLikePredicate(
      final LikePredicate node, final Context context) {
    // TODO: Support stringLiteral like id/attr?
    if (!(node.getValue() instanceof SymbolReference)
        || !(node.getPattern() instanceof StringLiteral)) {
      return null;
    }
    return wrapIdOrAttributeFilter(
        new LikeFilter(
            (((StringLiteral) node.getPattern()).getValue()),
            node.getEscape().isPresent()
                ? getEscapeCharacter(((StringLiteral) node.getEscape().get()).getValue())
                : Optional.empty()),
        ((SymbolReference) node.getValue()).getName(),
        context);
  }

  @Override
  protected @Nullable SchemaFilter visitLogicalExpression(
      final LogicalExpression node, final Context context) {
    final List<SchemaFilter> children = new ArrayList<>();
    for (final Expression term : node.getTerms()) {
      final SchemaFilter childResult = term.accept(this, context);
      if (Objects.nonNull(childResult)) {
        children.add(childResult);
      } else {
        return null;
      }
    }
    return node.getOperator() == LogicalExpression.Operator.OR
        ? new OrFilter(children)
        : new AndFilter(children);
  }

  @Override
  protected @Nullable SchemaFilter visitNotExpression(
      final NotExpression node, final Context context) {
    final SchemaFilter result = node.getValue().accept(this, context);
    return Objects.nonNull(result) ? new NotFilter(result) : null;
  }

  @Override
  protected @Nullable SchemaFilter visitComparisonExpression(
      final ComparisonExpression node, final Context context) {
    final String columnName;
    final String value;
    final boolean isOrdered;
    if (node.getLeft() instanceof Literal) {
      value = ((StringLiteral) (node.getLeft())).getValue();
      checkArgument(isSymbolReference(node.getRight()));
      columnName = ((SymbolReference) (node.getRight())).getName();
      isOrdered = false;
    } else if (node.getRight() instanceof Literal) {
      value = ((StringLiteral) (node.getRight())).getValue();
      checkArgument(isSymbolReference(node.getLeft()));
      columnName = ((SymbolReference) (node.getLeft())).getName();
      isOrdered = true;
    } else {
      return null;
    }

    return wrapIdOrAttributeFilter(
        node.getOperator() == ComparisonExpression.Operator.EQUAL
            ? new PreciseFilter(value)
            : new ComparisonFilter(
                convertExpressionOperator2SchemaOperator(node.getOperator(), isOrdered), value),
        columnName,
        context);
  }

  private ComparisonFilter.Operator convertExpressionOperator2SchemaOperator(
      final ComparisonExpression.Operator operator, final boolean isOrdered) {
    switch (operator) {
      case NOT_EQUAL:
        return ComparisonFilter.Operator.NOT_EQUAL;
      case LESS_THAN:
        return isOrdered
            ? ComparisonFilter.Operator.LESS_THAN
            : ComparisonFilter.Operator.GREATER_THAN;
      case LESS_THAN_OR_EQUAL:
        return isOrdered
            ? ComparisonFilter.Operator.LESS_THAN_OR_EQUAL
            : ComparisonFilter.Operator.GREATER_THAN_OR_EQUAL;
      case GREATER_THAN:
        return isOrdered
            ? ComparisonFilter.Operator.GREATER_THAN
            : ComparisonFilter.Operator.LESS_THAN;
      case GREATER_THAN_OR_EQUAL:
        return isOrdered
            ? ComparisonFilter.Operator.GREATER_THAN_OR_EQUAL
            : ComparisonFilter.Operator.LESS_THAN_OR_EQUAL;
      default:
        throw new UnsupportedOperationException("Unsupported operator " + operator);
    }
  }

  @Override
  protected SchemaFilter visitSimpleCaseExpression(
      final SimpleCaseExpression node, final Context context) {
    return visitExpression(node, context);
  }

  @Override
  protected SchemaFilter visitSearchedCaseExpression(
      final SearchedCaseExpression node, final Context context) {
    return visitExpression(node, context);
  }

  @Override
  protected SchemaFilter visitIfExpression(final IfExpression node, final Context context) {
    return visitExpression(node, context);
  }

  @Override
  protected SchemaFilter visitNullIfExpression(final NullIfExpression node, final Context context) {
    return visitExpression(node, context);
  }

  @Override
  protected SchemaFilter visitBetweenPredicate(final BetweenPredicate node, final Context context) {
    return visitExpression(node, context);
  }

  private SchemaFilter wrapIdOrAttributeFilter(
      final SchemaFilter filter, final String columnName, final Context context) {
    return context
            .table
            .getColumnSchema(columnName)
            .getColumnCategory()
            .equals(TsTableColumnCategory.ID)
        ? new IdFilter(filter, context.idColumnIndexMap.get(columnName))
        : new AttributeFilter(filter, columnName);
  }

  public static class Context {

    private final TsTable table;
    private final Map<String, Integer> idColumnIndexMap;

    public Context(final TsTable table) {
      this.table = table;
      this.idColumnIndexMap = getIdColumnIndex(table);
    }

    private Map<String, Integer> getIdColumnIndex(final TsTable table) {
      Map<String, Integer> map = new HashMap<>();
      List<TsTableColumnSchema> columnSchemaList = table.getColumnList();
      int idIndex = 0;
      for (TsTableColumnSchema columnSchema : columnSchemaList) {
        if (columnSchema.getColumnCategory().equals(TsTableColumnCategory.ID)) {
          map.put(columnSchema.getColumnName(), idIndex);
          idIndex++;
        }
      }
      return map;
    }
  }
}
