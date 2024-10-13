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

import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.AliasedRelation;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.CoalesceExpression;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.ComparisonExpression;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.DereferenceExpression;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.Expression;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.Fill;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.FunctionCall;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.GroupBy;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.Identifier;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.LogicalExpression;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.Node;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.Offset;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.OrderBy;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.QualifiedName;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.Query;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.QueryBody;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.QuerySpecification;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.Relation;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.SearchedCaseExpression;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.Select;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.SelectItem;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.SingleColumn;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.SortItem;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.StringLiteral;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.Table;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.TableSubquery;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.WhenClause;

import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import static java.util.Arrays.asList;

public final class QueryUtil {
  private QueryUtil() {}

  public static Identifier identifier(String name) {
    return new Identifier(name);
  }

  public static Identifier quotedIdentifier(String name) {
    return new Identifier(name, true);
  }

  public static Expression nameReference(String first, String... rest) {
    return DereferenceExpression.from(QualifiedName.of(first, rest));
  }

  public static SelectItem unaliasedName(String name) {
    return new SingleColumn(identifier(name));
  }

  public static SelectItem aliasedName(String name, String alias) {
    return new SingleColumn(identifier(name), identifier(alias));
  }

  public static Select selectList(Expression... expressions) {
    return selectList(asList(expressions));
  }

  public static Select selectList(List<Expression> expressions) {
    ImmutableList.Builder<SelectItem> items = ImmutableList.builder();
    for (Expression expression : expressions) {
      items.add(new SingleColumn(expression));
    }
    return new Select(false, items.build());
  }

  public static Select selectList(SelectItem... items) {
    return new Select(false, ImmutableList.copyOf(items));
  }

  public static Select selectAll(List<SelectItem> items) {
    return new Select(false, items);
  }

  public static Table table(QualifiedName name) {
    return new Table(name);
  }

  public static Relation subquery(Query query) {
    return new TableSubquery(query);
  }

  public static SortItem ascending(String name) {
    return new SortItem(
        identifier(name), SortItem.Ordering.ASCENDING, SortItem.NullOrdering.UNDEFINED);
  }

  public static Expression logicalAnd(Expression left, Expression right) {
    return LogicalExpression.and(left, right);
  }

  public static Expression equal(Expression left, Expression right) {
    return new ComparisonExpression(ComparisonExpression.Operator.EQUAL, left, right);
  }

  public static Expression caseWhen(Expression operand, Expression result) {
    return new SearchedCaseExpression(ImmutableList.of(new WhenClause(operand, result)));
  }

  public static Expression functionCall(String name, Expression... arguments) {
    return new FunctionCall(QualifiedName.of(name), ImmutableList.copyOf(arguments));
  }

  public static Relation aliased(Relation relation, String alias) {
    return new AliasedRelation(relation, identifier(alias), null);
  }

  public static Relation aliased(Relation relation, String alias, List<String> columnAliases) {
    return new AliasedRelation(
        relation,
        identifier(alias),
        columnAliases.stream().map(QueryUtil::identifier).collect(Collectors.toList()));
  }

  public static SelectItem aliasedNullToEmpty(String column, String alias) {
    return new SingleColumn(
        new CoalesceExpression(identifier(column), new StringLiteral("")), identifier(alias));
  }

  public static OrderBy ordering(SortItem... items) {
    return new OrderBy(ImmutableList.copyOf(items));
  }

  public static Query simpleQuery(Select select) {
    return query(
        new QuerySpecification(
            select,
            Optional.empty(),
            Optional.empty(),
            Optional.empty(),
            Optional.empty(),
            Optional.empty(),
            Optional.empty(),
            Optional.empty(),
            Optional.empty()));
  }

  public static Query simpleQuery(Select select, Relation from) {
    return simpleQuery(select, from, Optional.empty(), Optional.empty());
  }

  public static Query simpleQuery(Select select, Relation from, OrderBy orderBy) {
    return simpleQuery(select, from, Optional.empty(), Optional.of(orderBy));
  }

  public static Query simpleQuery(Select select, Relation from, Expression where) {
    return simpleQuery(select, from, Optional.of(where), Optional.empty());
  }

  public static Query simpleQuery(Select select, Relation from, Expression where, OrderBy orderBy) {
    return simpleQuery(select, from, Optional.of(where), Optional.of(orderBy));
  }

  public static Query simpleQuery(
      Select select, Relation from, Optional<Expression> where, Optional<OrderBy> orderBy) {
    return simpleQuery(
        select,
        from,
        where,
        Optional.empty(),
        Optional.empty(),
        Optional.empty(),
        orderBy,
        Optional.empty(),
        Optional.empty());
  }

  public static Query simpleQuery(
      Select select,
      Relation from,
      Optional<Expression> where,
      Optional<GroupBy> groupBy,
      Optional<Expression> having,
      Optional<Fill> fill,
      Optional<OrderBy> orderBy,
      Optional<Offset> offset,
      Optional<Node> limit) {
    return query(
        new QuerySpecification(
            select, Optional.of(from), where, groupBy, having, fill, orderBy, offset, limit));
  }

  public static Query query(QueryBody body) {
    return new Query(
        Optional.empty(),
        body,
        Optional.empty(),
        Optional.empty(),
        Optional.empty(),
        Optional.empty());
  }
}
