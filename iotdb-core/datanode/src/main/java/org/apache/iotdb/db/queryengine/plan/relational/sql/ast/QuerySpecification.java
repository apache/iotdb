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

package org.apache.iotdb.db.queryengine.plan.relational.sql.ast;

import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

public class QuerySpecification extends QueryBody {

  private final Select select;
  private final Optional<Relation> from;
  private final Optional<Expression> where;
  private final Optional<GroupBy> groupBy;
  private final Optional<Expression> having;
  private final Optional<Fill> fill;
  private final Optional<OrderBy> orderBy;
  private final Optional<Offset> offset;
  private final Optional<Node> limit;

  public QuerySpecification(
      Select select,
      Optional<Relation> from,
      Optional<Expression> where,
      Optional<GroupBy> groupBy,
      Optional<Expression> having,
      Optional<Fill> fill,
      Optional<OrderBy> orderBy,
      Optional<Offset> offset,
      Optional<Node> limit) {
    this(null, select, from, where, groupBy, having, fill, orderBy, offset, limit);
  }

  public QuerySpecification(
      NodeLocation location,
      Select select,
      Optional<Relation> from,
      Optional<Expression> where,
      Optional<GroupBy> groupBy,
      Optional<Expression> having,
      Optional<Fill> fill,
      Optional<OrderBy> orderBy,
      Optional<Offset> offset,
      Optional<Node> limit) {
    super(location);

    this.select = requireNonNull(select, "select is null");
    this.from = requireNonNull(from, "from is null");
    this.where = requireNonNull(where, "where is null");
    this.groupBy = requireNonNull(groupBy, "groupBy is null");
    this.having = requireNonNull(having, "having is null");
    this.fill = requireNonNull(fill, "fill is null");
    this.orderBy = requireNonNull(orderBy, "orderBy is null");
    this.offset = requireNonNull(offset, "offset is null");
    this.limit = requireNonNull(limit, "limit is null");
  }

  public Select getSelect() {
    return select;
  }

  public Optional<Relation> getFrom() {
    return from;
  }

  public Optional<Expression> getWhere() {
    return where;
  }

  public Optional<GroupBy> getGroupBy() {
    return groupBy;
  }

  public Optional<Expression> getHaving() {
    return having;
  }

  public Optional<Fill> getFill() {
    return fill;
  }

  public Optional<OrderBy> getOrderBy() {
    return orderBy;
  }

  public Optional<Offset> getOffset() {
    return offset;
  }

  public Optional<Node> getLimit() {
    return limit;
  }

  @Override
  public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
    return visitor.visitQuerySpecification(this, context);
  }

  @Override
  public List<Node> getChildren() {
    ImmutableList.Builder<Node> nodes = ImmutableList.builder();
    nodes.add(select);
    from.ifPresent(nodes::add);
    where.ifPresent(nodes::add);
    groupBy.ifPresent(nodes::add);
    having.ifPresent(nodes::add);
    fill.ifPresent(nodes::add);
    orderBy.ifPresent(nodes::add);
    offset.ifPresent(nodes::add);
    limit.ifPresent(nodes::add);
    return nodes.build();
  }

  @Override
  public String toString() {
    return toStringHelper(this)
        .add("select", select)
        .add("from", from)
        .add("where", where.orElse(null))
        .add("groupBy", groupBy)
        .add("having", having.orElse(null))
        .add("fill", fill.orElse(null))
        .add("orderBy", orderBy)
        .add("offset", offset.orElse(null))
        .add("limit", limit.orElse(null))
        .toString();
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if ((obj == null) || (getClass() != obj.getClass())) {
      return false;
    }
    QuerySpecification o = (QuerySpecification) obj;
    return Objects.equals(select, o.select)
        && Objects.equals(from, o.from)
        && Objects.equals(where, o.where)
        && Objects.equals(groupBy, o.groupBy)
        && Objects.equals(having, o.having)
        && Objects.equals(fill, o.fill)
        && Objects.equals(orderBy, o.orderBy)
        && Objects.equals(offset, o.offset)
        && Objects.equals(limit, o.limit);
  }

  @Override
  public int hashCode() {
    return Objects.hash(select, from, where, groupBy, having, fill, orderBy, offset, limit);
  }

  @Override
  public boolean shallowEquals(Node other) {
    return sameClass(this, other);
  }
}
