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

public class ShowStatement extends Statement {
  private final String tableName;

  private final Optional<Expression> where;
  private final Optional<OrderBy> orderBy;
  private final Optional<Offset> offset;
  private final Optional<Node> limit;

  public ShowStatement(
      NodeLocation location,
      String tableName,
      Optional<Expression> where,
      Optional<OrderBy> orderBy,
      Optional<Offset> offset,
      Optional<Node> limit) {
    super(requireNonNull(location, "location is null"));
    this.tableName = tableName;
    this.where = where;
    this.orderBy = orderBy;
    this.offset = offset;
    this.limit = limit;
  }

  public String getTableName() {
    return tableName;
  }

  public Optional<Expression> getWhere() {
    return where;
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
    return visitor.visitShowStatement(this, context);
  }

  @Override
  public List<Node> getChildren() {
    return ImmutableList.of();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    ShowStatement that = (ShowStatement) o;
    return Objects.equals(tableName, that.tableName)
        && Objects.equals(where, that.where)
        && Objects.equals(orderBy, that.orderBy)
        && Objects.equals(offset, that.offset)
        && Objects.equals(limit, that.limit);
  }

  @Override
  public int hashCode() {
    return Objects.hash(tableName, where, orderBy, offset, limit);
  }

  @Override
  public String toString() {
    return toStringHelper(this)
        .add("tableName", tableName)
        .add("where", where.orElse(null))
        .add("orderBy", orderBy)
        .add("offset", offset.orElse(null))
        .add("limit", limit.orElse(null))
        .omitNullValues()
        .toString();
  }
}
