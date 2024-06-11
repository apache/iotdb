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

package org.apache.iotdb.db.queryengine.plan.relational.sql.tree;

import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.auth.entity.PrivilegeType;
import org.apache.iotdb.db.auth.AuthorityChecker;
import org.apache.iotdb.db.exception.sql.SemanticException;
import org.apache.iotdb.rpc.TSStatusCode;

import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

public class Query extends Statement {

  private final Optional<With> with;
  private final QueryBody queryBody;
  private final Optional<OrderBy> orderBy;
  private final Optional<Offset> offset;
  private final Optional<Node> limit;

  public Query(
      Optional<With> with,
      QueryBody queryBody,
      Optional<OrderBy> orderBy,
      Optional<Offset> offset,
      Optional<Node> limit) {
    this(null, with, queryBody, orderBy, offset, limit);
  }

  public Query(
      NodeLocation location,
      Optional<With> with,
      QueryBody queryBody,
      Optional<OrderBy> orderBy,
      Optional<Offset> offset,
      Optional<Node> limit) {
    super(location);
    requireNonNull(with, "with is null");
    requireNonNull(queryBody, "queryBody is null");
    requireNonNull(orderBy, "orderBy is null");
    requireNonNull(offset, "offset is null");
    requireNonNull(limit, "limit is null");
    checkArgument(
        !limit.isPresent() || limit.get() instanceof Limit,
        "limit must be optional of either FetchFirst or Limit type");

    this.with = with;
    this.queryBody = queryBody;
    this.orderBy = orderBy;
    this.offset = offset;
    this.limit = limit;
  }

  public Optional<With> getWith() {
    return with;
  }

  public QueryBody getQueryBody() {
    return queryBody;
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
    return visitor.visitQuery(this, context);
  }

  @Override
  public TSStatus checkPermissionBeforeProcess(String userName, String databaseName) {
    if (AuthorityChecker.SUPER_USER.equals(userName)) {
      return new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode());
    }

    if (databaseName == null) {
      throw new SemanticException("unknown database");
    }
    String tableName = null;
    if (this.queryBody instanceof QuerySpecification) {
      QuerySpecification specification = (QuerySpecification) this.queryBody;
      if (specification.getFrom().get() instanceof Table) {
        Table table = (Table) specification.getFrom().get();
        tableName = table.getName().toString();
      }
    }

    if (null == tableName) {
      throw new SemanticException("unknown table");
    }
    return AuthorityChecker.getTSStatus(
        AuthorityChecker.checkDBPermision(userName, databaseName, PrivilegeType.READ_DATA.ordinal())
            || AuthorityChecker.checkDBPermision(
                userName, databaseName, PrivilegeType.WRITE_DATA.ordinal())
            || AuthorityChecker.checkTBPermission(
                userName, databaseName, tableName, PrivilegeType.WRITE_DATA.ordinal())
            || AuthorityChecker.checkTBPermission(
                userName, databaseName, tableName, PrivilegeType.READ_DATA.ordinal()),
        "NEED ONE PRIVILEGES OF DB OR TABLE");
  }

  @Override
  public List<Node> getChildren() {
    ImmutableList.Builder<Node> nodes = ImmutableList.builder();
    with.ifPresent(nodes::add);
    nodes.add(queryBody);
    orderBy.ifPresent(nodes::add);
    offset.ifPresent(nodes::add);
    limit.ifPresent(nodes::add);
    return nodes.build();
  }

  @Override
  public String toString() {
    return toStringHelper(this)
        .add("with", with.orElse(null))
        .add("queryBody", queryBody)
        .add("orderBy", orderBy)
        .add("offset", offset.orElse(null))
        .add("limit", limit.orElse(null))
        .omitNullValues()
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
    Query o = (Query) obj;
    return Objects.equals(with, o.with)
        && Objects.equals(queryBody, o.queryBody)
        && Objects.equals(orderBy, o.orderBy)
        && Objects.equals(offset, o.offset)
        && Objects.equals(limit, o.limit);
  }

  @Override
  public int hashCode() {
    return Objects.hash(with, queryBody, orderBy, offset, limit);
  }

  @Override
  public boolean shallowEquals(Node other) {
    return sameClass(this, other);
  }
}
