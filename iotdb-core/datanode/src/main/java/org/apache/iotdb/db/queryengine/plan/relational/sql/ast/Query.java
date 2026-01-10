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

import org.apache.iotdb.db.utils.cte.CteDataStore;

import com.google.common.collect.ImmutableList;
import org.apache.tsfile.utils.RamUsageEstimator;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

public class Query extends Statement {

  private static final long INSTANCE_SIZE = RamUsageEstimator.shallowSizeOfInstance(Query.class);

  private final Optional<With> with;
  private final QueryBody queryBody;
  private final Optional<Fill> fill;
  private final Optional<OrderBy> orderBy;
  private final Optional<Offset> offset;
  private final Optional<Node> limit;
  // whether this query needs materialization
  private boolean materialized = false;
  // whether this query has ever been executed
  private boolean isExecuted = false;
  // materialization has been executed successfully if cteDataStore is not null
  private CteDataStore cteDataStore = null;

  public Query(
      Optional<With> with,
      QueryBody queryBody,
      Optional<Fill> fill,
      Optional<OrderBy> orderBy,
      Optional<Offset> offset,
      Optional<Node> limit) {
    this(null, with, queryBody, fill, orderBy, offset, limit);
  }

  public Query(
      NodeLocation location,
      Optional<With> with,
      QueryBody queryBody,
      Optional<Fill> fill,
      Optional<OrderBy> orderBy,
      Optional<Offset> offset,
      Optional<Node> limit) {
    super(location);
    requireNonNull(with, "with is null");
    requireNonNull(queryBody, "queryBody is null");
    requireNonNull(fill, "fill is null");
    requireNonNull(orderBy, "orderBy is null");
    requireNonNull(offset, "offset is null");
    requireNonNull(limit, "limit is null");
    checkArgument(
        !limit.isPresent() || limit.get() instanceof Limit,
        "limit must be optional of either FetchFirst or Limit type");

    this.with = with;
    this.queryBody = queryBody;
    this.fill = fill;
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

  public boolean isMaterialized() {
    return materialized;
  }

  public void setMaterialized(boolean materialized) {
    this.materialized = materialized;
  }

  public boolean isExecuted() {
    return isExecuted;
  }

  public void setExecuted(boolean executed) {
    isExecuted = executed;
  }

  public boolean isDone() {
    return cteDataStore != null;
  }

  public void setCteDataStore(CteDataStore cteDataStore) {
    this.cteDataStore = cteDataStore;
  }

  public CteDataStore getCteDataStore() {
    return this.cteDataStore;
  }

  @Override
  public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
    return visitor.visitQuery(this, context);
  }

  @Override
  public List<Node> getChildren() {
    ImmutableList.Builder<Node> nodes = ImmutableList.builder();
    with.ifPresent(nodes::add);
    nodes.add(queryBody);
    fill.ifPresent(nodes::add);
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
        .add("orderBy", fill.orElse(null))
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
        && Objects.equals(fill, o.fill)
        && Objects.equals(orderBy, o.orderBy)
        && Objects.equals(offset, o.offset)
        && Objects.equals(limit, o.limit);
  }

  @Override
  public int hashCode() {
    return Objects.hash(with, queryBody, fill, orderBy, offset, limit);
  }

  @Override
  public boolean shallowEquals(Node other) {
    return sameClass(this, other);
  }

  @Override
  public long ramBytesUsed() {
    long size = INSTANCE_SIZE;
    size += AstMemoryEstimationHelper.getEstimatedSizeOfNodeLocation(getLocationInternal());
    size += AstMemoryEstimationHelper.getEstimatedSizeOfAccountableObject(queryBody);
    size += 5 * AstMemoryEstimationHelper.OPTIONAL_INSTANCE_SIZE;
    size += AstMemoryEstimationHelper.getEstimatedSizeOfAccountableObject(with.orElse(null));
    size += AstMemoryEstimationHelper.getEstimatedSizeOfAccountableObject(fill.orElse(null));
    size += AstMemoryEstimationHelper.getEstimatedSizeOfAccountableObject(orderBy.orElse(null));
    size += AstMemoryEstimationHelper.getEstimatedSizeOfAccountableObject(offset.orElse(null));
    size += AstMemoryEstimationHelper.getEstimatedSizeOfAccountableObject(limit.orElse(null));
    return size;
  }
}
