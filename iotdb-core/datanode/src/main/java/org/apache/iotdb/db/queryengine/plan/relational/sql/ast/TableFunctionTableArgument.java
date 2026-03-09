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
import org.apache.tsfile.utils.RamUsageEstimator;

import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;

import static java.util.Objects.requireNonNull;
import static org.apache.iotdb.db.queryengine.plan.relational.sql.util.ExpressionFormatter.formatSortItems;

public class TableFunctionTableArgument extends Node {
  private static final long INSTANCE_SIZE =
      RamUsageEstimator.shallowSizeOfInstance(TableFunctionTableArgument.class);

  private final Relation table;
  private final Optional<List<Expression>> partitionBy; // it is allowed to partition by empty list
  private Optional<OrderBy> orderBy;

  public TableFunctionTableArgument(
      NodeLocation location,
      Relation table,
      Optional<List<Expression>> partitionBy,
      Optional<OrderBy> orderBy) {
    super(location);
    this.table = requireNonNull(table, "table is null");
    this.partitionBy = requireNonNull(partitionBy, "partitionBy is null");
    this.orderBy = requireNonNull(orderBy, "orderBy is null");
  }

  public Relation getTable() {
    return table;
  }

  public Optional<List<Expression>> getPartitionBy() {
    return partitionBy;
  }

  public Optional<OrderBy> getOrderBy() {
    return orderBy;
  }

  public void updateOrderBy(OrderBy orderBy) {
    this.orderBy = Optional.of(orderBy);
  }

  @Override
  public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
    return visitor.visitTableArgument(this, context);
  }

  @Override
  public List<? extends Node> getChildren() {
    ImmutableList.Builder<Node> builder = ImmutableList.builder();
    builder.add(table);
    partitionBy.ifPresent(builder::addAll);
    orderBy.ifPresent(builder::add);

    return builder.build();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    TableFunctionTableArgument other = (TableFunctionTableArgument) o;
    return Objects.equals(table, other.table)
        && Objects.equals(partitionBy, other.partitionBy)
        && Objects.equals(orderBy, other.orderBy);
  }

  @Override
  public int hashCode() {
    return Objects.hash(table, partitionBy, orderBy);
  }

  @Override
  public String toString() {
    StringBuilder builder = new StringBuilder();
    builder.append(table);
    partitionBy.ifPresent(
        partitioning ->
            builder.append(
                partitioning.stream()
                    .map(Expression::toString)
                    .collect(Collectors.joining(", ", " PARTITION BY (", ")"))));
    orderBy.ifPresent(
        ordering ->
            builder
                .append(" ORDER BY (")
                .append(formatSortItems(ordering.getSortItems()))
                .append(")"));

    return builder.toString();
  }

  @Override
  public boolean shallowEquals(Node o) {
    return sameClass(this, o);
  }

  @Override
  public long ramBytesUsed() {
    long size = INSTANCE_SIZE;
    size += AstMemoryEstimationHelper.getEstimatedSizeOfNodeLocation(getLocationInternal());
    size += AstMemoryEstimationHelper.getEstimatedSizeOfAccountableObject(table);
    size += 2 * AstMemoryEstimationHelper.OPTIONAL_INSTANCE_SIZE;
    size += AstMemoryEstimationHelper.getEstimatedSizeOfNodeList(partitionBy.orElse(null));
    size += AstMemoryEstimationHelper.getEstimatedSizeOfAccountableObject(orderBy.orElse(null));
    return size;
  }
}
