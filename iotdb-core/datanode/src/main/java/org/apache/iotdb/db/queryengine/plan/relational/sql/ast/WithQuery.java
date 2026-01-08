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

import javax.annotation.Nullable;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

public class WithQuery extends Node {

  private static final long INSTANCE_SIZE =
      RamUsageEstimator.shallowSizeOfInstance(WithQuery.class);

  private final Identifier name;
  private final Query query;
  private final boolean materialized;

  @Nullable private final List<Identifier> columnNames;

  public WithQuery(Identifier name, Query query, boolean materialized) {
    super(null);
    this.name = name;
    this.query = requireNonNull(query, "query is null");
    this.columnNames = null;
    this.materialized = materialized;
  }

  public WithQuery(
      Identifier name, Query query, List<Identifier> columnNames, boolean materialized) {
    super(null);
    this.name = name;
    this.query = requireNonNull(query, "query is null");
    this.columnNames = requireNonNull(columnNames, "columnNames is null");
    this.materialized = materialized;
  }

  public WithQuery(NodeLocation location, Identifier name, Query query, boolean materialized) {
    super(requireNonNull(location, "location is null"));
    this.name = name;
    this.query = requireNonNull(query, "query is null");
    this.columnNames = null;
    this.materialized = materialized;
  }

  public WithQuery(
      NodeLocation location,
      Identifier name,
      Query query,
      List<Identifier> columnNames,
      boolean materialized) {
    super(requireNonNull(location, "location is null"));
    this.name = name;
    this.query = requireNonNull(query, "query is null");
    this.columnNames = requireNonNull(columnNames, "columnNames is null");
    this.materialized = materialized;
  }

  public Identifier getName() {
    return name;
  }

  public Query getQuery() {
    return query;
  }

  public Optional<List<Identifier>> getColumnNames() {
    return Optional.ofNullable(columnNames);
  }

  public boolean isMaterialized() {
    return materialized;
  }

  @Override
  public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
    return visitor.visitWithQuery(this, context);
  }

  @Override
  public List<Node> getChildren() {
    return ImmutableList.of(query);
  }

  @Override
  public String toString() {
    return toStringHelper(this)
        .add("name", name)
        .add("query", query)
        .add("columnNames", columnNames)
        .add("materialized", materialized)
        .omitNullValues()
        .toString();
  }

  @Override
  public int hashCode() {
    return Objects.hash(name, query, columnNames, materialized);
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if ((obj == null) || (getClass() != obj.getClass())) {
      return false;
    }
    WithQuery o = (WithQuery) obj;
    return Objects.equals(name, o.name)
        && Objects.equals(query, o.query)
        && Objects.equals(columnNames, o.columnNames)
        && Objects.equals(materialized, o.materialized);
  }

  @Override
  public boolean shallowEquals(Node other) {
    if (!sameClass(this, other)) {
      return false;
    }

    WithQuery otherRelation = (WithQuery) other;
    return name.equals(otherRelation.name)
        && Objects.equals(columnNames, otherRelation.columnNames);
  }

  @Override
  public long ramBytesUsed() {
    long size = INSTANCE_SIZE;
    size += AstMemoryEstimationHelper.getEstimatedSizeOfNodeLocation(getLocationInternal());
    size += AstMemoryEstimationHelper.getEstimatedSizeOfAccountableObject(name);
    size += AstMemoryEstimationHelper.getEstimatedSizeOfAccountableObject(query);
    size += AstMemoryEstimationHelper.getEstimatedSizeOfNodeList(columnNames);
    return size;
  }
}
