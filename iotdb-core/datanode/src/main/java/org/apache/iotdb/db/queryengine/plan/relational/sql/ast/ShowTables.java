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

import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.AstMemoryEstimationHelper;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.IAstVisitor;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.Identifier;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.Node;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.NodeLocation;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.Statement;
import org.apache.iotdb.commons.schema.table.TableType;

import com.google.common.collect.ImmutableList;
import org.apache.tsfile.utils.RamUsageEstimator;

import javax.annotation.Nullable;

import java.util.Collections;
import java.util.EnumSet;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

public class ShowTables extends Statement {

  private static final long INSTANCE_SIZE =
      RamUsageEstimator.shallowSizeOfInstance(ShowTables.class);

  @Nullable private final Identifier dbName;

  private final boolean isDetails;

  @Nullable private final Set<TableType> tableTypeFilter;

  public ShowTables(final NodeLocation location, final boolean isDetails) {
    this(location, isDetails, null, null);
  }

  public ShowTables(final NodeLocation location, final Identifier dbName, final boolean isDetails) {
    this(location, isDetails, requireNonNull(dbName, "dbName is null"), null);
  }

  public ShowTables(
      final NodeLocation location, final boolean isDetails, final Set<TableType> tableTypeFilter) {
    this(location, isDetails, null, tableTypeFilter);
  }

  public ShowTables(
      final NodeLocation location,
      final Identifier dbName,
      final boolean isDetails,
      final Set<TableType> tableTypeFilter) {
    this(location, isDetails, requireNonNull(dbName, "dbName is null"), tableTypeFilter);
  }

  private ShowTables(
      final NodeLocation location,
      final boolean isDetails,
      @Nullable final Identifier dbName,
      @Nullable final Set<TableType> tableTypeFilter) {
    super(requireNonNull(location, "location is null"));
    this.dbName = dbName;
    this.isDetails = isDetails;
    this.tableTypeFilter =
        Objects.isNull(tableTypeFilter)
            ? null
            : Collections.unmodifiableSet(
                tableTypeFilter.isEmpty()
                    ? EnumSet.noneOf(TableType.class)
                    : EnumSet.copyOf(tableTypeFilter));
  }

  public Optional<Identifier> getDbName() {
    return Optional.ofNullable(dbName);
  }

  public boolean isDetails() {
    return isDetails;
  }

  public Optional<Set<TableType>> getTableTypeFilter() {
    return Optional.ofNullable(tableTypeFilter);
  }

  @Override
  public <R, C> R accept(final IAstVisitor<R, C> visitor, final C context) {
    return ((AstVisitor<R, C>) visitor).visitShowTables(this, context);
  }

  @Override
  public List<Node> getChildren() {
    return ImmutableList.of();
  }

  @Override
  public int hashCode() {
    return Objects.hash(dbName, isDetails, tableTypeFilter);
  }

  @Override
  public boolean equals(final Object obj) {
    if (this == obj) {
      return true;
    }
    if ((obj == null) || (getClass() != obj.getClass())) {
      return false;
    }
    final ShowTables o = (ShowTables) obj;
    return isDetails == o.isDetails
        && Objects.equals(dbName, o.dbName)
        && Objects.equals(tableTypeFilter, o.tableTypeFilter);
  }

  @Override
  public String toString() {
    return toStringHelper(this)
        .add("dbName", dbName)
        .add("isDetails", isDetails)
        .add("tableTypeFilter", tableTypeFilter)
        .toString();
  }

  @Override
  public long ramBytesUsed() {
    long size = INSTANCE_SIZE;
    size += AstMemoryEstimationHelper.getEstimatedSizeOfNodeLocation(getLocationInternal());
    size += AstMemoryEstimationHelper.getEstimatedSizeOfAccountableObject(dbName);
    return size;
  }
}
