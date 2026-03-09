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

import org.apache.iotdb.common.rpc.thrift.TRegionReplicaSet;
import org.apache.iotdb.db.storageengine.dataregion.modification.TableDeletionEntry;

import com.google.common.collect.ImmutableList;
import org.apache.tsfile.utils.RamUsageEstimator;

import javax.annotation.Nullable;

import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

public class Delete extends Statement {

  private static final long INSTANCE_SIZE = RamUsageEstimator.shallowSizeOfInstance(Delete.class);

  private Table table;
  @Nullable private Expression where;

  // generated after analysis or pipe transfer
  private List<TableDeletionEntry> tableDeletionEntries;
  private String databaseName;
  private Collection<TRegionReplicaSet> replicaSets;

  public Delete(final Table table) {
    super(null);
    this.table = requireNonNull(table, "table is null");
  }

  public Delete(final NodeLocation location, final Table table) {
    super(requireNonNull(location, "location is null"));
    this.table = requireNonNull(table, "table is null");
    this.where = null;
  }

  public Delete(final NodeLocation location, final Table table, final Expression where) {
    super(requireNonNull(location, "location is null"));
    this.table = requireNonNull(table, "table is null");
    this.where = requireNonNull(where, "where is null");
  }

  public Table getTable() {
    return table;
  }

  public Optional<Expression> getWhere() {
    return Optional.ofNullable(where);
  }

  @Override
  public <R, C> R accept(final AstVisitor<R, C> visitor, final C context) {
    return visitor.visitDelete(this, context);
  }

  @Override
  public List<Node> getChildren() {
    final ImmutableList.Builder<Node> nodes = ImmutableList.builder();
    nodes.add(table);
    if (where != null) {
      nodes.add(where);
    }
    return nodes.build();
  }

  @Override
  public int hashCode() {
    return Objects.hash(table, where);
  }

  @Override
  public boolean equals(final Object obj) {
    if (this == obj) {
      return true;
    }
    if ((obj == null) || (getClass() != obj.getClass())) {
      return false;
    }
    final Delete o = (Delete) obj;
    return Objects.equals(table, o.table) && Objects.equals(where, o.where);
  }

  @Override
  public String toString() {
    return toStringHelper(this).add("table", table.getName()).add("where", where).toString();
  }

  public List<TableDeletionEntry> getTableDeletionEntries() {
    return tableDeletionEntries;
  }

  public void setTableDeletionEntries(final List<TableDeletionEntry> tableDeletionEntries) {
    this.tableDeletionEntries = tableDeletionEntries;
  }

  public String getDatabaseName() {
    return databaseName;
  }

  public void setDatabaseName(final String databaseName) {
    this.databaseName = databaseName;
  }

  public Collection<TRegionReplicaSet> getReplicaSets() {
    return replicaSets;
  }

  public void setReplicaSets(final Collection<TRegionReplicaSet> replicaSets) {
    this.replicaSets = replicaSets;
  }

  @Override
  public long ramBytesUsed() {
    long size = INSTANCE_SIZE;
    size += AstMemoryEstimationHelper.getEstimatedSizeOfNodeLocation(getLocationInternal());
    size += AstMemoryEstimationHelper.getEstimatedSizeOfAccountableObject(table);
    size += AstMemoryEstimationHelper.getEstimatedSizeOfAccountableObject(where);
    return size;
  }
}
