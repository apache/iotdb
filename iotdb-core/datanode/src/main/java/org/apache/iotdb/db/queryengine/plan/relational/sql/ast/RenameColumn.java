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

import org.apache.iotdb.db.exception.sql.SemanticException;

import com.google.common.collect.ImmutableList;
import org.apache.tsfile.utils.RamUsageEstimator;

import java.util.List;
import java.util.Objects;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

public final class RenameColumn extends Statement {
  private static final long INSTANCE_SIZE =
      RamUsageEstimator.shallowSizeOfInstance(RenameColumn.class);

  private final QualifiedName table;
  private final Identifier source;
  private final Identifier target;

  private final boolean tableIfExists;
  private final boolean columnIfNotExists;
  private final boolean view;

  public RenameColumn(
      final NodeLocation location,
      final QualifiedName table,
      final Identifier source,
      final Identifier target,
      final boolean tableIfExists,
      final boolean columnIfNotExists,
      final boolean view) {
    super(requireNonNull(location, "location is null"));
    this.table = requireNonNull(table, "table is null");
    this.source = requireNonNull(source, "source is null");
    this.target = requireNonNull(target, "target is null");
    this.tableIfExists = tableIfExists;
    this.columnIfNotExists = columnIfNotExists;
    this.view = view;
    if (!view) {
      throw new SemanticException("The renaming for base table column is currently unsupported");
    }
  }

  public QualifiedName getTable() {
    return table;
  }

  public Identifier getSource() {
    return source;
  }

  public Identifier getTarget() {
    return target;
  }

  public boolean tableIfExists() {
    return tableIfExists;
  }

  public boolean columnIfExists() {
    return columnIfNotExists;
  }

  public boolean isView() {
    return view;
  }

  @Override
  public <R, C> R accept(final AstVisitor<R, C> visitor, final C context) {
    return visitor.visitRenameColumn(this, context);
  }

  @Override
  public List<Node> getChildren() {
    return ImmutableList.of();
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    final RenameColumn that = (RenameColumn) o;
    return tableIfExists == that.tableIfExists
        && columnIfNotExists == that.columnIfNotExists
        && Objects.equals(table, that.table)
        && Objects.equals(source, that.source)
        && Objects.equals(target, that.target)
        && view == that.view;
  }

  @Override
  public int hashCode() {
    return Objects.hash(table, source, target, view);
  }

  @Override
  public String toString() {
    return toStringHelper(this)
        .add("table", table)
        .add("source", source)
        .add("target", target)
        .add("tableIfExists", tableIfExists)
        .add("columnIfExists", columnIfNotExists)
        .add("view", view)
        .toString();
  }

  @Override
  public long ramBytesUsed() {
    long size = INSTANCE_SIZE;
    size += AstMemoryEstimationHelper.getEstimatedSizeOfNodeLocation(getLocationInternal());
    size += table == null ? 0L : table.ramBytesUsed();
    size += AstMemoryEstimationHelper.getEstimatedSizeOfAccountableObject(source);
    size += AstMemoryEstimationHelper.getEstimatedSizeOfAccountableObject(target);
    return size;
  }
}
