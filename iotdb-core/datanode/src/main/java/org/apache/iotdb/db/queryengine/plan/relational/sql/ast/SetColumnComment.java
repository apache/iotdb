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

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

public class SetColumnComment extends Statement {
  private static final long INSTANCE_SIZE =
      RamUsageEstimator.shallowSizeOfInstance(SetColumnComment.class);
  private final QualifiedName table;
  private final Identifier field;

  private final boolean tableIfExists;
  private final boolean columnIfExists;
  @Nullable private final String comment;

  public SetColumnComment(
      final NodeLocation location,
      final QualifiedName table,
      final Identifier field,
      final boolean tableIfExists,
      final boolean columnIfExists,
      final @Nullable String comment) {
    super(requireNonNull(location, "location is null"));
    this.table = requireNonNull(table, "table is null");
    this.field = requireNonNull(field, "field is null");
    this.tableIfExists = tableIfExists;
    this.columnIfExists = columnIfExists;
    this.comment = comment;
  }

  public QualifiedName getTable() {
    return table;
  }

  public Identifier getField() {
    return field;
  }

  public boolean tableIfExists() {
    return tableIfExists;
  }

  public boolean columnIfExists() {
    return columnIfExists;
  }

  public String getComment() {
    return comment;
  }

  @Override
  public <R, C> R accept(final AstVisitor<R, C> visitor, final C context) {
    return visitor.visitSetColumnComment(this, context);
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
    final SetColumnComment that = (SetColumnComment) o;
    return tableIfExists == that.tableIfExists
        && columnIfExists == that.columnIfExists
        && Objects.equals(table, that.table)
        && Objects.equals(field, that.field)
        && Objects.equals(comment, that.comment);
  }

  @Override
  public int hashCode() {
    return Objects.hash(table, field, tableIfExists, columnIfExists, comment);
  }

  @Override
  public String toString() {
    return toStringHelper(this)
        .add("table", table)
        .add("field", field)
        .add("tableIfExists", tableIfExists)
        .add("columnIfExists", columnIfExists)
        .add("comment", comment)
        .toString();
  }

  @Override
  public long ramBytesUsed() {
    long size = INSTANCE_SIZE;
    size += AstMemoryEstimationHelper.getEstimatedSizeOfNodeLocation(getLocationInternal());
    size += table == null ? 0L : table.ramBytesUsed();
    size += AstMemoryEstimationHelper.getEstimatedSizeOfAccountableObject(field);
    size += RamUsageEstimator.sizeOf(comment);
    return size;
  }
}
