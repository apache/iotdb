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

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import java.util.List;
import java.util.Objects;

import static com.google.common.base.MoreObjects.toStringHelper;

public class SetTableComment extends Statement {
  private static final long INSTANCE_SIZE =
      RamUsageEstimator.shallowSizeOfInstance(SetTableComment.class);
  private final QualifiedName tableName;
  private final boolean ifExists;
  @Nullable private final String comment;
  private final boolean view;

  public SetTableComment(
      final @Nonnull NodeLocation location,
      final QualifiedName tableName,
      final boolean ifExists,
      final @Nullable String comment,
      final boolean view) {
    super(location);
    this.tableName = tableName;
    this.ifExists = ifExists;
    this.comment = comment;
    this.view = view;
  }

  public QualifiedName getTableName() {
    return tableName;
  }

  public boolean ifExists() {
    return ifExists;
  }

  @Nullable
  public String getComment() {
    return comment;
  }

  public boolean isView() {
    return view;
  }

  @Override
  public <R, C> R accept(final AstVisitor<R, C> visitor, final C context) {
    return visitor.visitSetTableComment(this, context);
  }

  @Override
  public List<? extends Node> getChildren() {
    return ImmutableList.of();
  }

  @Override
  public int hashCode() {
    return Objects.hash(tableName, ifExists, comment, view);
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    final SetTableComment that = (SetTableComment) o;
    return ifExists == that.ifExists
        && Objects.equals(tableName, that.tableName)
        && Objects.equals(comment, that.comment)
        && view == that.view;
  }

  @Override
  public String toString() {
    return toStringHelper(this)
        .add("tableName", tableName)
        .add("ifExists", ifExists)
        .add("comment", comment)
        .add("view", view)
        .toString();
  }

  @Override
  public long ramBytesUsed() {
    long size = INSTANCE_SIZE;
    size += AstMemoryEstimationHelper.getEstimatedSizeOfNodeLocation(getLocationInternal());
    size += tableName == null ? 0L : tableName.ramBytesUsed();
    size += RamUsageEstimator.sizeOf(comment);
    return size;
  }
}
