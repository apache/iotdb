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

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import java.util.List;
import java.util.Objects;

import static com.google.common.base.MoreObjects.toStringHelper;

public class SetTableComment extends Statement {
  private final QualifiedName tableName;
  private final boolean ifExists;
  @Nullable private final String comment;

  public SetTableComment(
      final @Nonnull NodeLocation location,
      final QualifiedName tableName,
      final boolean ifExists,
      final @Nullable String comment) {
    super(location);
    this.tableName = tableName;
    this.ifExists = ifExists;
    this.comment = comment;
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
    return Objects.hash(tableName, ifExists, comment);
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
        && Objects.equals(comment, that.comment);
  }

  @Override
  public String toString() {
    return toStringHelper(this)
        .add("tableName", tableName)
        .add("ifExists", ifExists)
        .add("comment", comment)
        .toString();
  }
}
