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

import org.apache.iotdb.commons.schema.table.column.TsTableColumnCategory;

import com.google.common.collect.ImmutableList;
import org.apache.tsfile.utils.RamUsageEstimator;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

public class ColumnDefinition extends Node {

  private static final long INSTANCE_SIZE =
      RamUsageEstimator.shallowSizeOfInstance(ColumnDefinition.class);

  private final Identifier name;
  private final DataType type;
  private final TsTableColumnCategory columnCategory;

  @Nullable private final String charsetName;
  @Nullable private final String comment;

  public ColumnDefinition(
      final @Nullable NodeLocation location,
      final Identifier name,
      final DataType type,
      final TsTableColumnCategory columnCategory,
      final @Nullable String charsetName,
      final @Nullable String comment) {
    super(location);
    this.name = requireNonNull(name, "name is null");
    this.columnCategory = requireNonNull(columnCategory, "columnCategory is null");
    this.type = getDefaultType(type);
    this.charsetName = charsetName;
    this.comment = comment;
  }

  protected DataType getDefaultType(final DataType type) {
    if (Objects.isNull(type)) {
      if ((columnCategory == TsTableColumnCategory.TAG
          || columnCategory == TsTableColumnCategory.ATTRIBUTE)) {
        return new GenericDataType(new Identifier("string"), new ArrayList<>());
      } else if (columnCategory == TsTableColumnCategory.TIME) {
        return new GenericDataType(new Identifier("timestamp"), new ArrayList<>());
      }
    }
    return type;
  }

  public Identifier getName() {
    return name;
  }

  public DataType getType() {
    return type;
  }

  public TsTableColumnCategory getColumnCategory() {
    return columnCategory;
  }

  public Optional<String> getCharsetName() {
    return Optional.ofNullable(charsetName);
  }

  public @Nullable String getComment() {
    return comment;
  }

  @Override
  public <R, C> R accept(final AstVisitor<R, C> visitor, C context) {
    return visitor.visitColumnDefinition(this, context);
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
    final ColumnDefinition that = (ColumnDefinition) o;
    return Objects.equals(name, that.name)
        && Objects.equals(type, that.type)
        && columnCategory == that.columnCategory
        && Objects.equals(charsetName, that.charsetName)
        && Objects.equals(comment, that.comment);
  }

  @Override
  public int hashCode() {
    return Objects.hash(name, type, columnCategory, charsetName, comment);
  }

  @Override
  public String toString() {
    return toStringHelper(this)
        .add("name", name)
        .add("type", type)
        .add("columnCategory", columnCategory)
        .add("charsetName", charsetName)
        .add("comment", comment)
        .toString();
  }

  @Override
  public long ramBytesUsed() {
    long size = INSTANCE_SIZE;
    size += AstMemoryEstimationHelper.getEstimatedSizeOfNodeLocation(getLocationInternal());
    size += AstMemoryEstimationHelper.getEstimatedSizeOfAccountableObject(name);
    size += AstMemoryEstimationHelper.getEstimatedSizeOfAccountableObject(type);
    size += RamUsageEstimator.sizeOf(charsetName);
    size += RamUsageEstimator.sizeOf(comment);
    return size;
  }
}
