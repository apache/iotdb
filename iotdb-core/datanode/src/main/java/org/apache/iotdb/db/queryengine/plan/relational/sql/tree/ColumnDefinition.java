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

package org.apache.iotdb.db.queryengine.plan.relational.sql.tree;

import com.google.common.collect.ImmutableList;

import javax.annotation.Nullable;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

public final class ColumnDefinition extends Node {

  public enum ColumnCategory {
    ID,
    ATTRIBUTE,
    TIME,
    MEASUREMENT;
  }

  private final Identifier name;
  private final DataType type;
  private final ColumnCategory columnCategory;

  @Nullable private final String charsetName;

  public ColumnDefinition(
      Identifier name, DataType type, ColumnCategory columnCategory, @Nullable String charsetName) {
    super(null);
    this.name = requireNonNull(name, "name is null");
    this.type = requireNonNull(type, "type is null");
    this.columnCategory = requireNonNull(columnCategory, "columnCategory is null");
    this.charsetName = charsetName;
  }

  public ColumnDefinition(
      NodeLocation location,
      Identifier name,
      DataType type,
      ColumnCategory columnCategory,
      @Nullable String charsetName) {
    super(requireNonNull(location, "location is null"));
    this.name = requireNonNull(name, "name is null");
    this.type = requireNonNull(type, "type is null");
    this.columnCategory = requireNonNull(columnCategory, "columnCategory is null");
    this.charsetName = charsetName;
  }

  public Identifier getName() {
    return name;
  }

  public DataType getType() {
    return type;
  }

  public ColumnCategory getColumnCategory() {
    return columnCategory;
  }

  public Optional<String> getCharsetName() {
    return Optional.ofNullable(charsetName);
  }

  @Override
  public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
    return visitor.visitColumnDefinition(this, context);
  }

  @Override
  public List<Node> getChildren() {
    return ImmutableList.of();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    ColumnDefinition that = (ColumnDefinition) o;
    return Objects.equals(name, that.name)
        && Objects.equals(type, that.type)
        && columnCategory == that.columnCategory
        && Objects.equals(charsetName, that.charsetName);
  }

  @Override
  public int hashCode() {
    return Objects.hash(name, type, columnCategory, charsetName);
  }

  @Override
  public String toString() {
    return toStringHelper(this)
        .add("name", name)
        .add("type", type)
        .add("columnCategory", columnCategory)
        .add("charsetName", charsetName)
        .toString();
  }
}
