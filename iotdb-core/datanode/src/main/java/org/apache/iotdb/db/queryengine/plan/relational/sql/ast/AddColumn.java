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

import java.util.Collections;
import java.util.List;
import java.util.Objects;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

public class AddColumn extends Statement {

  private final QualifiedName tableName;
  private final ColumnDefinition column;

  private final boolean tableIfExists;
  private final boolean columnIfNotExists;

  public AddColumn(
      final QualifiedName tableName,
      final ColumnDefinition column,
      final boolean tableIfExists,
      final boolean columnIfNotExists) {
    super(null);

    this.tableName = requireNonNull(tableName, "tableName is null");
    this.column = requireNonNull(column, "column is null");
    this.tableIfExists = tableIfExists;
    this.columnIfNotExists = columnIfNotExists;
  }

  public AddColumn(
      final NodeLocation location,
      final QualifiedName tableName,
      final ColumnDefinition column,
      final boolean tableIfExists,
      final boolean columnIfNotExists) {
    super(requireNonNull(location, "location is null"));

    this.tableName = requireNonNull(tableName, "tableName is null");
    this.column = requireNonNull(column, "column is null");
    this.tableIfExists = tableIfExists;
    this.columnIfNotExists = columnIfNotExists;
  }

  public QualifiedName getTableName() {
    return tableName;
  }

  public ColumnDefinition getColumn() {
    return column;
  }

  public boolean tableIfExists() {
    return tableIfExists;
  }

  public boolean columnIfNotExists() {
    return columnIfNotExists;
  }

  @Override
  public <R, C> R accept(final AstVisitor<R, C> visitor, final C context) {
    return visitor.visitAddColumn(this, context);
  }

  @Override
  public List<? extends Node> getChildren() {
    return Collections.singletonList(column);
  }

  @Override
  public int hashCode() {
    return Objects.hash(tableName, column, tableIfExists, columnIfNotExists);
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    final AddColumn that = (AddColumn) o;
    return tableIfExists == that.tableIfExists
        && columnIfNotExists == that.columnIfNotExists
        && Objects.equals(tableName, that.tableName)
        && Objects.equals(column, that.column);
  }

  @Override
  public String toString() {
    return toStringHelper(this)
        .add("tableName", tableName)
        .add("column", column)
        .add("tableIfExists", tableIfExists)
        .add("columnIfExists", columnIfNotExists)
        .toString();
  }
}
