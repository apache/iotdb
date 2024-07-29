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

public class AlterTableAddColumn extends Statement {
  private final QualifiedName name;
  private final ColumnDefinition element;

  private final boolean tableIfExists;
  private final boolean columnIfNotExists;

  protected AlterTableAddColumn(
      final NodeLocation location,
      final QualifiedName name,
      final ColumnDefinition element,
      final boolean tableIfExists,
      final boolean columnIfNotExists) {
    super(requireNonNull(location, "location is null"));
    this.name = requireNonNull(name, "name is null");
    this.element = requireNonNull(element, "elements is null");
    this.tableIfExists = tableIfExists;
    this.columnIfNotExists = columnIfNotExists;
  }

  public QualifiedName getName() {
    return name;
  }

  public ColumnDefinition getElement() {
    return element;
  }

  public boolean tableIfExists() {
    return tableIfExists;
  }

  public boolean columnIfNotExists() {
    return columnIfNotExists;
  }

  @Override
  public <R, C> R accept(final AstVisitor<R, C> visitor, final C context) {
    return visitor.visitAlterTableAddColumn(this, context);
  }

  @Override
  public List<? extends Node> getChildren() {
    return Collections.singletonList(element);
  }

  @Override
  public int hashCode() {
    return Objects.hash(name, element, tableIfExists, columnIfNotExists);
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    final AlterTableAddColumn that = (AlterTableAddColumn) o;
    return tableIfExists == that.tableIfExists
        && columnIfNotExists == that.columnIfNotExists
        && Objects.equals(name, that.name)
        && Objects.equals(element, that.element);
  }

  @Override
  public String toString() {
    return toStringHelper(this)
        .add("name", name)
        .add("element", element)
        .add("tableIfExists", tableIfExists)
        .add("columnIfExists", columnIfNotExists)
        .toString();
  }
}
