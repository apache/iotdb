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

public final class Insert extends Statement {

  private final Table table;
  private final Query query;

  @Nullable private final List<Identifier> columns;

  public Insert(Table table, Query query) {
    super(null);
    this.table = requireNonNull(table, "target is null");
    this.columns = null;
    this.query = requireNonNull(query, "query is null");
  }

  public Insert(Table table, List<Identifier> columns, Query query) {
    super(null);
    this.table = requireNonNull(table, "target is null");
    this.columns = requireNonNull(columns, "columns is null");
    this.query = requireNonNull(query, "query is null");
  }

  public Table getTable() {
    return table;
  }

  public QualifiedName getTarget() {
    return table.getName();
  }

  public Optional<List<Identifier>> getColumns() {
    return Optional.ofNullable(columns);
  }

  public Query getQuery() {
    return query;
  }

  @Override
  public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
    return visitor.visitInsert(this, context);
  }

  @Override
  public List<Node> getChildren() {
    return ImmutableList.of(query);
  }

  @Override
  public int hashCode() {
    return Objects.hash(table, columns, query);
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if ((obj == null) || (getClass() != obj.getClass())) {
      return false;
    }
    Insert o = (Insert) obj;
    return Objects.equals(table, o.table)
        && Objects.equals(columns, o.columns)
        && Objects.equals(query, o.query);
  }

  @Override
  public String toString() {
    return toStringHelper(this)
        .add("table", table)
        .add("columns", columns)
        .add("query", query)
        .toString();
  }
}
