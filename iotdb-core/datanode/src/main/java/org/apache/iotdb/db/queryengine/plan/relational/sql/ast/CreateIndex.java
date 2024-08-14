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

import java.util.List;
import java.util.Objects;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

public class CreateIndex extends Statement {

  private final QualifiedName tableName;

  private final Identifier indexName;

  private final List<Identifier> columnList;

  public CreateIndex(QualifiedName tableName, Identifier indexName, List<Identifier> columnList) {
    super(null);
    this.tableName = requireNonNull(tableName, "tableName is null");
    this.indexName = requireNonNull(indexName, "indexName is null");
    requireNonNull(columnList, "columnList is null");
    checkArgument(!columnList.isEmpty(), "size of columnList should be larger than 1");
    this.columnList = columnList;
  }

  public CreateIndex(
      NodeLocation location,
      QualifiedName tableName,
      Identifier indexName,
      List<Identifier> columnList) {
    super(requireNonNull(location, "location is null"));
    this.tableName = requireNonNull(tableName, "tableName is null");
    this.indexName = requireNonNull(indexName, "indexName is null");
    requireNonNull(columnList, "columnList is null");
    checkArgument(!columnList.isEmpty(), "size of columnList should be larger than 1");
    this.columnList = columnList;
  }

  public QualifiedName getTableName() {
    return tableName;
  }

  public Identifier getIndexName() {
    return indexName;
  }

  public List<Identifier> getColumnList() {
    return columnList;
  }

  @Override
  public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
    return visitor.visitCreateIndex(this, context);
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
    CreateIndex that = (CreateIndex) o;
    return Objects.equals(tableName, that.tableName)
        && Objects.equals(indexName, that.indexName)
        && Objects.equals(columnList, that.columnList);
  }

  @Override
  public int hashCode() {
    return Objects.hash(tableName, indexName, columnList);
  }

  @Override
  public String toString() {
    return toStringHelper(this)
        .add("tableName", tableName)
        .add("indexName", indexName)
        .add("columnList", columnList)
        .toString();
  }
}
