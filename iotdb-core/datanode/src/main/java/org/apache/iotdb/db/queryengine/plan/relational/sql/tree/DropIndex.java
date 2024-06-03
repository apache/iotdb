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

import java.util.List;
import java.util.Objects;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

public class DropIndex extends Statement {

  private final QualifiedName tableName;

  private final Identifier indexName;

  public DropIndex(QualifiedName tableName, Identifier indexName) {
    super(null);
    this.tableName = requireNonNull(tableName, "tableName is null");
    this.indexName = requireNonNull(indexName, "indexName is null");
  }

  public DropIndex(NodeLocation location, QualifiedName tableName, Identifier indexName) {
    super(requireNonNull(location, "location is null"));
    this.tableName = requireNonNull(tableName, "tableName is null");
    this.indexName = requireNonNull(indexName, "indexName is null");
  }

  public QualifiedName getTableName() {
    return tableName;
  }

  public Identifier getIndexName() {
    return indexName;
  }

  @Override
  public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
    return visitor.visitDropIndex(this, context);
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
    DropIndex dropIndex = (DropIndex) o;
    return Objects.equals(tableName, dropIndex.tableName)
        && Objects.equals(indexName, dropIndex.indexName);
  }

  @Override
  public int hashCode() {
    return Objects.hash(tableName, indexName);
  }

  @Override
  public String toString() {
    return toStringHelper(this).add("tableName", tableName).add("indexName", indexName).toString();
  }
}
