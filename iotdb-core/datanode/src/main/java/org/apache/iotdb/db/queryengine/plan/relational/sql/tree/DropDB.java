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

public class DropDB extends Statement {

  private final Identifier dbName;
  private final boolean exists;

  public DropDB(Identifier catalogName, boolean exists) {
    super(null);
    this.dbName = requireNonNull(catalogName, "catalogName is null");
    this.exists = exists;
  }

  public DropDB(NodeLocation location, Identifier catalogName, boolean exists) {
    super(requireNonNull(location, "location is null"));
    this.dbName = requireNonNull(catalogName, "catalogName is null");
    this.exists = exists;
  }

  public Identifier getDbName() {
    return dbName;
  }

  public boolean isExists() {
    return exists;
  }

  @Override
  public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
    return visitor.visitDropDB(this, context);
  }

  @Override
  public List<Node> getChildren() {
    return ImmutableList.of();
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if ((obj == null) || (getClass() != obj.getClass())) {
      return false;
    }
    DropDB o = (DropDB) obj;
    return Objects.equals(dbName, o.dbName) && (exists == o.exists);
  }

  @Override
  public int hashCode() {
    return Objects.hash(dbName, exists);
  }

  @Override
  public String toString() {
    return toStringHelper(this).add("catalogName", dbName).add("exists", exists).toString();
  }
}
