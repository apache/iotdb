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

import javax.annotation.Nullable;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

public class ShowTables extends Statement {

  @Nullable private final Identifier dbName;

  private final boolean isDetails;

  public ShowTables(final NodeLocation location, final boolean isDetails) {
    super(requireNonNull(location, "location is null"));
    this.dbName = null;
    this.isDetails = isDetails;
  }

  public ShowTables(final NodeLocation location, final Identifier dbName, final boolean isDetails) {
    super(requireNonNull(location, "location is null"));
    this.dbName = requireNonNull(dbName, "dbName is null");
    this.isDetails = isDetails;
  }

  public Optional<Identifier> getDbName() {
    return Optional.ofNullable(dbName);
  }

  public boolean isDetails() {
    return isDetails;
  }

  @Override
  public <R, C> R accept(final AstVisitor<R, C> visitor, final C context) {
    return visitor.visitShowTables(this, context);
  }

  @Override
  public List<Node> getChildren() {
    return ImmutableList.of();
  }

  @Override
  public int hashCode() {
    return Objects.hash(dbName);
  }

  @Override
  public boolean equals(final Object obj) {
    if (this == obj) {
      return true;
    }
    if ((obj == null) || (getClass() != obj.getClass())) {
      return false;
    }
    final ShowTables o = (ShowTables) obj;
    return Objects.equals(dbName, o.dbName);
  }

  @Override
  public String toString() {
    return toStringHelper(this).add("dbName", dbName).toString();
  }
}
