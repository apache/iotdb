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
import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

public class Limit extends Node {
  private final Expression rowCount;

  public Limit(Expression rowCount) {
    super(null);
    checkArgument(
        rowCount instanceof AllRows
            || rowCount instanceof LongLiteral
            || rowCount instanceof Parameter,
        "unexpected rowCount class: %s",
        rowCount.getClass().getSimpleName());
    this.rowCount = rowCount;
  }

  public Limit(NodeLocation location, Expression rowCount) {
    super(requireNonNull(location, "location is null"));
    checkArgument(
        rowCount instanceof AllRows
            || rowCount instanceof LongLiteral
            || rowCount instanceof Parameter,
        "unexpected rowCount class: %s",
        rowCount.getClass().getSimpleName());
    this.rowCount = rowCount;
  }

  public Expression getRowCount() {
    return rowCount;
  }

  @Override
  public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
    return visitor.visitLimit(this, context);
  }

  @Override
  public List<? extends Node> getChildren() {
    return ImmutableList.of(rowCount);
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if ((obj == null) || (getClass() != obj.getClass())) {
      return false;
    }
    Limit o = (Limit) obj;
    return Objects.equals(rowCount, o.rowCount);
  }

  @Override
  public int hashCode() {
    return Objects.hash(rowCount);
  }

  @Override
  public String toString() {
    return toStringHelper(this).add("limit", rowCount).toString();
  }

  @Override
  public boolean shallowEquals(Node other) {
    return sameClass(this, other);
  }
}
