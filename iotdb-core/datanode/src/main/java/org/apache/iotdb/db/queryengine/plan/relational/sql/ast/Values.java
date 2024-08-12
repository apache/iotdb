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

import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Objects;

import static java.util.Objects.requireNonNull;

public final class Values extends QueryBody {
  private final List<Expression> rows;

  public Values(List<Expression> rows) {
    super(null);
    this.rows = ImmutableList.copyOf(requireNonNull(rows, "rows is null"));
  }

  public Values(NodeLocation location, List<Expression> rows) {
    super(requireNonNull(location, "location is null"));
    this.rows = ImmutableList.copyOf(requireNonNull(rows, "rows is null"));
  }

  public List<Expression> getRows() {
    return rows;
  }

  @Override
  public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
    return visitor.visitValues(this, context);
  }

  @Override
  public List<? extends Node> getChildren() {
    return rows;
  }

  @Override
  public String toString() {
    return "(" + Joiner.on(", ").join(rows) + ")";
  }

  @Override
  public int hashCode() {
    return Objects.hash(rows);
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj == null || getClass() != obj.getClass()) {
      return false;
    }
    Values other = (Values) obj;
    return Objects.equals(this.rows, other.rows);
  }

  @Override
  public boolean shallowEquals(Node other) {
    return sameClass(this, other);
  }
}
