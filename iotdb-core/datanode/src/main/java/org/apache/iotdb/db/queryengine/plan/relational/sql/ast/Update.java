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

public class Update extends Statement {

  private final Table table;
  private final List<UpdateAssignment> assignments;
  @Nullable private final Expression where;

  public Update(NodeLocation location, Table table, List<UpdateAssignment> assignments) {
    super(requireNonNull(location, "location is null"));
    this.table = requireNonNull(table, "table is null");
    this.assignments = requireNonNull(assignments, "assignments is null");
    this.where = null;
  }

  public Update(
      NodeLocation location, Table table, List<UpdateAssignment> assignments, Expression where) {
    super(requireNonNull(location, "location is null"));
    this.table = requireNonNull(table, "table is null");
    this.assignments = requireNonNull(assignments, "assignments is null");
    this.where = requireNonNull(where, "where is null");
  }

  public Table getTable() {
    return table;
  }

  public List<UpdateAssignment> getAssignments() {
    return assignments;
  }

  public Optional<Expression> getWhere() {
    return Optional.ofNullable(where);
  }

  @Override
  public List<? extends Node> getChildren() {
    ImmutableList.Builder<Node> nodes = ImmutableList.builder();
    nodes.addAll(assignments);
    if (where != null) {
      nodes.add(where);
    }
    return nodes.build();
  }

  @Override
  public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
    return visitor.visitUpdate(this, context);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    Update update = (Update) o;
    return table.equals(update.table)
        && assignments.equals(update.assignments)
        && where.equals(update.where);
  }

  @Override
  public int hashCode() {
    return Objects.hash(table, assignments, where);
  }

  @Override
  public String toString() {
    return toStringHelper(this)
        .add("table", table)
        .add("assignments", assignments)
        .add("where", where)
        .omitNullValues()
        .toString();
  }
}
