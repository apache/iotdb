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
import static java.util.Objects.requireNonNull;

public final class RenameColumn extends Statement {
  private final QualifiedName table;
  private final Identifier source;
  private final Identifier target;

  public RenameColumn(
      NodeLocation location, QualifiedName table, Identifier source, Identifier target) {
    super(requireNonNull(location, "location is null"));
    this.table = requireNonNull(table, "table is null");
    this.source = requireNonNull(source, "source is null");
    this.target = requireNonNull(target, "target is null");
  }

  public QualifiedName getTable() {
    return table;
  }

  public Identifier getSource() {
    return source;
  }

  public Identifier getTarget() {
    return target;
  }

  @Override
  public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
    return visitor.visitRenameColumn(this, context);
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
    RenameColumn that = (RenameColumn) o;
    return Objects.equals(table, that.table)
        && Objects.equals(source, that.source)
        && Objects.equals(target, that.target);
  }

  @Override
  public int hashCode() {
    return Objects.hash(table, source, target);
  }

  @Override
  public String toString() {
    return toStringHelper(this)
        .add("table", table)
        .add("source", source)
        .add("target", target)
        .toString();
  }
}
