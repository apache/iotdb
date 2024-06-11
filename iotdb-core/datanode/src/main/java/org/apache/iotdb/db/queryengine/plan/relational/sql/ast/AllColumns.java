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

import javax.annotation.Nullable;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

public class AllColumns extends SelectItem {

  private final List<Identifier> aliases;
  @Nullable private final Expression target;

  public AllColumns() {
    super(null);
    this.target = null;
    this.aliases = ImmutableList.of();
  }

  public AllColumns(Expression target) {
    super(null);
    this.target = requireNonNull(target, "target is null");
    this.aliases = ImmutableList.of();
  }

  public AllColumns(Expression target, List<Identifier> aliases) {
    super(null);
    this.target = requireNonNull(target, "target is null");
    this.aliases = ImmutableList.copyOf(requireNonNull(aliases, "aliases is null"));
  }

  public AllColumns(NodeLocation location, Expression target, List<Identifier> aliases) {
    super(requireNonNull(location, "location is null"));
    this.target = requireNonNull(target, "target is null");
    this.aliases = ImmutableList.copyOf(requireNonNull(aliases, "aliases is null"));
  }

  public AllColumns(NodeLocation location, List<Identifier> aliases) {
    super(requireNonNull(location, "location is null"));
    this.target = null;
    this.aliases = ImmutableList.copyOf(requireNonNull(aliases, "aliases is null"));
  }

  public List<Identifier> getAliases() {
    return aliases;
  }

  public Optional<Expression> getTarget() {
    return Optional.ofNullable(target);
  }

  @Override
  public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
    return visitor.visitAllColumns(this, context);
  }

  @Override
  public List<Node> getChildren() {
    return target == null ? ImmutableList.of() : ImmutableList.of(target);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    AllColumns other = (AllColumns) o;
    return Objects.equals(aliases, other.aliases) && Objects.equals(target, other.target);
  }

  @Override
  public int hashCode() {
    return Objects.hash(aliases, target);
  }

  @Override
  public String toString() {
    StringBuilder builder = new StringBuilder();

    if (target != null) {
      builder.append(target).append(".");
    }
    builder.append("*");

    if (!aliases.isEmpty()) {
      builder.append(" (");
      Joiner.on(", ").appendTo(builder, aliases);
      builder.append(")");
    }

    return builder.toString();
  }

  @Override
  public boolean shallowEquals(Node other) {
    if (!sameClass(this, other)) {
      return false;
    }

    return aliases.equals(((AllColumns) other).aliases);
  }
}
