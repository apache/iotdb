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

import static java.util.Objects.requireNonNull;

public class SingleColumn extends SelectItem {

  @Nullable private final Identifier alias;
  private final Expression expression;

  public SingleColumn(Expression expression) {
    super(null);
    this.expression = requireNonNull(expression, "expression is null");
    this.alias = null;
  }

  public SingleColumn(NodeLocation location, Expression expression) {
    super(requireNonNull(location, "location is null"));
    this.expression = requireNonNull(expression, "expression is null");
    this.alias = null;
  }

  public SingleColumn(Expression expression, Identifier alias) {
    super(null);
    this.expression = requireNonNull(expression, "expression is null");
    this.alias = requireNonNull(alias, "alias is null");
  }

  public SingleColumn(NodeLocation location, Expression expression, Identifier alias) {
    super(requireNonNull(location, "location is null"));
    this.expression = requireNonNull(expression, "expression is null");
    this.alias = requireNonNull(alias, "alias is null");
  }

  public Optional<Identifier> getAlias() {
    return Optional.ofNullable(alias);
  }

  public Expression getExpression() {
    return expression;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj == null || getClass() != obj.getClass()) {
      return false;
    }
    SingleColumn other = (SingleColumn) obj;
    return Objects.equals(this.alias, other.alias)
        && Objects.equals(this.expression, other.expression);
  }

  @Override
  public int hashCode() {
    return Objects.hash(alias, expression);
  }

  @Override
  public String toString() {
    if (alias != null) {
      return expression.toString() + " " + alias;
    }

    return expression.toString();
  }

  @Override
  public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
    return visitor.visitSingleColumn(this, context);
  }

  @Override
  public List<Node> getChildren() {
    return ImmutableList.of(expression);
  }

  @Override
  public boolean shallowEquals(Node other) {
    if (!sameClass(this, other)) {
      return false;
    }

    if (alias == null) {
      return ((SingleColumn) other).alias == null;
    }

    return alias.equals(((SingleColumn) other).alias);
  }
}
