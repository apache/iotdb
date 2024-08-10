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

public class DereferenceExpression extends Expression {

  private final Expression base;
  @Nullable private final Identifier field;

  public DereferenceExpression(Expression base, Identifier field) {
    super(null);
    this.base = requireNonNull(base, "base is null");
    this.field = requireNonNull(field, "field is null");
  }

  public DereferenceExpression(NodeLocation location, Expression base, Identifier field) {
    super(requireNonNull(location, "location is null"));
    this.base = requireNonNull(base, "base is null");
    this.field = requireNonNull(field, "field is null");
  }

  public DereferenceExpression(Identifier field) {
    super(null);
    this.base = null;
    this.field = requireNonNull(field, "field is null");
  }

  public DereferenceExpression(NodeLocation location, Identifier field) {
    super(requireNonNull(location, "location is null"));
    this.base = null;
    this.field = requireNonNull(field, "field is null");
  }

  @Override
  public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
    return visitor.visitDereferenceExpression(this, context);
  }

  @Override
  public List<Node> getChildren() {
    ImmutableList.Builder<Node> children = ImmutableList.builder();
    children.add(base);
    if (field != null) {
      children.add(field);
    }
    return children.build();
  }

  public Expression getBase() {
    return base;
  }

  public Optional<Identifier> getField() {
    return Optional.ofNullable(field);
  }

  /**
   * If this DereferenceExpression looks like a QualifiedName, return QualifiedName. Otherwise
   * return null
   */
  public static QualifiedName getQualifiedName(DereferenceExpression expression) {
    if (expression.field == null) {
      return null;
    }

    Identifier field = expression.field;

    List<Identifier> parts = null;
    if (expression.base instanceof Identifier) {
      parts = ImmutableList.of((Identifier) expression.base, field);
    } else if (expression.base instanceof DereferenceExpression) {
      QualifiedName baseQualifiedName = getQualifiedName((DereferenceExpression) expression.base);
      if (baseQualifiedName != null) {
        ImmutableList.Builder<Identifier> builder = ImmutableList.builder();
        builder.addAll(baseQualifiedName.getOriginalParts());
        builder.add(field);
        parts = builder.build();
      }
    }

    return parts == null ? null : QualifiedName.of(parts);
  }

  public static Expression from(QualifiedName name) {
    Expression result = null;

    for (String part : name.getParts()) {
      if (result == null) {
        result = new Identifier(part);
      } else {
        result = new DereferenceExpression(result, new Identifier(part));
      }
    }

    return result;
  }

  public static boolean isQualifiedAllFieldsReference(Expression expression) {
    return expression instanceof DereferenceExpression
        && ((DereferenceExpression) expression).field == null;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    DereferenceExpression that = (DereferenceExpression) o;
    return Objects.equals(base, that.base) && Objects.equals(field, that.field);
  }

  @Override
  public int hashCode() {
    return Objects.hash(base, field);
  }

  @Override
  public boolean shallowEquals(Node other) {
    return sameClass(this, other);
  }
}
