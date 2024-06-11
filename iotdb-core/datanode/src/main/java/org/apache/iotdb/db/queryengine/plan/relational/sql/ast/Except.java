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

public final class Except extends SetOperation {
  private final Relation left;
  private final Relation right;

  public Except(Relation left, Relation right, boolean distinct) {
    super(null, distinct);

    this.left = requireNonNull(left, "left is null");
    this.right = requireNonNull(right, "right is null");
  }

  public Except(NodeLocation location, Relation left, Relation right, boolean distinct) {
    super(requireNonNull(location, "location is null"), distinct);

    this.left = requireNonNull(left, "left is null");
    this.right = requireNonNull(right, "right is null");
  }

  public Relation getLeft() {
    return left;
  }

  public Relation getRight() {
    return right;
  }

  @Override
  public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
    return visitor.visitExcept(this, context);
  }

  @Override
  public List<Node> getChildren() {
    return ImmutableList.of(left, right);
  }

  @Override
  public List<Relation> getRelations() {
    return ImmutableList.of(left, right);
  }

  @Override
  public String toString() {
    return toStringHelper(this)
        .add("left", left)
        .add("right", right)
        .add("distinct", isDistinct())
        .toString();
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if ((obj == null) || (getClass() != obj.getClass())) {
      return false;
    }
    Except o = (Except) obj;
    return Objects.equals(left, o.left)
        && Objects.equals(right, o.right)
        && Objects.equals(isDistinct(), o.isDistinct());
  }

  @Override
  public int hashCode() {
    return Objects.hash(left, right, isDistinct());
  }

  @Override
  public boolean shallowEquals(Node other) {
    if (!sameClass(this, other)) {
      return false;
    }

    return this.isDistinct() == ((Except) other).isDistinct();
  }
}
