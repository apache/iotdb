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

package org.apache.iotdb.db.relational.sql.tree;

import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Objects;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

public final class Intersect extends SetOperation {
  private final List<Relation> relations;

  public Intersect(List<Relation> relations, boolean distinct) {
    super(null, distinct);
    this.relations = ImmutableList.copyOf(requireNonNull(relations, "relations is null"));
  }

  public Intersect(NodeLocation location, List<Relation> relations, boolean distinct) {
    super(requireNonNull(location, "location is null"), distinct);
    this.relations = ImmutableList.copyOf(requireNonNull(relations, "relations is null"));
  }

  @Override
  public List<Relation> getRelations() {
    return relations;
  }

  @Override
  public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
    return visitor.visitIntersect(this, context);
  }

  @Override
  public List<? extends Node> getChildren() {
    return relations;
  }

  @Override
  public String toString() {
    return toStringHelper(this)
        .add("relations", relations)
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
    Intersect o = (Intersect) obj;
    return Objects.equals(relations, o.relations) && Objects.equals(isDistinct(), o.isDistinct());
  }

  @Override
  public int hashCode() {
    return Objects.hash(relations, isDistinct());
  }

  @Override
  public boolean shallowEquals(Node other) {
    if (!sameClass(this, other)) {
      return false;
    }

    return this.isDistinct() == ((Intersect) other).isDistinct();
  }
}
