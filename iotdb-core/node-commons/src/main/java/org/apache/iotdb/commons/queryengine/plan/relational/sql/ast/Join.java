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

package org.apache.iotdb.commons.queryengine.plan.relational.sql.ast;

import org.apache.iotdb.commons.i18n.QueryMessages;

import com.google.common.collect.ImmutableList;
import org.apache.tsfile.utils.RamUsageEstimator;

import javax.annotation.Nullable;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

public class Join extends Relation {

  private static final long INSTANCE_SIZE = RamUsageEstimator.shallowSizeOfInstance(Join.class);

  public enum Type {
    CROSS,
    INNER,
    LEFT,
    RIGHT,
    FULL,
    IMPLICIT
  }

  private final Type type;
  private final Relation left;
  private final Relation right;

  @Nullable private final JoinCriteria criteria;

  public Join(Type type, Relation left, Relation right) {
    super(null);
    this.criteria = null;
    checkArgument(
        (type == Type.CROSS) || (type == Type.IMPLICIT),
        QueryMessages.EXCEPTION_NO_JOIN_CRITERIA_SPECIFIED_44DED0B9);
    this.type = type;
    this.left = requireNonNull(left, QueryMessages.EXCEPTION_LEFT_IS_NULL_2C1080C5);
    this.right = requireNonNull(right, QueryMessages.EXCEPTION_RIGHT_IS_NULL_97BD6491);
  }

  public Join(NodeLocation location, Type type, Relation left, Relation right) {
    super(requireNonNull(location, QueryMessages.EXCEPTION_LOCATION_IS_NULL_F134D388));
    this.criteria = null;
    checkArgument(
        (type == Type.CROSS) || (type == Type.IMPLICIT),
        QueryMessages.EXCEPTION_NO_JOIN_CRITERIA_SPECIFIED_44DED0B9);
    this.type = type;
    this.left = requireNonNull(left, QueryMessages.EXCEPTION_LEFT_IS_NULL_2C1080C5);
    this.right = requireNonNull(right, QueryMessages.EXCEPTION_RIGHT_IS_NULL_97BD6491);
  }

  public Join(Type type, Relation left, Relation right, JoinCriteria criteria) {
    super(null);
    this.criteria = requireNonNull(criteria, QueryMessages.EXCEPTION_CRITERIA_IS_NULL_2996D1A3);
    checkArgument(
        !((type == Type.CROSS) || (type == Type.IMPLICIT)),
        QueryMessages.EXCEPTION_ARG_JOIN_CANNOT_HAVE_JOIN_CRITERIA_3B23E0D1,
        type);
    this.type = type;
    this.left = requireNonNull(left, QueryMessages.EXCEPTION_LEFT_IS_NULL_2C1080C5);
    this.right = requireNonNull(right, QueryMessages.EXCEPTION_RIGHT_IS_NULL_97BD6491);
  }

  public Join(
      NodeLocation location, Type type, Relation left, Relation right, JoinCriteria criteria) {
    super(requireNonNull(location, QueryMessages.EXCEPTION_LOCATION_IS_NULL_F134D388));
    this.criteria = requireNonNull(criteria, QueryMessages.EXCEPTION_CRITERIA_IS_NULL_2996D1A3);
    checkArgument(
        !((type == Type.CROSS) || (type == Type.IMPLICIT)),
        QueryMessages.EXCEPTION_ARG_JOIN_CANNOT_HAVE_JOIN_CRITERIA_3B23E0D1,
        type);
    this.type = type;
    this.left = requireNonNull(left, QueryMessages.EXCEPTION_LEFT_IS_NULL_2C1080C5);
    this.right = requireNonNull(right, QueryMessages.EXCEPTION_RIGHT_IS_NULL_97BD6491);
  }

  public Type getType() {
    return type;
  }

  public Relation getLeft() {
    return left;
  }

  public Relation getRight() {
    return right;
  }

  public Optional<JoinCriteria> getCriteria() {
    return Optional.ofNullable(criteria);
  }

  @Override
  public <R, C> R accept(IAstVisitor<R, C> visitor, C context) {
    return ((CommonQueryAstVisitor<R, C>) visitor).visitJoin(this, context);
  }

  @Override
  public List<Node> getChildren() {
    ImmutableList.Builder<Node> nodes = ImmutableList.builder();
    nodes.add(left);
    nodes.add(right);
    if (criteria != null) {
      nodes.addAll(criteria.getNodes());
    }
    return nodes.build();
  }

  @Override
  public String toString() {
    return toStringHelper(this)
        .add("type", type)
        .add("left", left)
        .add("right", right)
        .add("criteria", criteria)
        .omitNullValues()
        .toString();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if ((o == null) || (getClass() != o.getClass())) {
      return false;
    }
    Join join = (Join) o;
    return (type == join.type)
        && Objects.equals(left, join.left)
        && Objects.equals(right, join.right)
        && Objects.equals(criteria, join.criteria);
  }

  @Override
  public int hashCode() {
    return Objects.hash(type, left, right, criteria);
  }

  @Override
  public boolean shallowEquals(Node other) {
    if (!sameClass(this, other)) {
      return false;
    }

    return type.equals(((Join) other).type);
  }

  @Override
  public long ramBytesUsed() {
    long size = INSTANCE_SIZE;
    size += AstMemoryEstimationHelper.getEstimatedSizeOfNodeLocation(getLocationInternal());
    size += AstMemoryEstimationHelper.getEstimatedSizeOfAccountableObject(left);
    size += AstMemoryEstimationHelper.getEstimatedSizeOfAccountableObject(right);
    size += AstMemoryEstimationHelper.getEstimatedSizeOfAccountableObject(criteria);
    return size;
  }
}
