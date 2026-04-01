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
import org.apache.tsfile.utils.RamUsageEstimator;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

public class RangeQuantifier extends PatternQuantifier {
  private static final long INSTANCE_SIZE =
      RamUsageEstimator.shallowSizeOfInstance(RangeQuantifier.class);

  private final Optional<LongLiteral> atLeast;
  private final Optional<LongLiteral> atMost;

  public RangeQuantifier(
      NodeLocation location,
      boolean greedy,
      Optional<LongLiteral> atLeast,
      Optional<LongLiteral> atMost) {
    super(location, greedy);
    this.atLeast = requireNonNull(atLeast, "atLeast is null");
    this.atMost = requireNonNull(atMost, "atMost is null");
  }

  public Optional<LongLiteral> getAtLeast() {
    return atLeast;
  }

  public Optional<LongLiteral> getAtMost() {
    return atMost;
  }

  @Override
  public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
    return visitor.visitRangeQuantifier(this, context);
  }

  @Override
  public List<Node> getChildren() {
    ImmutableList.Builder<Node> children = ImmutableList.builder();
    atLeast.ifPresent(children::add);
    atMost.ifPresent(children::add);
    return children.build();
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if ((obj == null) || (getClass() != obj.getClass())) {
      return false;
    }
    RangeQuantifier o = (RangeQuantifier) obj;
    return isGreedy() == o.isGreedy()
        && Objects.equals(atLeast, o.atLeast)
        && Objects.equals(atMost, o.atMost);
  }

  @Override
  public int hashCode() {
    return Objects.hash(isGreedy(), atLeast, atMost);
  }

  @Override
  public String toString() {
    return toStringHelper(this)
        .add("atLeast", atLeast)
        .add("atMost", atMost)
        .add("greedy", isGreedy())
        .toString();
  }

  @Override
  public long ramBytesUsed() {
    long size = INSTANCE_SIZE;
    size += AstMemoryEstimationHelper.getEstimatedSizeOfNodeLocation(getLocationInternal());
    size += 2 * AstMemoryEstimationHelper.OPTIONAL_INSTANCE_SIZE;
    size += AstMemoryEstimationHelper.getEstimatedSizeOfAccountableObject(atLeast.orElse(null));
    size += AstMemoryEstimationHelper.getEstimatedSizeOfAccountableObject(atMost.orElse(null));
    return size;
  }
}
