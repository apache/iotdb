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
import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;
import static org.apache.iotdb.db.queryengine.plan.relational.sql.ast.SkipTo.Position.FIRST;
import static org.apache.iotdb.db.queryengine.plan.relational.sql.ast.SkipTo.Position.LAST;
import static org.apache.iotdb.db.queryengine.plan.relational.sql.ast.SkipTo.Position.NEXT;
import static org.apache.iotdb.db.queryengine.plan.relational.sql.ast.SkipTo.Position.PAST_LAST;

public class SkipTo extends Node {
  private static final long INSTANCE_SIZE = RamUsageEstimator.shallowSizeOfInstance(SkipTo.class);

  private final Position position;
  private final Optional<Identifier> identifier;

  public enum Position {
    PAST_LAST,
    NEXT,
    FIRST,
    LAST
  }

  // default
  public static SkipTo skipPastLastRow(NodeLocation location) {
    return new SkipTo(location, PAST_LAST, Optional.empty());
  }

  public static SkipTo skipToNextRow(NodeLocation location) {
    return new SkipTo(location, NEXT, Optional.empty());
  }

  public static SkipTo skipToFirst(NodeLocation location, Identifier identifier) {
    return new SkipTo(location, FIRST, Optional.of(identifier));
  }

  public static SkipTo skipToLast(NodeLocation location, Identifier identifier) {
    return new SkipTo(location, LAST, Optional.of(identifier));
  }

  private SkipTo(NodeLocation location, Position position, Optional<Identifier> identifier) {
    super(location);
    requireNonNull(position, "position is null");
    requireNonNull(identifier, "identifier is null");
    checkArgument(
        identifier.isPresent() || (position == PAST_LAST || position == NEXT),
        "missing identifier in SKIP TO %s",
        position.name());
    checkArgument(
        !identifier.isPresent() || (position == FIRST || position == LAST),
        "unexpected identifier in SKIP TO %s",
        position.name());
    this.position = position;
    this.identifier = identifier;
  }

  public Position getPosition() {
    return position;
  }

  public Optional<Identifier> getIdentifier() {
    return identifier;
  }

  @Override
  public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
    return visitor.visitSkipTo(this, context);
  }

  @Override
  public List<? extends Node> getChildren() {
    return identifier.map(ImmutableList::of).orElse(ImmutableList.of());
  }

  @Override
  public String toString() {
    return toStringHelper(this)
        .add("position", position)
        .add("identifier", identifier.orElse(null))
        .omitNullValues()
        .toString();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    SkipTo that = (SkipTo) o;
    return Objects.equals(position, that.position) && Objects.equals(identifier, that.identifier);
  }

  @Override
  public int hashCode() {
    return Objects.hash(position, identifier);
  }

  @Override
  public boolean shallowEquals(Node other) {
    if (!sameClass(this, other)) {
      return false;
    }

    return position == ((SkipTo) other).position;
  }

  @Override
  public long ramBytesUsed() {
    long size = INSTANCE_SIZE;
    size += AstMemoryEstimationHelper.getEstimatedSizeOfNodeLocation(getLocationInternal());
    size += AstMemoryEstimationHelper.OPTIONAL_INSTANCE_SIZE;
    size += AstMemoryEstimationHelper.getEstimatedSizeOfAccountableObject(identifier.orElse(null));
    return size;
  }
}
