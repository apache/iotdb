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

import com.google.common.collect.ImmutableList;
import org.apache.tsfile.utils.RamUsageEstimator;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

public final class PivotRelation extends QueryBody {
  private static final long INSTANCE_SIZE =
      RamUsageEstimator.shallowSizeOfInstance(PivotRelation.class);

  public record Value(Expression expression, Optional<Identifier> alias) {}

  // An absent list denotes a dynamically discovered ON item; an empty list is never parsed.
  public record OnItem(Expression expression, Optional<List<Value>> values) {}

  public record UsingItem(Expression expression, Optional<Identifier> alias) {}

  private final Relation input;
  private final List<OnItem> onItems;
  private final List<UsingItem> usingItems;
  private final Optional<List<Expression>> groupBy;
  private final boolean simplified;

  public PivotRelation(
      NodeLocation location,
      Relation input,
      List<OnItem> onItems,
      List<UsingItem> usingItems,
      Optional<List<Expression>> groupBy,
      boolean simplified) {
    super(location);
    this.input = Objects.requireNonNull(input);
    this.onItems = ImmutableList.copyOf(onItems);
    this.usingItems = ImmutableList.copyOf(usingItems);
    this.groupBy = groupBy.map(ImmutableList::copyOf);
    this.simplified = simplified;
  }

  public Relation getInput() {
    return input;
  }

  public List<OnItem> getOnItems() {
    return onItems;
  }

  public List<UsingItem> getUsingItems() {
    return usingItems;
  }

  public Optional<List<Expression>> getGroupBy() {
    return groupBy;
  }

  public boolean isSimplified() {
    return simplified;
  }

  public boolean isDynamic() {
    return onItems.stream().anyMatch(item -> item.values().isEmpty());
  }

  @Override
  public <R, C> R accept(IAstVisitor<R, C> visitor, C context) {
    return ((CommonQueryAstVisitor<R, C>) visitor).visitPivotRelation(this, context);
  }

  @Override
  public List<Node> getChildren() {
    List<Node> children = new ArrayList<>();
    children.add(input);
    for (OnItem item : onItems) {
      children.add(item.expression());
      item.values()
          .ifPresent(
              values ->
                  values.forEach(
                      value -> {
                        children.add(value.expression());
                        value.alias().ifPresent(children::add);
                      }));
    }
    for (UsingItem item : usingItems) {
      children.add(item.expression());
      item.alias().ifPresent(children::add);
    }
    groupBy.ifPresent(children::addAll);
    return ImmutableList.copyOf(children);
  }

  @Override
  public boolean shallowEquals(Node other) {
    return sameClass(this, other);
  }

  @Override
  public long ramBytesUsed() {
    return INSTANCE_SIZE;
  }

  @Override
  public String toString() {
    return "PivotRelation{" + input + ", " + onItems + ", " + usingItems + "}";
  }

  @Override
  public boolean equals(Object other) {
    if (!(other instanceof PivotRelation)) {
      return false;
    }
    PivotRelation that = (PivotRelation) other;
    return input.equals(that.input)
        && onItems.equals(that.onItems)
        && usingItems.equals(that.usingItems)
        && groupBy.equals(that.groupBy)
        && simplified == that.simplified;
  }

  @Override
  public int hashCode() {
    return Objects.hash(input, onItems, usingItems, groupBy, simplified);
  }
}
