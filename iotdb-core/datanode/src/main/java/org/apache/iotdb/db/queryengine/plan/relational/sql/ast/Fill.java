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

import org.apache.iotdb.db.queryengine.plan.statement.component.FillPolicy;

import com.google.common.base.MoreObjects;
import com.google.common.collect.ImmutableList;
import org.apache.tsfile.utils.TimeDuration;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

public class Fill extends Node {
  private final FillPolicy fillMethod;

  // used for constant fill
  private final Literal fillValue;

  // used for previous fill
  private final TimeDuration timeBound;

  // used for linear fill or previous fill
  private final LongLiteral timeColumnIndex;

  // used for linear fill or previous fill
  private final List<LongLiteral> fillGroupingElements;

  // used for constant fill
  public Fill(NodeLocation location, Literal fillValue) {
    super(requireNonNull(location, "location is null"));
    this.fillValue = fillValue;
    this.timeBound = null;
    this.timeColumnIndex = null;
    this.fillMethod = FillPolicy.CONSTANT;
    this.fillGroupingElements = null;
  }

  // used for previous fill
  public Fill(
      NodeLocation location,
      TimeDuration timeBound,
      LongLiteral timeColumnIndex,
      List<LongLiteral> fillGroupingElements) {
    super(requireNonNull(location, "location is null"));
    this.fillValue = null;
    this.timeBound = timeBound;
    this.timeColumnIndex = timeColumnIndex;
    this.fillMethod = FillPolicy.PREVIOUS;
    this.fillGroupingElements = fillGroupingElements;
  }

  // used for linear fill
  public Fill(
      NodeLocation location, LongLiteral timeColumnIndex, List<LongLiteral> fillGroupingElements) {
    super(requireNonNull(location, "location is null"));
    this.fillValue = null;
    this.timeBound = null;
    this.timeColumnIndex = timeColumnIndex;
    this.fillMethod = FillPolicy.LINEAR;
    this.fillGroupingElements = fillGroupingElements;
  }

  public FillPolicy getFillMethod() {
    return fillMethod;
  }

  public Optional<Literal> getFillValue() {
    return Optional.ofNullable(fillValue);
  }

  public Optional<TimeDuration> getTimeBound() {
    return Optional.ofNullable(timeBound);
  }

  public Optional<LongLiteral> getTimeColumnIndex() {
    return Optional.ofNullable(timeColumnIndex);
  }

  public Optional<List<LongLiteral>> getFillGroupingElements() {
    return Optional.ofNullable(fillGroupingElements);
  }

  @Override
  public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
    return visitor.visitFill(this, context);
  }

  @Override
  public List<Node> getChildren() {
    return fillValue != null ? ImmutableList.of(fillValue) : ImmutableList.of();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    Fill fill = (Fill) o;
    return fillMethod == fill.fillMethod
        && Objects.equals(fillValue, fill.fillValue)
        && Objects.equals(timeBound, fill.timeBound)
        && Objects.equals(timeColumnIndex, fill.timeColumnIndex)
        && Objects.equals(fillGroupingElements, fill.fillGroupingElements);
  }

  @Override
  public int hashCode() {
    return Objects.hash(fillMethod, fillValue, timeBound, timeColumnIndex, fillGroupingElements);
  }

  @Override
  public String toString() {
    MoreObjects.ToStringHelper stringHelper = toStringHelper(this).add("fillMethod", fillMethod);
    if (fillValue != null) {
      stringHelper.add("value", fillValue);
    }
    if (timeBound != null) {
      stringHelper.add("TIME_BOUND", timeBound);
    }
    if (timeColumnIndex != null) {
      stringHelper.add("TIME_COLUMN", timeColumnIndex);
    }
    if (fillGroupingElements != null) {
      stringHelper.add("FILL_GROUP", fillGroupingElements);
    }
    return stringHelper.toString();
  }

  @Override
  public boolean shallowEquals(Node other) {
    if (this == other) {
      return true;
    }
    if (other == null || getClass() != other.getClass()) {
      return false;
    }
    Fill fill = (Fill) other;
    return fillMethod == fill.fillMethod
        && Objects.equals(timeBound, fill.timeBound)
        && Objects.equals(timeColumnIndex, fill.timeColumnIndex)
        && Objects.equals(fillGroupingElements, fill.fillGroupingElements);
  }
}
