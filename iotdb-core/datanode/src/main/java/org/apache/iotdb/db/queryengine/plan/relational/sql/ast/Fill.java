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
  private final TimeDuration timeDurationThreshold;

  // used for linear fill or previous fill
  private final LongLiteral index;

  // used for constant fill
  public Fill(NodeLocation location, Literal fillValue) {
    super(requireNonNull(location, "location is null"));
    this.fillValue = fillValue;
    this.timeDurationThreshold = null;
    this.index = null;
    this.fillMethod = FillPolicy.VALUE;
  }

  // used for previous fill
  public Fill(NodeLocation location, TimeDuration timeDurationThreshold, LongLiteral index) {
    super(requireNonNull(location, "location is null"));
    this.fillValue = null;
    this.timeDurationThreshold = timeDurationThreshold;
    this.index = index;
    this.fillMethod = FillPolicy.PREVIOUS;
  }

  // used for linear fill
  public Fill(NodeLocation location, LongLiteral index) {
    super(requireNonNull(location, "location is null"));
    this.fillValue = null;
    this.timeDurationThreshold = null;
    this.index = index;
    this.fillMethod = FillPolicy.LINEAR;
  }

  public Fill(NodeLocation location) {
    super(requireNonNull(location, "location is null"));
    this.fillValue = null;
    this.timeDurationThreshold = null;
    this.index = null;
    this.fillMethod = FillPolicy.LINEAR;
  }

  public FillPolicy getFillMethod() {
    return fillMethod;
  }

  public Optional<Literal> getFillValue() {
    return Optional.ofNullable(fillValue);
  }

  public Optional<TimeDuration> getTimeDurationThreshold() {
    return Optional.ofNullable(timeDurationThreshold);
  }

  public Optional<LongLiteral> getIndex() {
    return Optional.ofNullable(index);
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
        && Objects.equals(timeDurationThreshold, fill.timeDurationThreshold)
        && Objects.equals(index, fill.index);
  }

  @Override
  public int hashCode() {
    return Objects.hash(fillMethod, fillValue, timeDurationThreshold, index);
  }

  @Override
  public String toString() {
    MoreObjects.ToStringHelper stringHelper = toStringHelper(this).add("fillMethod", fillMethod);
    if (fillValue != null) {
      stringHelper.add("value", fillValue);
    }
    if (timeDurationThreshold != null) {
      stringHelper.add("timeDurationThreshold", timeDurationThreshold);
    }
    if (index != null) {
      stringHelper.add("index", index);
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
        && Objects.equals(timeDurationThreshold, fill.timeDurationThreshold)
        && Objects.equals(index, fill.index);
  }
}
