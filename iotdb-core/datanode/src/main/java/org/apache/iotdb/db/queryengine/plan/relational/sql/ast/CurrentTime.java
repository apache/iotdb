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

public class CurrentTime extends Expression {
  private final Function function;

  @Nullable private final Integer precision;

  public enum Function {
    TIME("current_time"),
    DATE("current_date"),
    TIMESTAMP("current_timestamp"),
    LOCALTIME("localtime"),
    LOCALTIMESTAMP("localtimestamp");

    private final String name;

    Function(String name) {
      this.name = name;
    }

    public String getName() {
      return name;
    }
  }

  public CurrentTime(NodeLocation location, Function function) {
    super(requireNonNull(location, "location is null"));

    this.function = requireNonNull(function, "function is null");
    this.precision = null;
  }

  public CurrentTime(NodeLocation location, Function function, Integer precision) {
    super(requireNonNull(location, "location is null"));

    this.function = requireNonNull(function, "function is null");
    this.precision = requireNonNull(precision, "precision is null");
  }

  public Function getFunction() {
    return function;
  }

  public Optional<Integer> getPrecision() {
    return Optional.ofNullable(precision);
  }

  @Override
  public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
    return visitor.visitCurrentTime(this, context);
  }

  @Override
  public List<Node> getChildren() {
    return ImmutableList.of();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if ((o == null) || (getClass() != o.getClass())) {
      return false;
    }
    CurrentTime that = (CurrentTime) o;
    return (function == that.function) && Objects.equals(precision, that.precision);
  }

  @Override
  public int hashCode() {
    return Objects.hash(function, precision);
  }

  @Override
  public boolean shallowEquals(Node other) {
    if (!sameClass(this, other)) {
      return false;
    }

    CurrentTime otherNode = (CurrentTime) other;
    return (function == otherNode.function) && Objects.equals(precision, otherNode.precision);
  }
}
