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

import org.apache.tsfile.utils.RamUsageEstimator;

import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

import static java.util.Objects.requireNonNull;

public class TableFunctionInvocation extends Relation {
  private static final long INSTANCE_SIZE =
      RamUsageEstimator.shallowSizeOfInstance(TableFunctionInvocation.class);

  private final QualifiedName name;
  private final List<TableFunctionArgument> arguments;

  public TableFunctionInvocation(
      NodeLocation location, QualifiedName name, List<TableFunctionArgument> arguments) {
    super(location);
    this.name = requireNonNull(name, "name is null");
    this.arguments = requireNonNull(arguments, "arguments is null");
  }

  public QualifiedName getName() {
    return name;
  }

  public List<TableFunctionArgument> getArguments() {
    return arguments;
  }

  @Override
  public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
    return visitor.visitTableFunctionInvocation(this, context);
  }

  @Override
  public List<? extends Node> getChildren() {
    return arguments;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    TableFunctionInvocation that = (TableFunctionInvocation) o;
    return Objects.equals(name, that.name) && Objects.equals(arguments, that.arguments);
  }

  @Override
  public int hashCode() {
    return Objects.hash(name, arguments);
  }

  @Override
  public String toString() {
    return name
        + "("
        + arguments.stream().map(TableFunctionArgument::toString).collect(Collectors.joining(", "))
        + ")";
  }

  @Override
  public boolean shallowEquals(Node o) {
    if (!sameClass(this, o)) {
      return false;
    }

    TableFunctionInvocation other = (TableFunctionInvocation) o;
    return Objects.equals(name, other.name);
  }

  @Override
  public long ramBytesUsed() {
    long size = INSTANCE_SIZE;
    size += AstMemoryEstimationHelper.getEstimatedSizeOfNodeLocation(getLocationInternal());
    size += name == null ? 0L : name.ramBytesUsed();
    size += AstMemoryEstimationHelper.getEstimatedSizeOfNodeList(arguments);
    return size;
  }
}
