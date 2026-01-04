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

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

/**
 * EXECUTE IMMEDIATE statement AST node. Example: EXECUTE IMMEDIATE 'SELECT * FROM table WHERE id =
 * 100'
 */
public final class ExecuteImmediate extends Statement {

  private static final long INSTANCE_SIZE =
      RamUsageEstimator.shallowSizeOfInstance(ExecuteImmediate.class);

  private final StringLiteral sql;
  private final List<Literal> parameters;

  public ExecuteImmediate(StringLiteral sql) {
    this(null, sql, ImmutableList.of());
  }

  public ExecuteImmediate(StringLiteral sql, List<Literal> parameters) {
    this(null, sql, parameters);
  }

  public ExecuteImmediate(NodeLocation location, StringLiteral sql, List<Literal> parameters) {
    super(location);
    this.sql = requireNonNull(sql, "sql is null");
    this.parameters = ImmutableList.copyOf(requireNonNull(parameters, "parameters is null"));
  }

  public StringLiteral getSql() {
    return sql;
  }

  public String getSqlString() {
    return sql.getValue();
  }

  public List<Literal> getParameters() {
    return parameters;
  }

  @Override
  public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
    return visitor.visitExecuteImmediate(this, context);
  }

  @Override
  public List<Node> getChildren() {
    ImmutableList.Builder<Node> children = ImmutableList.builder();
    children.add(sql);
    children.addAll(parameters);
    return children.build();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    ExecuteImmediate that = (ExecuteImmediate) o;
    return Objects.equals(sql, that.sql) && Objects.equals(parameters, that.parameters);
  }

  @Override
  public int hashCode() {
    return Objects.hash(sql, parameters);
  }

  @Override
  public String toString() {
    return toStringHelper(this).add("sql", sql).add("parameters", parameters).toString();
  }

  @Override
  public long ramBytesUsed() {
    long size = INSTANCE_SIZE;
    size += AstMemoryEstimationHelper.getEstimatedSizeOfNodeLocation(getLocationInternal());
    size += AstMemoryEstimationHelper.getEstimatedSizeOfAccountableObject(sql);
    size += AstMemoryEstimationHelper.getEstimatedSizeOfNodeList(parameters);
    return size;
  }
}
