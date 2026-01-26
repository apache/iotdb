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

/** PREPARE statement AST node. Example: PREPARE stmt1 FROM SELECT * FROM table WHERE id = ? */
public final class Prepare extends Statement {

  private static final long INSTANCE_SIZE = RamUsageEstimator.shallowSizeOfInstance(Prepare.class);

  private final Identifier statementName;
  private final Statement sql;

  public Prepare(Identifier statementName, Statement sql) {
    super(null);
    this.statementName = requireNonNull(statementName, "statementName is null");
    this.sql = requireNonNull(sql, "sql is null");
  }

  public Prepare(NodeLocation location, Identifier statementName, Statement sql) {
    super(location);
    this.statementName = requireNonNull(statementName, "statementName is null");
    this.sql = requireNonNull(sql, "sql is null");
  }

  public Identifier getStatementName() {
    return statementName;
  }

  public Statement getSql() {
    return sql;
  }

  @Override
  public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
    return visitor.visitPrepare(this, context);
  }

  @Override
  public List<Node> getChildren() {
    return ImmutableList.of(statementName, sql);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    Prepare that = (Prepare) o;
    return Objects.equals(statementName, that.statementName) && Objects.equals(sql, that.sql);
  }

  @Override
  public int hashCode() {
    return Objects.hash(statementName, sql);
  }

  @Override
  public String toString() {
    return toStringHelper(this).add("statementName", statementName).add("sql", sql).toString();
  }

  @Override
  public long ramBytesUsed() {
    long size = INSTANCE_SIZE;
    size += AstMemoryEstimationHelper.getEstimatedSizeOfNodeLocation(getLocationInternal());
    size += AstMemoryEstimationHelper.getEstimatedSizeOfAccountableObject(statementName);
    size += AstMemoryEstimationHelper.getEstimatedSizeOfAccountableObject(sql);
    return size;
  }
}
