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

/** DEALLOCATE PREPARE statement AST node. Example: DEALLOCATE PREPARE stmt1 */
public final class Deallocate extends Statement {

  private static final long INSTANCE_SIZE =
      RamUsageEstimator.shallowSizeOfInstance(Deallocate.class);

  private final Identifier statementName;

  public Deallocate(Identifier statementName) {
    this(null, statementName);
  }

  public Deallocate(NodeLocation location, Identifier statementName) {
    super(location);
    this.statementName = requireNonNull(statementName, "statementName is null");
  }

  public Identifier getStatementName() {
    return statementName;
  }

  @Override
  public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
    return visitor.visitDeallocate(this, context);
  }

  @Override
  public List<Node> getChildren() {
    return ImmutableList.of(statementName);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    Deallocate that = (Deallocate) o;
    return Objects.equals(statementName, that.statementName);
  }

  @Override
  public int hashCode() {
    return Objects.hash(statementName);
  }

  @Override
  public String toString() {
    return toStringHelper(this).add("statementName", statementName).toString();
  }

  @Override
  public long ramBytesUsed() {
    long size = INSTANCE_SIZE;
    size += AstMemoryEstimationHelper.getEstimatedSizeOfNodeLocation(getLocationInternal());
    size += AstMemoryEstimationHelper.getEstimatedSizeOfAccountableObject(statementName);
    return size;
  }
}
