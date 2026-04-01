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

import javax.annotation.Nonnull;

import java.io.DataOutputStream;
import java.io.IOException;
import java.util.List;
import java.util.Objects;

import static java.util.Objects.requireNonNull;

public final class Columns extends Expression {
  private static final long INSTANCE_SIZE = RamUsageEstimator.shallowSizeOfInstance(Columns.class);

  private final String pattern;

  public Columns(@Nonnull NodeLocation location, String pattern) {
    super(requireNonNull(location, "location is null"));
    this.pattern = pattern;
  }

  public String getPattern() {
    return pattern;
  }

  public boolean isColumnsAsterisk() {
    return pattern == null;
  }

  @Override
  public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
    return visitor.visitColumns(this, context);
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
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    Columns columns = (Columns) o;
    return Objects.equals(columns.pattern, this.pattern);
  }

  @Override
  public int hashCode() {
    return Objects.hash(pattern);
  }

  @Override
  public boolean shallowEquals(Node other) {
    if (!sameClass(this, other)) {
      return false;
    }

    Columns otherColumns = (Columns) other;
    return Objects.equals(otherColumns.pattern, this.pattern);
  }

  // =============== serialize =================
  @Override
  public TableExpressionType getExpressionType() {
    throw new UnsupportedOperationException("Columns should be expanded in Analyze stage");
  }

  @Override
  public void serialize(DataOutputStream stream) throws IOException {
    throw new UnsupportedOperationException("Columns should be expanded in Analyze stage");
  }

  @Override
  public long ramBytesUsed() {
    long size = INSTANCE_SIZE;
    size += AstMemoryEstimationHelper.getEstimatedSizeOfNodeLocation(getLocationInternal());
    size += RamUsageEstimator.sizeOf(pattern);
    return size;
  }
}
