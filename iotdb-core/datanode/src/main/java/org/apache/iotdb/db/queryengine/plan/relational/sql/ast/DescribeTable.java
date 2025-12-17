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

import java.util.List;
import java.util.Objects;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

public class DescribeTable extends Statement {
  private static final long INSTANCE_SIZE =
      RamUsageEstimator.shallowSizeOfInstance(DescribeTable.class);
  private final QualifiedName table;
  private final boolean isDetails;

  // true: showCreateView
  // false: showCreateTable
  // null: Desc
  private final Boolean isShowCreateView;

  public DescribeTable(
      final @Nonnull NodeLocation location,
      final QualifiedName table,
      final boolean isDetails,
      final Boolean isShowCreateView) {
    super(requireNonNull(location, "location is null"));
    this.table = requireNonNull(table, "table is null");
    this.isDetails = isDetails;
    this.isShowCreateView = isShowCreateView;
  }

  public QualifiedName getTable() {
    return table;
  }

  public boolean isDetails() {
    return isDetails;
  }

  public Boolean getShowCreateView() {
    return isShowCreateView;
  }

  @Override
  public <R, C> R accept(final AstVisitor<R, C> visitor, final C context) {
    return visitor.visitDescribeTable(this, context);
  }

  @Override
  public List<Node> getChildren() {
    return ImmutableList.of();
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    final DescribeTable that = (DescribeTable) o;
    return Objects.equals(table, that.table)
        && isDetails == that.isDetails
        && Objects.equals(isShowCreateView, that.isShowCreateView);
  }

  @Override
  public int hashCode() {
    return Objects.hash(table, isDetails, isShowCreateView);
  }

  @Override
  public String toString() {
    return toStringHelper(this)
        .add("table", table)
        .add("isDetails", isDetails)
        .add("isShowCreateView", isShowCreateView)
        .toString();
  }

  @Override
  public long ramBytesUsed() {
    long size = INSTANCE_SIZE;
    size += AstMemoryEstimationHelper.getEstimatedSizeOfNodeLocation(getLocationInternal());
    size += table == null ? 0L : table.ramBytesUsed();
    return size;
  }
}
