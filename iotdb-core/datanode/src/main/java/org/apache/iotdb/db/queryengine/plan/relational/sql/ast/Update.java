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

import javax.annotation.Nullable;

import java.util.List;
import java.util.Objects;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

public class Update extends AbstractTraverseDevice {
  private static final long INSTANCE_SIZE = RamUsageEstimator.shallowSizeOfInstance(Update.class);

  private List<UpdateAssignment> assignments;

  public Update(
      final @Nullable NodeLocation location,
      final Table table,
      final List<UpdateAssignment> assignments,
      final Expression where) {
    super(location, table, where);
    this.assignments = requireNonNull(assignments, "assignments is null");
  }

  public List<UpdateAssignment> getAssignments() {
    return assignments;
  }

  public void setAssignments(final List<UpdateAssignment> assignments) {
    this.assignments = assignments;
  }

  @Override
  public List<? extends Node> getChildren() {
    final ImmutableList.Builder<Node> nodes = ImmutableList.builder();
    nodes.addAll(assignments);
    if (where != null) {
      nodes.add(where);
    }
    return nodes.build();
  }

  @Override
  public <R, C> R accept(final AstVisitor<R, C> visitor, final C context) {
    return visitor.visitUpdate(this, context);
  }

  @Override
  public boolean equals(final Object o) {
    return super.equals(o) && assignments.equals(((Update) o).assignments);
  }

  @Override
  public int hashCode() {
    return Objects.hash(super.hashCode(), assignments);
  }

  @Override
  public String toString() {
    return toStringHelper(this).add("assignments", assignments).omitNullValues()
        + " - "
        + super.toStringContent();
  }

  @Override
  public long ramBytesUsed() {
    long size = INSTANCE_SIZE;
    size += ramBytesUsedForCommonFields();
    size += AstMemoryEstimationHelper.getEstimatedSizeOfNodeList(assignments);
    return size;
  }
}
