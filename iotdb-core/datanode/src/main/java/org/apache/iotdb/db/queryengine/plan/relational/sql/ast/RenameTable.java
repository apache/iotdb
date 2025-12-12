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

import org.apache.iotdb.db.exception.sql.SemanticException;

import com.google.common.collect.ImmutableList;
import org.apache.tsfile.utils.RamUsageEstimator;

import java.util.List;
import java.util.Objects;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

public class RenameTable extends Statement {
  private static final long INSTANCE_SIZE =
      RamUsageEstimator.shallowSizeOfInstance(RenameTable.class);

  private final QualifiedName source;
  private final Identifier target;

  private final boolean tableIfExists;
  private final boolean view;

  public RenameTable(
      final NodeLocation location,
      final QualifiedName source,
      final Identifier target,
      final boolean tableIfExists,
      final boolean view) {
    super(requireNonNull(location, "location is null"));
    this.source = requireNonNull(source, "source name is null");
    this.target = requireNonNull(target, "target name is null");
    this.tableIfExists = tableIfExists;
    this.view = view;
    if (!view) {
      throw new SemanticException("The renaming for base table is currently unsupported");
    }
  }

  public QualifiedName getSource() {
    return source;
  }

  public Identifier getTarget() {
    return target;
  }

  public boolean tableIfExists() {
    return tableIfExists;
  }

  public boolean isView() {
    return view;
  }

  @Override
  public <R, C> R accept(final AstVisitor<R, C> visitor, final C context) {
    return visitor.visitRenameTable(this, context);
  }

  @Override
  public List<Node> getChildren() {
    return ImmutableList.of();
  }

  @Override
  public int hashCode() {
    return Objects.hash(source, target, tableIfExists, view);
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }
    if ((o == null) || (getClass() != o.getClass())) {
      return false;
    }
    final RenameTable that = (RenameTable) o;
    return tableIfExists == that.tableIfExists
        && Objects.equals(source, that.source)
        && Objects.equals(target, that.target)
        && view == that.view;
  }

  @Override
  public String toString() {
    return toStringHelper(this)
        .add("source", source)
        .add("target", target)
        .add("tableIfExists", tableIfExists)
        .add("view", view)
        .toString();
  }

  @Override
  public long ramBytesUsed() {
    long size = INSTANCE_SIZE;
    size += AstMemoryEstimationHelper.getEstimatedSizeOfNodeLocation(getLocationInternal());
    size += source == null ? 0L : source.ramBytesUsed();
    size += AstMemoryEstimationHelper.getEstimatedSizeOfAccountableObject(target);
    return size;
  }
}
