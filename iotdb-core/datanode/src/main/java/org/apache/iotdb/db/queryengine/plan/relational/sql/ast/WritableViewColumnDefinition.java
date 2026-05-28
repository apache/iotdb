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

import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.Node;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.NodeLocation;

import com.google.common.collect.ImmutableList;
import org.apache.tsfile.utils.RamUsageEstimator;

import javax.annotation.Nullable;

import java.util.List;
import java.util.Objects;

import static com.google.common.base.MoreObjects.toStringHelper;

public class WritableViewColumnDefinition extends Node {
  private static final long INSTANCE_SIZE =
      RamUsageEstimator.shallowSizeOfInstance(WritableViewColumnDefinition.class);
  private final String viewColumnName;
  private final String sourceColumnName;
  private final String comment;

  public WritableViewColumnDefinition(
      @Nullable NodeLocation location,
      final String viewColumnName,
      final String sourceColumnName,
      final String comment) {
    super(location);
    this.viewColumnName = viewColumnName;
    this.sourceColumnName = sourceColumnName;
    this.comment = comment;
  }

  public String getViewColumnName() {
    return viewColumnName;
  }

  public String getSourceColumnName() {
    return sourceColumnName;
  }

  public String getComment() {
    return comment;
  }

  @Override
  public List<? extends Node> getChildren() {
    return ImmutableList.of();
  }

  @Override
  public int hashCode() {
    return Objects.hash(viewColumnName, sourceColumnName, comment);
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj == null || getClass() != obj.getClass()) {
      return false;
    }
    final WritableViewColumnDefinition that = (WritableViewColumnDefinition) obj;
    return Objects.equals(viewColumnName, that.viewColumnName)
        && Objects.equals(sourceColumnName, that.sourceColumnName)
        && Objects.equals(comment, that.comment);
  }

  @Override
  public String toString() {
    return toStringHelper(this)
        .add("viewColumnName", viewColumnName)
        .add("sourceColumnName", sourceColumnName)
        .add("comment", comment)
        .toString();
  }

  @Override
  public long ramBytesUsed() {
    long size = INSTANCE_SIZE;
    size += RamUsageEstimator.sizeOf(viewColumnName);
    size += RamUsageEstimator.sizeOf(sourceColumnName);
    size += RamUsageEstimator.sizeOf(comment);
    return size;
  }
}
