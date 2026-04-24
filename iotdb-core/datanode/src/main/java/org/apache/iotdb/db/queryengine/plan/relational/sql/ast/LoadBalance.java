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

import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.AstMemoryEstimationHelper;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.IAstVisitor;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.Node;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.NodeLocation;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.Statement;

import com.google.common.collect.ImmutableList;
import org.apache.tsfile.utils.RamUsageEstimator;

import javax.annotation.Nullable;

import java.util.List;
import java.util.Objects;

import static com.google.common.base.MoreObjects.toStringHelper;

public class LoadBalance extends Statement {
  private static final long INSTANCE_SIZE =
      RamUsageEstimator.shallowSizeOfInstance(LoadBalance.class);

  private final List<Integer> targetNodeIds;

  public LoadBalance() {
    this(null, null);
  }

  public LoadBalance(@Nullable NodeLocation location, @Nullable List<Integer> targetNodeIds) {
    super(location);
    this.targetNodeIds = targetNodeIds;
  }

  public LoadBalance(@Nullable List<Integer> targetNodeIds) {
    this(null, targetNodeIds);
  }

  public List<Integer> getTargetNodeIds() {
    return targetNodeIds;
  }

  @Override
  public <R, C> R accept(IAstVisitor<R, C> visitor, C context) {
    return ((AstVisitor<R, C>) visitor).visitLoadBalance(this, context);
  }

  @Override
  public List<? extends Node> getChildren() {
    return ImmutableList.of();
  }

  @Override
  public int hashCode() {
    return Objects.hash(LoadBalance.class, targetNodeIds);
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (!(obj instanceof LoadBalance)) {
      return false;
    }
    LoadBalance another = (LoadBalance) obj;
    return Objects.equals(targetNodeIds, another.targetNodeIds);
  }

  @Override
  public String toString() {
    return toStringHelper(this).add("targetNodeIds", targetNodeIds).toString();
  }

  @Override
  public long ramBytesUsed() {
    long size = INSTANCE_SIZE;
    size += AstMemoryEstimationHelper.getEstimatedSizeOfNodeLocation(getLocationInternal());
    size += AstMemoryEstimationHelper.getEstimatedSizeOfIntegerList(targetNodeIds);
    return size;
  }
}
