/*
 *
 *  * Licensed to the Apache Software Foundation (ASF) under one
 *  * or more contributor license agreements.  See the NOTICE file
 *  * distributed with this work for additional information
 *  * regarding copyright ownership.  The ASF licenses this file
 *  * to you under the Apache License, Version 2.0 (the
 *  * "License"); you may not use this file except in compliance
 *  * with the License.  You may obtain a copy of the License at
 *  *
 *  *     http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing,
 *  * software distributed under the License is distributed on an
 *  * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  * KIND, either express or implied.  See the License for the
 *  * specific language governing permissions and limitations
 *  * under the License.
 *
 */

package org.apache.iotdb.db.queryengine.plan.relational.sql.ast;

import com.google.common.collect.ImmutableList;
import org.apache.tsfile.utils.RamUsageEstimator;

import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class FollowerHintItem extends Node {
  private static final long INSTANCE_SIZE =
      RamUsageEstimator.shallowSizeOfInstance(FollowerHintItem.class);

  private static final String hintItemName = "follower";
  private final List<String> tables;
  private final List<List<Integer>> nodeIds;

  public FollowerHintItem(List<String> tables, List<List<Integer>> nodeIds) {
    super(null);
    this.tables = ImmutableList.copyOf(tables);
    this.nodeIds = ImmutableList.copyOf(nodeIds);
  }

  public List<String> getTables() {
    return tables;
  }

  public List<List<Integer>> getNodeIds() {
    return nodeIds;
  }

  @Override
  public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
    return visitor.visitFollowerHintItem(this, context);
  }

  @Override
  public List<? extends Node> getChildren() {
    return ImmutableList.of();
  }

  @Override
  public int hashCode() {
    return Objects.hash(tables);
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj == null || getClass() != obj.getClass()) {
      return false;
    }
    FollowerHintItem other = (FollowerHintItem) obj;
    return Objects.equals(this.tables, other.tables) && Objects.equals(this.nodeIds, other.nodeIds);
  }

  @Override
  public String toString() {
    return hintItemName
        + "("
        + IntStream.range(0, tables.size())
            .mapToObj(
                i -> {
                  String table = tables.get(i);
                  List<Integer> ids = nodeIds.get(i);
                  if (ids == null || ids.isEmpty()) {
                    return table;
                  }
                  return table
                      + "("
                      + ids.stream().map(String::valueOf).collect(Collectors.joining(","))
                      + ")";
                })
            .collect(Collectors.joining(","))
        + ")";
  }

  @Override
  public long ramBytesUsed() {
    long size = INSTANCE_SIZE;
    size += AstMemoryEstimationHelper.getEstimatedSizeOfNodeLocation(getLocationInternal());
    size += AstMemoryEstimationHelper.getEstimatedSizeOfStringList(tables);
    size += RamUsageEstimator.sizeOfArrayList(nodeIds);
    return size;
  }
}
