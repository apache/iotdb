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

package org.apache.iotdb.db.queryengine.execution.operator.source.relational.aggregation.grouped.hash;

import org.apache.iotdb.db.queryengine.execution.operator.source.relational.aggregation.grouped.UpdateMemory;

import com.google.common.annotations.VisibleForTesting;
import org.apache.tsfile.block.column.Column;
import org.apache.tsfile.read.common.block.column.BooleanColumn;
import org.apache.tsfile.read.common.block.column.RunLengthEncodedColumn;
import org.apache.tsfile.read.common.type.Type;

import java.util.List;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static org.apache.iotdb.db.queryengine.execution.operator.source.relational.aggregation.grouped.hash.GroupByHash.DEFAULT_GROUP_NUMBER;
import static org.apache.iotdb.db.queryengine.execution.operator.source.relational.aggregation.grouped.hash.GroupByHash.createGroupByHash;

public class MarkDistinctHash {
  private final GroupByHash groupByHash;
  private long nextDistinctId;

  public MarkDistinctHash(List<Type> types, boolean hasPrecomputedHash, UpdateMemory updateMemory) {
    this.groupByHash =
        createGroupByHash(types, hasPrecomputedHash, DEFAULT_GROUP_NUMBER, updateMemory);
  }

  public long getEstimatedSize() {
    return groupByHash.getEstimatedSize() + Long.BYTES;
  }

  public Column markDistinctRows(Column[] columns) {
    checkArgument(columns.length > 0, "columns shouldn't be empty here");
    int[] groupIds = groupByHash.getGroupIds(columns);
    return processNextGroupIds(
        groupByHash.getGroupCount(), groupIds, columns[0].getPositionCount());
  }

  @VisibleForTesting
  public int getCapacity() {
    return groupByHash.getCapacity();
  }

  private Column processNextGroupIds(int groupCount, int[] ids, int positions) {
    if (positions > 1) {
      // must have > 1 positions to benefit from using a RunLengthEncoded block
      if (nextDistinctId == groupCount) {
        // no new distinct positions
        return new RunLengthEncodedColumn(
            new BooleanColumn(1, Optional.empty(), new boolean[] {false}), positions);
      }
      if (nextDistinctId + positions == groupCount) {
        // all positions are distinct
        nextDistinctId = groupCount;
        return new RunLengthEncodedColumn(
            new BooleanColumn(1, Optional.empty(), new boolean[] {true}), positions);
      }
    }
    boolean[] distinctMask = new boolean[positions];
    for (int position = 0; position < distinctMask.length; position++) {
      if (ids[position] == nextDistinctId) {
        distinctMask[position] = true;
        nextDistinctId++;
      } else {
        distinctMask[position] = false;
      }
    }
    checkState(nextDistinctId == groupCount);
    return new BooleanColumn(positions, Optional.empty(), distinctMask);
  }
}
