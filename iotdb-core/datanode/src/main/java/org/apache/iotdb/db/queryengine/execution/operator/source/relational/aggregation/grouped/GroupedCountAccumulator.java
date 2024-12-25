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
package org.apache.iotdb.db.queryengine.execution.operator.source.relational.aggregation.grouped;

import org.apache.iotdb.db.queryengine.execution.operator.source.relational.aggregation.grouped.array.LongBigArray;

import org.apache.tsfile.block.column.Column;
import org.apache.tsfile.block.column.ColumnBuilder;
import org.apache.tsfile.utils.RamUsageEstimator;

public class GroupedCountAccumulator implements GroupedAccumulator {
  private final LongBigArray countValues = new LongBigArray(0L);

  private static final long INSTANCE_SIZE =
      RamUsageEstimator.shallowSizeOfInstance(GroupedCountAccumulator.class);

  @Override
  public long getEstimatedSize() {
    return INSTANCE_SIZE + countValues.sizeOf();
  }

  @Override
  public void setGroupCount(long groupCount) {
    countValues.ensureCapacity(groupCount);
  }

  @Override
  public void addInput(int[] groupIds, Column[] arguments) {
    for (int i = 0; i < groupIds.length; i++) {
      if (!arguments[0].isNull(i)) {
        countValues.increment(groupIds[i]);
      }
    }
  }

  @Override
  public void addIntermediate(int[] groupIds, Column argument) {
    for (int i = 0; i < groupIds.length; i++) {
      if (!argument.isNull(i)) {
        countValues.add(groupIds[i], argument.getLong(i));
      }
    }
  }

  @Override
  public void evaluateIntermediate(int groupId, ColumnBuilder columnBuilder) {
    columnBuilder.writeLong(countValues.get(groupId));
  }

  @Override
  public void evaluateFinal(int groupId, ColumnBuilder columnBuilder) {
    columnBuilder.writeLong(countValues.get(groupId));
  }

  @Override
  public void prepareFinal() {}

  @Override
  public void reset() {
    countValues.reset();
  }
}
