/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iotdb.db.queryengine.execution.operator.source.relational.aggregation.grouped.hash;

import org.apache.tsfile.block.column.Column;
import org.apache.tsfile.read.common.block.TsBlockBuilder;
import org.apache.tsfile.utils.RamUsageEstimator;

public class NoChannelGroupByHash implements GroupByHash {
  private static final long INSTANCE_SIZE =
      RamUsageEstimator.shallowSizeOfInstance(NoChannelGroupByHash.class);

  private int groupCount;

  @Override
  public long getEstimatedSize() {
    return INSTANCE_SIZE;
  }

  @Override
  public int getGroupCount() {
    return groupCount;
  }

  @Override
  public void appendValuesTo(int groupId, TsBlockBuilder pageBuilder) {
    throw new UnsupportedOperationException("NoChannelGroupByHash does not support appendValuesTo");
  }

  @Override
  public void addPage(Column[] groupedColumns) {
    updateGroupCount(groupedColumns);
  }

  @Override
  public int[] getGroupIds(Column[] groupedColumns) {
    return new int[groupedColumns[0].getPositionCount()];
  }

  @Override
  public long getRawHash(int groupId) {
    throw new UnsupportedOperationException("NoChannelGroupByHash does not support getRawHash");
  }

  @Override
  public int getCapacity() {
    return 2;
  }

  private void updateGroupCount(Column[] columns) {
    if (columns[0].getPositionCount() > 0 && groupCount == 0) {
      groupCount = 1;
    }
  }
}
