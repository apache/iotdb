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

package org.apache.iotdb.db.queryengine.execution.operator.process.function;

import org.apache.iotdb.db.queryengine.common.transformation.TableFunctionPartitionImpl;
import org.apache.iotdb.db.queryengine.common.transformation.Transformation;
import org.apache.iotdb.db.queryengine.common.transformation.TransformationState;

import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.read.common.block.TsBlock;

import java.util.List;
import java.util.Optional;
import java.util.Queue;

public class TableFunctionPartitionTransformation
    implements Transformation<TsBlock, TableFunctionPartitionImpl> {

  private final List<TSDataType> outputDataTypes;
  private final List<Integer> requiredChannels;
  private final List<Integer> partitionChannels;
  private final int properChannelCount;

  private Queue<TsBlock> cachedBlocks;
  private int endBlockIndex; // where previous block end
  private int endPositionIndex; // where previous position end

  public TableFunctionPartitionTransformation(
      List<TSDataType> outputDataTypes,
      List<Integer> requiredChannels,
      List<Integer> partitionChannels,
      int properChannelCount) {
    this.outputDataTypes = outputDataTypes;
    this.requiredChannels = requiredChannels;
    this.partitionChannels = partitionChannels;
    this.properChannelCount = properChannelCount;
  }

  @Override
  public TransformationState<TableFunctionPartitionImpl> process(Optional<TsBlock> element) {
    // 1. if element is null, find the remain partition and return the RESULT state
    if (element == null) {}

    // 2. if the input block is empty, return the NEEDS_MORE_DATA state
    // 3. if the first element and the last element are in the same partition, return
    // NEEDS_MORE_DATA state
    if (!element.isPresent()) {
      return TransformationState.needsMoreData();
    }
    TsBlock block = element.get();

    // 4. if the first element and the last element are not in the same partition, find the
    // partition and return the RESULT state
    return null;
  }

  /** Pop the depreciated blocks from the cache. */
  private void popDepreciatedBlocks() {
    for (int i = 0; i < endBlockIndex; i++) {
      cachedBlocks.poll();
    }
  }

  /** Check if the block is in the same partition with current partition. */
  private boolean isSamePartition(TsBlock block) {

    return false;
  }
}
