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

package org.apache.iotdb.db.queryengine.execution.operator.process;

import org.apache.iotdb.commons.exception.IoTDBException;
import org.apache.iotdb.db.queryengine.execution.MemoryEstimationHelper;
import org.apache.iotdb.db.queryengine.execution.operator.Operator;
import org.apache.iotdb.db.queryengine.execution.operator.OperatorContext;
import org.apache.iotdb.db.utils.datastructure.SortKey;
import org.apache.iotdb.db.utils.sort.DiskSpiller;

import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.read.common.block.TsBlock;
import org.apache.tsfile.read.common.block.TsBlockBuilder;
import org.apache.tsfile.utils.RamUsageEstimator;

import java.util.Comparator;
import java.util.List;

public abstract class SortOperator extends AbstractSortOperator {

  private static final long INSTANCE_SIZE =
      RamUsageEstimator.shallowSizeOfInstance(SortOperator.class);

  SortOperator(
      OperatorContext operatorContext,
      Operator inputOperator,
      List<TSDataType> dataTypes,
      DiskSpiller diskSpiller,
      Comparator<SortKey> comparator) {
    super(operatorContext, inputOperator, dataTypes, diskSpiller, comparator);
  }

  @Override
  public TsBlock next() throws Exception {
    if (!inputOperator.hasNextWithTimer()) {
      buildResult();
      TsBlock res = buildFinalResult(tsBlockBuilder);
      tsBlockBuilder.reset();
      return res;
    }
    long startTime = System.nanoTime();
    try {
      TsBlock tsBlock = inputOperator.nextWithTimer();
      if (tsBlock == null) {
        return null;
      }
      dataSize += tsBlock.getRetainedSizeInBytes();
      cacheTsBlock(tsBlock);
    } catch (IoTDBException e) {
      clear();
      throw e;
    } finally {
      prepareUntilReadyCost += System.nanoTime() - startTime;
    }

    return null;
  }

  protected abstract TsBlock buildFinalResult(TsBlockBuilder resultBuilder);

  @Override
  public long ramBytesUsed() {
    return INSTANCE_SIZE
        + MemoryEstimationHelper.getEstimatedSizeOfAccountableObject(inputOperator)
        + MemoryEstimationHelper.getEstimatedSizeOfAccountableObject(operatorContext)
        + RamUsageEstimator.sizeOf(noMoreData)
        + tsBlockBuilder.getRetainedSizeInBytes();
  }
}
