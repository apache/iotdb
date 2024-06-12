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

package org.apache.iotdb.db.queryengine.execution.operator.source;

import org.apache.iotdb.commons.exception.IllegalPathException;
import org.apache.iotdb.db.queryengine.execution.MemoryEstimationHelper;
import org.apache.iotdb.db.storageengine.dataregion.read.IQueryDataSource;

import org.apache.tsfile.common.conf.TSFileDescriptor;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.read.common.block.TsBlock;
import org.apache.tsfile.read.common.block.TsBlockBuilder;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.TimeUnit;

public abstract class AbstractRegionScanDataSourceOperator extends AbstractSourceOperator
    implements DataSourceOperator {

  protected boolean finished = false;

  protected boolean outputCount;
  protected long count = 0;

  protected AbstractRegionScanForActiveDataUtil regionScanUtil;
  protected TsBlockBuilder resultTsBlockBuilder;

  protected abstract boolean getNextTsFileHandle() throws IOException, IllegalPathException;

  protected abstract boolean isAllDataChecked();

  protected abstract void updateActiveData();

  protected abstract List<TSDataType> getResultDataTypes();

  @Override
  public void initQueryDataSource(IQueryDataSource dataSource) {
    regionScanUtil.initQueryDataSource(dataSource);
    resultTsBlockBuilder = new TsBlockBuilder(getResultDataTypes());
  }

  @Override
  public TsBlock next() throws Exception {
    if (retainedTsBlock != null) {
      return getResultFromRetainedTsBlock();
    }
    if (resultTsBlockBuilder.isEmpty()) {
      return null;
    }
    resultTsBlock = resultTsBlockBuilder.build();
    resultTsBlockBuilder.reset();
    return checkTsBlockSizeAndGetResult();
  }

  @Override
  public boolean hasNext() throws Exception {
    if (!resultTsBlockBuilder.isEmpty() || retainedTsBlock != null) {
      return true;
    }
    try {
      // start stopwatch
      long maxRuntime = operatorContext.getMaxRunTime().roundTo(TimeUnit.NANOSECONDS);
      long start = System.nanoTime();

      do {
        if (regionScanUtil.isCurrentTsFileFinished() && !getNextTsFileHandle()) {
          break;
        }

        // continue if there are more
        if (regionScanUtil.filterChunkMetaData() && !regionScanUtil.isCurrentTsFileFinished()) {
          continue;
        }

        if (regionScanUtil.filterChunkData() && !regionScanUtil.isCurrentTsFileFinished()) {
          continue;
        }

        updateActiveData();
        regionScanUtil.finishCurrentFile();

      } while (System.nanoTime() - start < maxRuntime && !resultTsBlockBuilder.isFull());

      finished =
          (resultTsBlockBuilder.isEmpty())
              && ((!regionScanUtil.hasMoreData() && regionScanUtil.isCurrentTsFileFinished())
                  || isAllDataChecked());

      boolean hasCachedCountValue = buildCountResultIfNeed();
      return !finished || hasCachedCountValue;
    } catch (IOException e) {
      throw new IOException("Error occurs when scanning active time series.", e);
    }
  }

  private boolean buildCountResultIfNeed() {
    if (!outputCount || !finished || count == -1) {
      return false;
    }
    resultTsBlockBuilder.getTimeColumnBuilder().writeLong(-1);
    resultTsBlockBuilder.getValueColumnBuilders()[0].writeLong(count);
    resultTsBlockBuilder.declarePosition();
    count = -1;
    return true;
  }

  @Override
  public void close() throws Exception {
    // do nothing
  }

  @Override
  public boolean isFinished() throws Exception {
    return finished;
  }

  @Override
  public long calculateMaxPeekMemory() {
    return Math.max(
        maxReturnSize, TSFileDescriptor.getInstance().getConfig().getPageSizeInByte() * 3L);
  }

  @Override
  public long calculateMaxReturnSize() {
    return maxReturnSize;
  }

  @Override
  public long calculateRetainedSizeAfterCallingNext() {
    return calculateMaxPeekMemoryWithCounter() - calculateMaxReturnSize();
  }

  @Override
  public long ramBytesUsed() {
    return (resultTsBlockBuilder == null ? 0 : resultTsBlockBuilder.getRetainedSizeInBytes())
        + MemoryEstimationHelper.getEstimatedSizeOfAccountableObject(regionScanUtil);
  }
}
