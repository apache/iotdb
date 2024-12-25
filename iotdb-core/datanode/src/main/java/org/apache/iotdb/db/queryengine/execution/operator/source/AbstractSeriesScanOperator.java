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

import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.read.common.block.TsBlock;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.TimeUnit;

public abstract class AbstractSeriesScanOperator extends AbstractDataSourceOperator {

  private boolean finished = false;

  @Override
  public TsBlock next() throws Exception {
    if (retainedTsBlock != null) {
      return getResultFromRetainedTsBlock();
    }
    // we don't get any data in current batch time slice, just return null
    if (resultTsBlockBuilder.isEmpty()) {
      return null;
    }
    resultTsBlock = resultTsBlockBuilder.build();
    resultTsBlockBuilder.reset();
    return checkTsBlockSizeAndGetResult();
  }

  @Override
  public boolean isFinished() throws Exception {
    return finished;
  }

  @SuppressWarnings("squid:S112")
  @Override
  public boolean hasNext() throws Exception {
    if (retainedTsBlock != null) {
      return true;
    }
    try {

      // start stopwatch
      long maxRuntime = operatorContext.getMaxRunTime().roundTo(TimeUnit.NANOSECONDS);
      long start = System.nanoTime();

      boolean noMoreData = false;

      // here use do-while to promise doing this at least once
      do {
        /*
         * 1. consume page data firstly
         * 2. consume chunk data secondly
         * 3. consume next file finally
         */
        if (!readPageData() && !readChunkData() && !readFileData()) {
          noMoreData = true;
          break;
        }

      } while (System.nanoTime() - start < maxRuntime
          && !resultTsBlockBuilder.isFull()
          && retainedTsBlock == null);

      finished = (resultTsBlockBuilder.isEmpty() && retainedTsBlock == null && noMoreData);

      return !finished;
    } catch (IOException e) {
      throw new RuntimeException("Error happened while scanning the file", e);
    }
  }

  protected boolean readFileData() throws IOException {
    while (seriesScanUtil.hasNextFile()) {
      if (readChunkData()) {
        return true;
      }
    }
    return false;
  }

  protected boolean readChunkData() throws IOException {
    while (seriesScanUtil.hasNextChunk()) {
      if (readPageData()) {
        return true;
      }
    }
    return false;
  }

  protected boolean readPageData() throws IOException {
    if (seriesScanUtil.hasNextPage()) {
      TsBlock tsBlock = seriesScanUtil.nextPage();
      if (!isEmpty(tsBlock)) {
        appendToBuilder(tsBlock);
      }
      return true;
    }
    return false;
  }

  protected boolean isEmpty(TsBlock tsBlock) {
    return tsBlock == null || tsBlock.isEmpty();
  }

  protected void appendToBuilder(TsBlock tsBlock) {
    int size = tsBlock.getPositionCount();
    if (resultTsBlockBuilder.isEmpty() && size >= resultTsBlockBuilder.getMaxTsBlockLineNumber()) {
      retainedTsBlock = tsBlock;
      return;
    }
    buildResult(tsBlock);
  }

  protected abstract void buildResult(TsBlock tsBlock);

  @Override
  protected List<TSDataType> getResultDataTypes() {
    return seriesScanUtil.getTsDataTypeList();
  }

  @Override
  public long calculateMaxReturnSize() {
    return maxReturnSize;
  }

  @Override
  public long calculateRetainedSizeAfterCallingNext() {
    return calculateMaxPeekMemoryWithCounter() - calculateMaxReturnSize();
  }
}
