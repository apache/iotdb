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
package org.apache.iotdb.db.mpp.execution.operator.process;

import org.apache.iotdb.db.mpp.execution.operator.Operator;
import org.apache.iotdb.db.mpp.execution.operator.OperatorContext;
import org.apache.iotdb.db.tools.DiskSpiller;
import org.apache.iotdb.db.tools.MemoryReader;
import org.apache.iotdb.db.tools.SortBufferManager;
import org.apache.iotdb.db.tools.SortReader;
import org.apache.iotdb.db.utils.datastructure.MergeSortHeap;
import org.apache.iotdb.db.utils.datastructure.MergeSortKey;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.common.block.TsBlock;
import org.apache.iotdb.tsfile.read.common.block.TsBlockBuilder;
import org.apache.iotdb.tsfile.read.common.block.TsBlockBuilderStatus;
import org.apache.iotdb.tsfile.read.common.block.column.ColumnBuilder;
import org.apache.iotdb.tsfile.read.common.block.column.TimeColumnBuilder;

import com.google.common.util.concurrent.ListenableFuture;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.TimeUnit;

public class SortOperator implements ProcessOperator {
  private final OperatorContext operatorContext;
  private final Operator inputOperator;
  private final TsBlockBuilder tsBlockBuilder;

  // Use to output the result in memory.
  // Because the memory may be larger than tsBlockBuilder's max size
  // so the data may be return in multiple times.
  private int curRow = 0;

  private List<MergeSortKey> cachedData;
  private final Comparator<MergeSortKey> comparator;
  private long cachedBytes;
  private final DiskSpiller diskSpiller;

  // For mergeSort

  private MergeSortHeap mergeSortHeap;
  private List<SortReader> sortReaders;
  private boolean[] noMoreData;
  private boolean[] isEmpty;

  public SortOperator(
      OperatorContext operatorContext,
      Operator inputOperator,
      List<TSDataType> dataTypes,
      String filePrefix,
      Comparator<MergeSortKey> comparator) {
    this.operatorContext = operatorContext;
    this.inputOperator = inputOperator;
    this.tsBlockBuilder = new TsBlockBuilder(dataTypes);
    this.cachedData = new ArrayList<>();
    this.comparator = comparator;
    this.cachedBytes = 0;
    this.diskSpiller = new DiskSpiller(filePrefix, dataTypes);
  }

  @Override
  public OperatorContext getOperatorContext() {
    return operatorContext;
  }

  @Override
  public ListenableFuture<?> isBlocked() {
    return inputOperator.isBlocked();
  }

  @Override
  public TsBlock next() throws Exception {

    if (!inputOperator.hasNextWithTimer()) {
      if (diskSpiller.hasSpilledData()) {
        prepareSortReaders();
        return mergeSort();
      } else {
        cachedData.sort(comparator);
        return buildTsBlockInMemory();
      }
    }

    TsBlock tsBlock = inputOperator.nextWithTimer();
    if (tsBlock == null) {
      return null;
    }
    cacheTsBlock(tsBlock);

    return null;
  }

  private void prepareSortReaders() throws IOException {
    if (sortReaders != null) return;

    sortReaders = new ArrayList<>();
    if (cachedBytes != 0) {
      cachedData.sort(comparator);
      if (SortBufferManager.allocate(cachedBytes)) {
        sortReaders.add(new MemoryReader(cachedData));
      } else {
        diskSpiller.spillSortedData(cachedData);
        cachedData = null;
      }
    }
    sortReaders.addAll(diskSpiller.getReaders());
    // if reader is finished
    noMoreData = new boolean[sortReaders.size()];
    // need to read data from reader when isEmpty is true
    isEmpty = new boolean[sortReaders.size()];
    Arrays.fill(isEmpty, true);
  }

  private void cacheTsBlock(TsBlock tsBlock) throws IOException {
    long bytesSize = tsBlock.getRetainedSizeInBytes();
    if (bytesSize + cachedBytes < SortBufferManager.SORT_BUFFER_SIZE) {
      cachedBytes += bytesSize;
      for (int i = 0; i < tsBlock.getPositionCount(); i++) {
        cachedData.add(new MergeSortKey(tsBlock, i));
      }
    } else {
      cachedData.sort(comparator);
      diskSpiller.spillSortedData(cachedData);
      cachedData.clear();
      cachedBytes = 0;
    }
  }

  private TsBlock buildTsBlockInMemory() {
    TimeColumnBuilder timeColumnBuilder = tsBlockBuilder.getTimeColumnBuilder();
    ColumnBuilder[] valueColumnBuilders = tsBlockBuilder.getValueColumnBuilders();
    for (int i = curRow; i < cachedData.size(); i++) {
      MergeSortKey mergeSortKey = cachedData.get(i);
      TsBlock tsBlock = mergeSortKey.tsBlock;
      int row = mergeSortKey.rowIndex;
      timeColumnBuilder.writeLong(tsBlock.getTimeByIndex(row));
      for (int j = 0; j < valueColumnBuilders.length; j++) {
        valueColumnBuilders[j].write(tsBlock.getColumn(j), row);
      }
      tsBlockBuilder.declarePosition();
      curRow++;
      if (tsBlockBuilder.isFull()) {
        break;
      }
    }

    TsBlock tsBlock = tsBlockBuilder.build();
    tsBlockBuilder.reset();
    return tsBlock;
  }

  private TsBlock mergeSort() throws IOException {

    if (!diskSpiller.allProcessingTaskFinished()) return null;

    if (mergeSortHeap == null) {
      mergeSortHeap = new MergeSortHeap(sortReaders.size(), comparator);
    }

    long startTime = System.nanoTime();
    long maxRuntime = operatorContext.getMaxRunTime().roundTo(TimeUnit.NANOSECONDS);

    // 1. fill the input from each reader
    for (int i = 0; i < sortReaders.size(); i++) {
      if (noMoreData[i] || !isEmpty[i]) continue;
      SortReader sortReader = sortReaders.get(i);
      if (sortReader.hasNext()) {
        MergeSortKey mergeSortKey = sortReader.next();
        mergeSortKey.columnIndex = i;
        mergeSortHeap.push(mergeSortKey);
        isEmpty[i] = false;
      } else {
        noMoreData[i] = true;
        SortBufferManager.releaseOneSortBranch();
      }
    }

    // 2. do merge sort until one TsBlock is consumed up
    tsBlockBuilder.reset();
    TimeColumnBuilder timeBuilder = tsBlockBuilder.getTimeColumnBuilder();
    ColumnBuilder[] valueColumnBuilders = tsBlockBuilder.getValueColumnBuilders();
    while (!mergeSortHeap.isEmpty()) {

      MergeSortKey mergeSortKey = mergeSortHeap.poll();
      TsBlock targetBlock = mergeSortKey.tsBlock;
      timeBuilder.writeLong(targetBlock.getTimeByIndex(mergeSortKey.rowIndex));
      for (int i = 0; i < valueColumnBuilders.length; i++) {
        if (targetBlock.getColumn(i).isNull(mergeSortKey.rowIndex)) {
          valueColumnBuilders[i].appendNull();
          continue;
        }
        valueColumnBuilders[i].write(targetBlock.getColumn(i), mergeSortKey.rowIndex);
      }
      tsBlockBuilder.declarePosition();

      int readerIndex = mergeSortKey.columnIndex;
      mergeSortKey = readNextMergeSortKey(readerIndex);
      if (mergeSortKey != null) {
        mergeSortHeap.push(mergeSortKey);
      } else {
        noMoreData[readerIndex] = true;
        SortBufferManager.releaseOneSortBranch();
      }

      // break if time is out or tsBlockBuilder is full or sortBuffer is not enough
      if (System.nanoTime() - startTime > maxRuntime
          || tsBlockBuilder.isFull()
          || tsBlockBuilder.getSizeInBytes() >= SortBufferManager.getBufferAvailable()) {
        break;
      }
    }
    return tsBlockBuilder.build();
  }

  private MergeSortKey readNextMergeSortKey(int readerIndex) throws IOException {
    SortReader sortReader = sortReaders.get(readerIndex);
    if (sortReader.hasNext()) {
      MergeSortKey mergeSortKey = sortReader.next();
      mergeSortKey.columnIndex = readerIndex;
      return mergeSortKey;
    }
    return null;
  }

  private boolean hasMoreData() {
    for (boolean noMore : noMoreData) {
      if (!noMore) {
        return true;
      }
    }
    return false;
  }

  @Override
  public boolean hasNext() throws Exception {
    return inputOperator.hasNextWithTimer()
        || (!diskSpiller.hasSpilledData() && curRow != cachedData.size())
        || (diskSpiller.hasSpilledData() && hasMoreData());
  }

  @Override
  public void close() throws Exception {
    cachedData = null;
    diskSpiller.clear();
    inputOperator.close();
  }

  @Override
  public boolean isFinished() throws Exception {
    return cachedData == null;
  }

  @Override
  public long calculateMaxPeekMemory() {
    return inputOperator.calculateMaxPeekMemory()
        + inputOperator.calculateRetainedSizeAfterCallingNext()
        + SortBufferManager.SORT_BUFFER_SIZE;
  }

  @Override
  public long calculateMaxReturnSize() {
    return inputOperator.calculateMaxReturnSize()
        + TsBlockBuilderStatus.DEFAULT_MAX_TSBLOCK_SIZE_IN_BYTES;
  }

  @Override
  public long calculateRetainedSizeAfterCallingNext() {
    return inputOperator.calculateRetainedSizeAfterCallingNext()
        + SortBufferManager.SORT_BUFFER_SIZE;
  }
}
