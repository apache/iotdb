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

package org.apache.iotdb.db.queryengine.execution.operator.process;

import org.apache.iotdb.commons.conf.IoTDBConstant;
import org.apache.iotdb.commons.exception.IoTDBException;
import org.apache.iotdb.db.queryengine.execution.MemoryEstimationHelper;
import org.apache.iotdb.db.queryengine.execution.operator.Operator;
import org.apache.iotdb.db.queryengine.execution.operator.OperatorContext;
import org.apache.iotdb.db.utils.datastructure.MergeSortHeap;
import org.apache.iotdb.db.utils.datastructure.MergeSortKey;
import org.apache.iotdb.db.utils.datastructure.SortKey;
import org.apache.iotdb.db.utils.sort.DiskSpiller;
import org.apache.iotdb.db.utils.sort.MemoryReader;
import org.apache.iotdb.db.utils.sort.SortBufferManager;
import org.apache.iotdb.db.utils.sort.SortReader;

import com.google.common.util.concurrent.ListenableFuture;
import org.apache.tsfile.block.column.ColumnBuilder;
import org.apache.tsfile.common.conf.TSFileDescriptor;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.read.common.block.TsBlock;
import org.apache.tsfile.read.common.block.TsBlockBuilder;
import org.apache.tsfile.read.common.block.column.TimeColumnBuilder;
import org.apache.tsfile.utils.RamUsageEstimator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static org.apache.iotdb.db.utils.sort.SortBufferManager.SORT_BUFFER_SIZE;

public class SortOperator implements ProcessOperator {

  private static final long INSTANCE_SIZE =
      RamUsageEstimator.shallowSizeOfInstance(SortOperator.class);
  private final OperatorContext operatorContext;
  private final Operator inputOperator;
  private final TsBlockBuilder tsBlockBuilder;

  // Use to output the result in memory.
  // Because the memory may be larger than tsBlockBuilder's max size
  // so the data may be return in multiple times.
  private int curRow = -1;

  private List<SortKey> cachedData;
  private final Comparator<SortKey> comparator;
  private long cachedBytes;
  private final DiskSpiller diskSpiller;
  private final SortBufferManager sortBufferManager;

  // For mergeSort

  private MergeSortHeap mergeSortHeap;
  private List<SortReader> sortReaders;
  private boolean[] noMoreData;

  private static final Logger logger = LoggerFactory.getLogger(IoTDBConstant.QUERY_LOGGER_NAME);

  private final int maxReturnSize =
      TSFileDescriptor.getInstance().getConfig().getMaxTsBlockSizeInBytes();

  private long prepareUntilReadyCost = 0;
  private long dataSize = 0;
  private long sortCost = 0;

  public SortOperator(
      OperatorContext operatorContext,
      Operator inputOperator,
      List<TSDataType> dataTypes,
      String folderPath,
      Comparator<SortKey> comparator) {
    this.operatorContext = operatorContext;
    this.inputOperator = inputOperator;
    this.tsBlockBuilder = new TsBlockBuilder(dataTypes);
    this.cachedData = new ArrayList<>();
    this.comparator = comparator;
    this.cachedBytes = 0;
    this.diskSpiller =
        new DiskSpiller(folderPath, folderPath + operatorContext.getOperatorId(), dataTypes);
    this.sortBufferManager = new SortBufferManager();
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
        try {
          prepareSortReaders();
          return mergeSort();
        } catch (Exception e) {
          clear();
          throw e;
        }
      } else {
        if (curRow == -1) {
          long startTime = System.nanoTime();
          cachedData.sort(comparator);
          sortCost += System.nanoTime() - startTime;
          curRow = 0;
        }
        return buildTsBlockInMemory();
      }
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

  private void recordMetrics() {
    operatorContext.recordSpecifiedInfo("prepareCost/ns", Long.toString(prepareUntilReadyCost));
    operatorContext.recordSpecifiedInfo("sortedDataSize", Long.toString(dataSize));
    operatorContext.recordSpecifiedInfo("sortCost/ns", Long.toString(sortCost));
    int spilledFileSize = diskSpiller.getFileSize();
    if (spilledFileSize > 0) {
      operatorContext.recordSpecifiedInfo(
          "merge sort branch", Integer.toString(diskSpiller.getFileSize() + 1));
    }
  }

  private void prepareSortReaders() throws IoTDBException {
    if (sortReaders != null) {
      return;
    }
    sortReaders = new ArrayList<>();
    if (cachedBytes != 0) {
      cachedData.sort(comparator);
      if (sortBufferManager.allocate(cachedBytes)) {
        sortReaders.add(
            new MemoryReader(
                cachedData.stream().map(MergeSortKey::new).collect(Collectors.toList())));
      } else {
        sortBufferManager.allocateOneSortBranch();
        diskSpiller.spillSortedData(cachedData);
        cachedData = null;
      }
    }
    sortReaders.addAll(diskSpiller.getReaders(sortBufferManager));
    // if reader is finished
    noMoreData = new boolean[sortReaders.size()];
  }

  private void cacheTsBlock(TsBlock tsBlock) throws IoTDBException {
    long bytesSize = tsBlock.getRetainedSizeInBytes();
    if (bytesSize + cachedBytes < SORT_BUFFER_SIZE) {
      cachedBytes += bytesSize;
      for (int i = 0; i < tsBlock.getPositionCount(); i++) {
        cachedData.add(new MergeSortKey(tsBlock, i));
      }
    } else {
      cachedData.sort(comparator);
      spill();
      cachedData.clear();
      cachedBytes = bytesSize;
      for (int i = 0; i < tsBlock.getPositionCount(); i++) {
        cachedData.add(new MergeSortKey(tsBlock, i));
      }
    }
  }

  private void spill() throws IoTDBException {
    // if current memory cannot put this tsBlock, an exception will be thrown in spillSortedData()
    // because there should be at least tsBlockBuilderStatus.DEFAULT_MAX_TSBLOCK_SIZE_IN_BYTES for
    // one branch.
    sortBufferManager.allocateOneSortBranch();
    diskSpiller.spillSortedData(cachedData);
  }

  private TsBlock buildTsBlockInMemory() {
    tsBlockBuilder.reset();
    TimeColumnBuilder timeColumnBuilder = tsBlockBuilder.getTimeColumnBuilder();
    ColumnBuilder[] valueColumnBuilders = tsBlockBuilder.getValueColumnBuilders();
    for (int i = curRow; i < cachedData.size(); i++) {
      SortKey sortKey = cachedData.get(i);
      TsBlock tsBlock = sortKey.tsBlock;
      timeColumnBuilder.writeLong(tsBlock.getTimeByIndex(sortKey.rowIndex));
      for (int j = 0; j < valueColumnBuilders.length; j++) {
        if (tsBlock.getColumn(j).isNull(sortKey.rowIndex)) {
          valueColumnBuilders[j].appendNull();
          continue;
        }
        valueColumnBuilders[j].write(tsBlock.getColumn(j), sortKey.rowIndex);
      }
      tsBlockBuilder.declarePosition();
      curRow++;
      if (tsBlockBuilder.isFull()) {
        break;
      }
    }

    return tsBlockBuilder.build();
  }

  private TsBlock mergeSort() throws IoTDBException {

    // 1. fill the input from each reader
    initMergeSortHeap();

    long startTime = System.nanoTime();
    long maxRuntime = operatorContext.getMaxRunTime().roundTo(TimeUnit.NANOSECONDS);

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

      int readerIndex = mergeSortKey.inputChannelIndex;
      mergeSortKey = readNextMergeSortKey(readerIndex);
      if (mergeSortKey != null) {
        mergeSortHeap.push(mergeSortKey);
      } else {
        noMoreData[readerIndex] = true;
        sortBufferManager.releaseOneSortBranch();
      }

      // break if time is out or tsBlockBuilder is full or sortBuffer is not enough
      if (System.nanoTime() - startTime > maxRuntime || tsBlockBuilder.isFull()) {
        break;
      }
    }
    sortCost += System.nanoTime() - startTime;
    return tsBlockBuilder.build();
  }

  private void initMergeSortHeap() throws IoTDBException {
    if (mergeSortHeap == null) {
      mergeSortHeap = new MergeSortHeap(sortReaders.size(), comparator);
      for (int i = 0; i < sortReaders.size(); i++) {
        SortReader sortReader = sortReaders.get(i);
        if (sortReader.hasNext()) {
          MergeSortKey mergeSortKey = sortReader.next();
          mergeSortKey.inputChannelIndex = i;
          mergeSortHeap.push(mergeSortKey);
        } else {
          noMoreData[i] = true;
          sortBufferManager.releaseOneSortBranch();
        }
      }
    }
  }

  private MergeSortKey readNextMergeSortKey(int readerIndex) throws IoTDBException {
    SortReader sortReader = sortReaders.get(readerIndex);
    if (sortReader.hasNext()) {
      MergeSortKey mergeSortKey = sortReader.next();
      mergeSortKey.inputChannelIndex = readerIndex;
      return mergeSortKey;
    }
    return null;
  }

  private boolean hasMoreData() {
    if (noMoreData == null) {
      return true;
    }
    for (boolean noMore : noMoreData) {
      if (!noMore) {
        return true;
      }
    }
    return false;
  }

  public void clear() {
    if (!diskSpiller.hasSpilledData()) {
      return;
    }
    try {
      if (sortReaders != null) {
        for (SortReader sortReader : sortReaders) {
          sortReader.close();
        }
      }
    } catch (Exception e) {
      logger.error("Fail to close fileChannel", e);
    }
  }

  @Override
  public boolean hasNext() throws Exception {
    return inputOperator.hasNextWithTimer()
        || (!diskSpiller.hasSpilledData() && curRow != cachedData.size())
        || (diskSpiller.hasSpilledData() && hasMoreData());
  }

  @Override
  public void close() throws Exception {
    recordMetrics();
    cachedData = null;
    clear();
    inputOperator.close();
  }

  @Override
  public boolean isFinished() throws Exception {
    return !this.hasNextWithTimer();
  }

  @Override
  public long calculateMaxPeekMemory() {
    return inputOperator.calculateMaxPeekMemoryWithCounter()
        + inputOperator.calculateRetainedSizeAfterCallingNext()
        + SORT_BUFFER_SIZE;
  }

  @Override
  public long calculateMaxReturnSize() {
    return maxReturnSize;
  }

  @Override
  public long calculateRetainedSizeAfterCallingNext() {
    return inputOperator.calculateRetainedSizeAfterCallingNext() + SORT_BUFFER_SIZE;
  }

  @Override
  public long ramBytesUsed() {
    return INSTANCE_SIZE
        + MemoryEstimationHelper.getEstimatedSizeOfAccountableObject(inputOperator)
        + MemoryEstimationHelper.getEstimatedSizeOfAccountableObject(operatorContext)
        + RamUsageEstimator.sizeOf(noMoreData)
        + tsBlockBuilder.getRetainedSizeInBytes();
  }
}
