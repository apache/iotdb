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

import org.apache.iotdb.commons.exception.IoTDBException;
import org.apache.iotdb.db.mpp.execution.operator.Operator;
import org.apache.iotdb.db.mpp.execution.operator.OperatorContext;
import org.apache.iotdb.db.tools.DiskSpiller;
import org.apache.iotdb.db.tools.MemoryReader;
import org.apache.iotdb.db.tools.SortBufferManager;
import org.apache.iotdb.db.tools.SortReader;
import org.apache.iotdb.db.utils.datastructure.MergeSortHeap;
import org.apache.iotdb.db.utils.datastructure.MergeSortKey;
import org.apache.iotdb.rpc.TSStatusCode;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.common.block.TsBlock;
import org.apache.iotdb.tsfile.read.common.block.TsBlockBuilder;
import org.apache.iotdb.tsfile.read.common.block.TsBlockBuilderStatus;
import org.apache.iotdb.tsfile.read.common.block.column.ColumnBuilder;
import org.apache.iotdb.tsfile.read.common.block.column.TimeColumnBuilder;

import com.google.common.util.concurrent.ListenableFuture;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
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
  private int curRow = -1;

  private final String filePrePath;

  private List<MergeSortKey> cachedData;
  private final Comparator<MergeSortKey> comparator;
  private long cachedBytes;
  private final DiskSpiller diskSpiller;
  private final SortBufferManager sortBufferManager;

  // For mergeSort

  private MergeSortHeap mergeSortHeap;
  private List<SortReader> sortReaders;
  private boolean[] noMoreData;
  private boolean[] isEmpty;

  Logger logger = LoggerFactory.getLogger(SortOperator.class);

  public SortOperator(
      OperatorContext operatorContext,
      Operator inputOperator,
      List<TSDataType> dataTypes,
      String folderPath,
      Comparator<MergeSortKey> comparator) {
    this.operatorContext = operatorContext;
    this.inputOperator = inputOperator;
    this.tsBlockBuilder = new TsBlockBuilder(dataTypes);
    this.cachedData = new ArrayList<>();
    this.comparator = comparator;
    this.cachedBytes = 0;
    this.filePrePath = folderPath + operatorContext.getOperatorId() + "-";
    this.diskSpiller = new DiskSpiller(folderPath, filePrePath, dataTypes);
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
          cachedData.sort(comparator);
          curRow = 0;
        }
        return buildTsBlockInMemory();
      }
    }

    TsBlock tsBlock = inputOperator.nextWithTimer();
    if (tsBlock == null) {
      return null;
    }

    try {
      cacheTsBlock(tsBlock);
    } catch (IoTDBException e) {
      clear();
      throw e;
    }

    return null;
  }

  private void prepareSortReaders() throws IoTDBException {
    if (sortReaders != null) return;

    try {

      sortReaders = new ArrayList<>();
      if (cachedBytes != 0) {
        cachedData.sort(comparator);
        if (sortBufferManager.allocate(cachedBytes)) {
          sortReaders.add(new MemoryReader(cachedData));
        } else {
          sortBufferManager.allocateOneSortBranch();
          diskSpiller.spillSortedData(cachedData);
          cachedData = null;
        }
      }
      sortReaders.addAll(diskSpiller.getReaders(sortBufferManager));
      // if reader is finished
      noMoreData = new boolean[sortReaders.size()];
      // need to read data from reader when isEmpty is true
      isEmpty = new boolean[sortReaders.size()];
      Arrays.fill(isEmpty, true);
    } catch (Exception e) {
      throw new IoTDBException(e.getMessage(), TSStatusCode.INTERNAL_SERVER_ERROR.getStatusCode());
    }
  }

  private void cacheTsBlock(TsBlock tsBlock) throws IoTDBException {
    long bytesSize = tsBlock.getRetainedSizeInBytes();
    if (bytesSize + cachedBytes < sortBufferManager.SORT_BUFFER_SIZE) {
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
    try {
      // if current memory cannot put this tsBlock, an exception will be thrown in spillSortedData()
      // because there should be at least tsBlockBuilderStatus.DEFAULT_MAX_TSBLOCK_SIZE_IN_BYTES for
      // one branch.
      sortBufferManager.allocateOneSortBranch();
      diskSpiller.spillSortedData(cachedData);
    } catch (IOException e) {
      throw new IoTDBException(e.getMessage(), TSStatusCode.INTERNAL_SERVER_ERROR.getStatusCode());
    }
  }

  private TsBlock buildTsBlockInMemory() {
    tsBlockBuilder.reset();
    TimeColumnBuilder timeColumnBuilder = tsBlockBuilder.getTimeColumnBuilder();
    ColumnBuilder[] valueColumnBuilders = tsBlockBuilder.getValueColumnBuilders();
    for (int i = curRow; i < cachedData.size(); i++) {
      MergeSortKey mergeSortKey = cachedData.get(i);
      TsBlock tsBlock = mergeSortKey.tsBlock;
      timeColumnBuilder.writeLong(tsBlock.getTimeByIndex(mergeSortKey.rowIndex));
      for (int j = 0; j < valueColumnBuilders.length; j++) {
        if (tsBlock.getColumn(j).isNull(mergeSortKey.rowIndex)) {
          valueColumnBuilders[j].appendNull();
          continue;
        }
        valueColumnBuilders[j].write(tsBlock.getColumn(j), mergeSortKey.rowIndex);
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
        sortBufferManager.releaseOneSortBranch();
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
        sortBufferManager.releaseOneSortBranch();
      }

      // break if time is out or tsBlockBuilder is full or sortBuffer is not enough
      if (System.nanoTime() - startTime > maxRuntime || tsBlockBuilder.isFull()) {
        break;
      }
    }
    return tsBlockBuilder.build();
  }

  private MergeSortKey readNextMergeSortKey(int readerIndex) throws IoTDBException {
    SortReader sortReader = sortReaders.get(readerIndex);
    if (sortReader.hasNext()) {
      MergeSortKey mergeSortKey = sortReader.next();
      mergeSortKey.columnIndex = readerIndex;
      return mergeSortKey;
    }
    return null;
  }

  private boolean hasMoreData() {
    if (noMoreData == null) return true;
    for (boolean noMore : noMoreData) {
      if (!noMore) {
        return true;
      }
    }
    return false;
  }

  public void clear() {
    if (!diskSpiller.hasSpilledData()) return;
    try {
      diskSpiller.clear();
      Path path = Paths.get(filePrePath);
      Files.deleteIfExists(path);
      if (sortReaders != null) {
        for (SortReader sortReader : sortReaders) {
          sortReader.close();
        }
      }
    } catch (Exception e) {
      logger.error("delete spilled file error: {}", e.getMessage());
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
    cachedData = null;
    clear();
    inputOperator.close();
  }

  @Override
  public boolean isFinished() throws Exception {
    return inputOperator.isFinished()
        && (cachedData == null || cachedData.size() == curRow)
        && ((diskSpiller.hasSpilledData() && !hasMoreData()) || !diskSpiller.hasSpilledData());
  }

  @Override
  public long calculateMaxPeekMemory() {
    return inputOperator.calculateMaxPeekMemory()
        + inputOperator.calculateRetainedSizeAfterCallingNext()
        + sortBufferManager.SORT_BUFFER_SIZE;
  }

  @Override
  public long calculateMaxReturnSize() {
    return TsBlockBuilderStatus.DEFAULT_MAX_TSBLOCK_SIZE_IN_BYTES;
  }

  @Override
  public long calculateRetainedSizeAfterCallingNext() {
    return inputOperator.calculateRetainedSizeAfterCallingNext()
        + sortBufferManager.SORT_BUFFER_SIZE;
  }
}
