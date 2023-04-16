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

package org.apache.iotdb.db.tools;

import org.apache.iotdb.db.utils.datastructure.MergeSortKey;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.common.block.TsBlock;
import org.apache.iotdb.tsfile.read.common.block.TsBlockBuilder;
import org.apache.iotdb.tsfile.read.common.block.column.ColumnBuilder;
import org.apache.iotdb.tsfile.read.common.block.column.TsBlockSerde;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;

import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;

import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Future;

import static org.weakref.jmx.internal.guava.util.concurrent.MoreExecutors.directExecutor;

public class DiskSpiller {
  private final List<Integer> fileIndex;
  private final List<TSDataType> dataTypeList;
  private final String filePrefix;
  private int index;
  private final List<ListenableFuture<?>> processingTask;

  public DiskSpiller(String filePrefix, List<TSDataType> dataTypeList) {
    this.filePrefix = filePrefix + this.getClass().getSimpleName();
    this.index = 0;
    this.dataTypeList = dataTypeList;
    this.fileIndex = new ArrayList<>();
    this.processingTask = new ArrayList<>();
  }

  public boolean allProcessingTaskFinished() {
    for (Future<?> future : processingTask) {
      if (!future.isDone()) return false;
    }
    return true;
  }

  public synchronized void spill(List<TsBlock> tsBlocks) {
    String fileName = filePrefix + index;
    fileIndex.add(index);
    index++;
    SortBufferManager.allocateOneSortBranch();

    ListenableFuture<?> future =
        Futures.submit(
            () -> {
              try {
                writeData(tsBlocks, fileName);
              } catch (IOException e) {
                throw new RuntimeException(e);
              }
            },
            directExecutor());
    processingTask.add(future);
  }

  public void spillSortedData(List<MergeSortKey> sortedData) {
    List<TsBlock> tsBlocks = new ArrayList<>();
    TsBlockBuilder tsBlockBuilder = new TsBlockBuilder(dataTypeList);
    ColumnBuilder[] columnBuilders = tsBlockBuilder.getValueColumnBuilders();
    ColumnBuilder timeColumnBuilder = tsBlockBuilder.getTimeColumnBuilder();
    for (MergeSortKey mergeSortKey : sortedData) {
      writeMergeSortKey(mergeSortKey, columnBuilders, timeColumnBuilder);
      tsBlockBuilder.declarePosition();
      if (tsBlockBuilder.isFull()) {
        tsBlocks.add(tsBlockBuilder.build());
        tsBlockBuilder.reset();
      }
    }
    spill(tsBlocks);
  }

  private void writeData(List<TsBlock> sortedData, String fileName) throws IOException {
    try (FileOutputStream fileOutputStream = new FileOutputStream(fileName)) {
      for (TsBlock tsBlock : sortedData) {
        ByteBuffer tsBlockBuffer = TsBlockSerde.serialize(tsBlock);
        ReadWriteIOUtils.write(tsBlockBuffer, fileOutputStream);
      }
    }
  }

  private void writeMergeSortKey(
      MergeSortKey mergeSortKey, ColumnBuilder[] columnBuilders, ColumnBuilder timeColumnBuilder) {
    timeColumnBuilder.writeLong(mergeSortKey.tsBlock.getTimeByIndex(mergeSortKey.rowIndex));
    for (int i = 0; i < columnBuilders.length; i++) {
      if (mergeSortKey.tsBlock.getColumn(i).isNull(mergeSortKey.rowIndex)) {
        columnBuilders[i].appendNull();
      } else {
        columnBuilders[i].write(mergeSortKey.tsBlock.getColumn(i), mergeSortKey.rowIndex);
      }
    }
  }

  public boolean hasSpilledData() {
    return !fileIndex.isEmpty();
  }

  public List<String> getFilePaths() {
    List<String> filePaths = new ArrayList<>();
    for (int index : fileIndex) {
      filePaths.add(filePrefix + index);
    }
    return filePaths;
  }

  public List<SortReader> getReaders() throws FileNotFoundException {
    List<String> filePaths = getFilePaths();
    List<SortReader> sortReaders = new ArrayList<>();
    for (String filePath : filePaths) {
      sortReaders.add(new FileSpillerReader(filePath));
    }
    return sortReaders;
  }

  public void clear() {
    // todo delete the spilled file
  }
}
