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

package org.apache.iotdb.db.utils.sort;

import org.apache.iotdb.commons.exception.IoTDBException;
import org.apache.iotdb.db.utils.datastructure.SortKey;
import org.apache.iotdb.rpc.TSStatusCode;

import org.apache.tsfile.block.column.ColumnBuilder;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.read.common.block.TsBlock;
import org.apache.tsfile.read.common.block.TsBlockBuilder;
import org.apache.tsfile.read.common.block.column.TsBlockSerde;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;

public abstract class DiskSpiller {

  private static final String FILE_SUFFIX = ".sortTemp";
  private final List<TSDataType> dataTypeList;
  private final String folderPath;
  private final String filePrefix;

  private int fileIndex;
  private boolean folderCreated = false;
  private final TsBlockSerde serde = new TsBlockSerde();

  DiskSpiller(String folderPath, String filePrefix, List<TSDataType> dataTypeList) {
    this.folderPath = folderPath;
    this.filePrefix = filePrefix + "-";
    this.fileIndex = 0;
    this.dataTypeList = dataTypeList;
  }

  private void createFolder(String folderPath) throws IOException {
    Path path = Paths.get(folderPath);
    Files.createDirectories(path);
    folderCreated = true;
  }

  private void spill(List<TsBlock> tsBlocks) throws IOException, IoTDBException {
    if (!folderCreated) {
      createFolder(folderPath);
    }
    String fileName = filePrefix + String.format("%05d", fileIndex) + FILE_SUFFIX;
    fileIndex++;

    writeData(tsBlocks, fileName);
  }

  /** todo: directly serialize the sorted line instead of copy into a new tsBlock. */
  public void spillSortedData(List<SortKey> sortedData) throws IoTDBException {
    List<TsBlock> tsBlocks = new ArrayList<>();
    TsBlockBuilder tsBlockBuilder = new TsBlockBuilder(dataTypeList);
    ColumnBuilder[] columnBuilders = tsBlockBuilder.getValueColumnBuilders();
    ColumnBuilder timeColumnBuilder = tsBlockBuilder.getTimeColumnBuilder();

    for (SortKey sortKey : sortedData) {
      writeSortKey(sortKey, columnBuilders, timeColumnBuilder);
      tsBlockBuilder.declarePosition();
      if (tsBlockBuilder.isFull()) {
        tsBlocks.add(buildSortedTsBlock(tsBlockBuilder));
        tsBlockBuilder.reset();
        timeColumnBuilder = tsBlockBuilder.getTimeColumnBuilder();
      }
    }

    if (!tsBlockBuilder.isEmpty()) {
      tsBlocks.add(buildSortedTsBlock(tsBlockBuilder));
    }

    try {
      spill(tsBlocks);
    } catch (IOException e) {
      throw new IoTDBException(
          "Create file error: " + filePrefix + (fileIndex - 1) + FILE_SUFFIX,
          e,
          TSStatusCode.INTERNAL_SERVER_ERROR.getStatusCode());
    }
  }

  protected abstract TsBlock buildSortedTsBlock(TsBlockBuilder resultBuilder);

  private void writeData(List<TsBlock> sortedData, String fileName) throws IoTDBException {
    Path filePath = Paths.get(fileName);
    // for stream sort we may reuse the previous tmp file name, so we need TRUNCATE_EXISTING and
    // CREATE
    try (FileChannel fileChannel =
        FileChannel.open(
            filePath,
            StandardOpenOption.WRITE,
            StandardOpenOption.TRUNCATE_EXISTING,
            StandardOpenOption.CREATE)) {
      for (TsBlock tsBlock : sortedData) {
        ByteBuffer tsBlockBuffer = serde.serialize(tsBlock);
        ByteBuffer length = ByteBuffer.allocate(4);
        length.putInt(tsBlockBuffer.capacity());
        length.flip();
        fileChannel.write(length);
        fileChannel.write(tsBlockBuffer);
      }
    } catch (IOException e) {
      throw new IoTDBException(
          "Can't write intermediate sorted data to file: " + fileName,
          e,
          TSStatusCode.INTERNAL_SERVER_ERROR.getStatusCode());
    }
  }

  private void writeSortKey(
      SortKey sortKey, ColumnBuilder[] columnBuilders, ColumnBuilder timeColumnBuilder) {
    appendTime(timeColumnBuilder, sortKey.tsBlock.getTimeByIndex(sortKey.rowIndex));
    for (int i = 0; i < columnBuilders.length; i++) {
      if (sortKey.tsBlock.getColumn(i).isNull(sortKey.rowIndex)) {
        columnBuilders[i].appendNull();
      } else {
        columnBuilders[i].write(sortKey.tsBlock.getColumn(i), sortKey.rowIndex);
      }
    }
  }

  protected abstract void appendTime(ColumnBuilder timeColumnBuilder, long time);

  public boolean hasSpilledData() {
    return fileIndex != 0;
  }

  private List<String> getFilePaths() {
    List<String> filePaths = new ArrayList<>();
    for (int i = 0; i < fileIndex; i++) {
      filePaths.add(filePrefix + String.format("%05d", i) + FILE_SUFFIX);
    }
    return filePaths;
  }

  public List<SortReader> getReaders(SortBufferManager sortBufferManager) throws IoTDBException {
    List<String> filePaths = getFilePaths();
    List<SortReader> sortReaders = new ArrayList<>();
    try {
      for (String filePath : filePaths) {
        sortReaders.add(new FileSpillerReader(filePath, sortBufferManager, serde));
      }
    } catch (IOException e) {
      throw new IoTDBException(
          "Can't get file for FileSpillerReader, check if the file exists: " + filePaths,
          e,
          TSStatusCode.INTERNAL_SERVER_ERROR.getStatusCode());
    }
    return sortReaders;
  }

  public int getFileSize() {
    return fileIndex;
  }

  public void reset() {
    fileIndex = 0;
  }
}
