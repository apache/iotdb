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

package org.apache.iotdb.calc.utils.sort;

import org.apache.iotdb.calc.i18n.CalcMessages;
import org.apache.iotdb.calc.utils.datastructure.SortKey;
import org.apache.iotdb.commons.exception.IoTDBException;
import org.apache.iotdb.rpc.TSStatusCode;

import org.apache.tsfile.block.column.ColumnBuilder;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.read.common.block.TsBlock;
import org.apache.tsfile.read.common.block.TsBlockBuilder;
import org.apache.tsfile.read.common.block.column.TsBlockSerde;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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

  private static final Logger LOGGER = LoggerFactory.getLogger(DiskSpiller.class);

  private static final String FILE_SUFFIX = ".sortTemp";

  /** Bytes reserved ahead of each spilled TsBlock payload (serialized length prefix). */
  private static final int LENGTH_PREFIX_BYTES = Integer.BYTES;

  private final List<TSDataType> dataTypeList;
  private final String folderPath;
  private final String filePrefix;

  private int fileIndex;
  private boolean folderCreated = false;
  private final TsBlockSerde serde = new TsBlockSerde();

  /** Session userId for TEMP_DISK quota accounting; negative means no accounting (exempt). */
  private long quotaUserId = -1;

  /** Injected gate; null disables TEMP_DISK accounting (tests / non-DataNode contexts). */
  private TempDiskSpillQuotaGate tempDiskSpillQuotaGate;

  /** TEMP_DISK bytes currently charged for this spiller's files. */
  private long chargedBytes;

  DiskSpiller(String folderPath, String filePrefix, List<TSDataType> dataTypeList) {
    this.folderPath = folderPath;
    this.filePrefix = filePrefix + "-";
    this.fileIndex = 0;
    this.dataTypeList = dataTypeList;
  }

  /** Set by the sort operator so spilled bytes are charged to the session user's TEMP_DISK. */
  public void setQuotaUserId(long quotaUserId) {
    this.quotaUserId = quotaUserId;
  }

  public void setTempDiskSpillQuotaGate(TempDiskSpillQuotaGate gate) {
    this.tempDiskSpillQuotaGate = gate;
  }

  private void chargeTempDisk(long bytes) throws IoTDBException {
    if (tempDiskSpillQuotaGate == null || quotaUserId < 0 || bytes <= 0) {
      return;
    }
    tempDiskSpillQuotaGate.acquire(quotaUserId, bytes);
    chargedBytes += bytes;
  }

  private void releaseTempDisk() {
    releaseTempDiskAmount(chargedBytes);
    chargedBytes = 0;
  }

  /** Release a portion of charged TEMP_DISK without clearing earlier successful spills. */
  private void releaseTempDiskAmount(long bytes) {
    if (bytes <= 0) {
      return;
    }
    long bytesToRelease = Math.min(bytes, chargedBytes);
    if (bytes > chargedBytes) {
      LOGGER.warn(
          CalcMessages.LOG_ATTEMPTED_TO_RELEASE_TEMP_DISK_BYTES_BUT_ONLY_WERE_CHARGED_DC962BD1,
          bytes,
          chargedBytes);
    }
    if (tempDiskSpillQuotaGate != null && quotaUserId >= 0 && bytesToRelease > 0) {
      tempDiskSpillQuotaGate.release(quotaUserId, bytesToRelease);
    }
    // Defensive clamp: never let chargedBytes go negative if release races with partial failure.
    chargedBytes = Math.max(0, chargedBytes - bytesToRelease);
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
    } catch (Exception e) {
      // IO after a successful charge still holds TEMP_DISK until reset(); release eagerly here.
      // Physical spill files are removed by the sort operator clear/reset path.
      releaseTempDisk();
      if (e instanceof IoTDBException) {
        throw (IoTDBException) e;
      }
      throw new IoTDBException(
          CalcMessages.EXCEPTION_CREATE_FILE_ERROR_B8B379CF
              + filePrefix
              + (fileIndex - 1)
              + FILE_SUFFIX,
          e,
          TSStatusCode.INTERNAL_SERVER_ERROR.getStatusCode());
    }
  }

  protected abstract TsBlock buildSortedTsBlock(TsBlockBuilder resultBuilder);

  /**
   * Serialize spilled TsBlocks and charge TEMP_DISK for length-prefix + payload; on failure unwind
   * only this attempt's charge. TEMP_DISK quota tokens are released in catch/reset; physical files
   * are cleaned by the sort operator clear/reset path.
   */
  private void writeData(List<TsBlock> sortedData, String fileName) throws IoTDBException {
    Path filePath = Paths.get(fileName);
    // for stream sort we may reuse the previous tmp file name, so we need TRUNCATE_EXISTING and
    // CREATE
    long chargedBefore = chargedBytes;
    try (FileChannel fileChannel =
        FileChannel.open(
            filePath,
            StandardOpenOption.WRITE,
            StandardOpenOption.TRUNCATE_EXISTING,
            StandardOpenOption.CREATE)) {
      for (TsBlock tsBlock : sortedData) {
        ByteBuffer tsBlockBuffer = serde.serialize(tsBlock);
        // Charge TEMP_DISK quota for exactly the bytes that hit the disk (length header + payload);
        // throws when the user quota or node capacity would be exceeded.
        chargeTempDisk(LENGTH_PREFIX_BYTES + (long) tsBlockBuffer.capacity());
        ByteBuffer length = ByteBuffer.allocate(LENGTH_PREFIX_BYTES);
        length.putInt(tsBlockBuffer.capacity());
        length.flip();
        fileChannel.write(length);
        fileChannel.write(tsBlockBuffer);
      }
    } catch (Exception e) {
      // Only unwind charges from this write attempt; keep earlier successful spill files accounted.
      releaseTempDiskAmount(chargedBytes - chargedBefore);
      if (e instanceof IoTDBException) {
        throw (IoTDBException) e;
      }
      throw new IoTDBException(
          CalcMessages.EXCEPTION_CAN_T_WRITE_INTERMEDIATE_SORTED_DATA_FILE_0027961E + fileName,
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
          CalcMessages.EXCEPTION_CAN_T_GET_FILE_FILESPILLERREADER_CHECK_IF_FILE_EXISTS_DEED83D9
              + filePaths,
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
    releaseTempDisk();
  }
}
