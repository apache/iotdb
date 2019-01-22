/**
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
package org.apache.iotdb.db.engine.overflow.ioV2;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.commons.io.FileUtils;
import org.apache.iotdb.db.engine.memtable.IMemTable;
import org.apache.iotdb.db.engine.memtable.MemTableFlushUtil;
import org.apache.iotdb.db.utils.MemUtils;
import org.apache.iotdb.tsfile.file.metadata.ChunkGroupMetaData;
import org.apache.iotdb.tsfile.file.metadata.ChunkMetaData;
import org.apache.iotdb.tsfile.file.metadata.TsDeviceMetadata;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.utils.BytesUtils;
import org.apache.iotdb.tsfile.utils.Pair;
import org.apache.iotdb.tsfile.write.schema.FileSchema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class OverflowResource {

  private static final Logger LOGGER = LoggerFactory.getLogger(OverflowResource.class);
  private static final String insertFileName = "unseqTsFile";
  private static final String updateDeleteFileName = "overflowFile";
  private static final String positionFileName = "positionFile";
  private static final int FOOTER_LENGTH = 4;
  private static final int POS_LENGTH = 8;
  private String parentPath;
  private String dataPath;
  private String insertFilePath;
  private String positionFilePath;
  private File insertFile;
  private File updateFile;
  private OverflowIO insertIO;
  private Map<String, Map<String, List<ChunkMetaData>>> insertMetadatas;
  private List<ChunkGroupMetaData> appendInsertMetadatas;

  public OverflowResource(String parentPath, String dataPath) throws IOException {
    this.insertMetadatas = new HashMap<>();
    this.appendInsertMetadatas = new ArrayList<>();
    this.parentPath = parentPath;
    this.dataPath = dataPath;
    File dataFile = new File(parentPath, dataPath);
    if (!dataFile.exists()) {
      dataFile.mkdirs();
    }
    insertFile = new File(dataFile, insertFileName);
    insertFilePath = insertFile.getPath();
    updateFile = new File(dataFile, updateDeleteFileName);
    positionFilePath = new File(dataFile, positionFileName).getPath();
    Pair<Long, Long> position = readPositionInfo();
    try {
      // insert stream
      OverflowIO.OverflowReadWriter readWriter = new OverflowIO.OverflowReadWriter(insertFilePath);
      // truncate
      readWriter.wrapAsFileChannel().truncate(position.left);
      // reposition
      // seek to zero
      readWriter.wrapAsFileChannel().position(0);
      // seek to tail
      // the tail is at least the len of magic string
      insertIO = new OverflowIO(readWriter);
      readMetadata();
    } catch (IOException e) {
      LOGGER.error("Failed to construct the OverflowIO.", e);
      throw e;
    }
  }

  private Pair<Long, Long> readPositionInfo() {
    try {
      FileInputStream inputStream = new FileInputStream(positionFilePath);
      byte[] insertPositionData = new byte[8];
      byte[] updatePositionData = new byte[8];
      inputStream.read(insertPositionData);
      inputStream.read(updatePositionData);
      long lastInsertPosition = BytesUtils.bytesToLong(insertPositionData);
      long lastUpdatePosition = BytesUtils.bytesToLong(updatePositionData);
      inputStream.close();
      return new Pair<Long, Long>(lastInsertPosition, lastUpdatePosition);
    } catch (IOException e) {
      long left = 0;
      long right = 0;
      File insertFile = new File(insertFilePath);
      if (insertFile.exists()) {
        left = insertFile.length();
      }
      if (updateFile.exists()) {
        right = updateFile.length();
      }
      return new Pair<Long, Long>(left, right);
    }
  }

  private void writePositionInfo(long lastInsertPosition, long lastUpdatePosition)
      throws IOException {
    FileOutputStream outputStream = new FileOutputStream(positionFilePath);
    byte[] data = new byte[16];
    BytesUtils.longToBytes(lastInsertPosition, data, 0);
    BytesUtils.longToBytes(lastUpdatePosition, data, 8);
    outputStream.write(data);
    outputStream.close();
  }

  private void readMetadata() throws IOException {
    // read insert meta-data
    insertIO.toTail();
    long position = insertIO.getPos();
    while (position != insertIO.magicStringBytes.length) {
      insertIO.getReader().position(position - FOOTER_LENGTH);
      int metadataLength = insertIO.getReader().readInt();
      byte[] buf = new byte[metadataLength];
      insertIO.getReader().position(position - FOOTER_LENGTH - metadataLength);
      insertIO.getReader().read(buf, 0, buf.length);
      ByteArrayInputStream inputStream = new ByteArrayInputStream(buf);
      TsDeviceMetadata tsDeviceMetadata = TsDeviceMetadata.deserializeFrom(inputStream);
      byte[] bytesPosition = new byte[8];
      insertIO.getReader().position(position - FOOTER_LENGTH - metadataLength - POS_LENGTH);
      insertIO.getReader().read(bytesPosition, 0, POS_LENGTH);
      position = BytesUtils.bytesToLong(bytesPosition);
      for (ChunkGroupMetaData rowGroupMetaData : tsDeviceMetadata.getChunkGroups()) {
        String deviceId = rowGroupMetaData.getDeviceID();
        if (!insertMetadatas.containsKey(deviceId)) {
          insertMetadatas.put(deviceId, new HashMap<>());
        }
        for (ChunkMetaData chunkMetaData : rowGroupMetaData.getChunkMetaDataList()) {
          String measurementId = chunkMetaData.getMeasurementUid();
          if (!insertMetadatas.get(deviceId).containsKey(measurementId)) {
            insertMetadatas.get(deviceId).put(measurementId, new ArrayList<>());
          }
          insertMetadatas.get(deviceId).get(measurementId).add(0, chunkMetaData);
        }
      }
    }
  }

  public List<ChunkMetaData> getInsertMetadatas(String deviceId, String measurementId,
      TSDataType dataType) {
    List<ChunkMetaData> chunkMetaDatas = new ArrayList<>();
    if (insertMetadatas.containsKey(deviceId)) {
      if (insertMetadatas.get(deviceId).containsKey(measurementId)) {
        for (ChunkMetaData chunkMetaData : insertMetadatas.get(deviceId).get(measurementId)) {
          // filter
          if (chunkMetaData.getTsDataType().equals(dataType)) {
            chunkMetaDatas.add(chunkMetaData);
          }
        }
      }
    }
    return chunkMetaDatas;
  }

  public void flush(FileSchema fileSchema, IMemTable memTable,
      Map<String, Map<String, OverflowSeriesImpl>> overflowTrees, String processorName)
      throws IOException {
    // insert data
    long startPos = insertIO.getPos();
    long startTime = System.currentTimeMillis();
    flush(fileSchema, memTable);
    long timeInterval = System.currentTimeMillis() - startTime;
    timeInterval = timeInterval == 0 ? 1 : timeInterval;
    long insertSize = insertIO.getPos() - startPos;
    LOGGER.info(
        "Overflow processor {} flushes overflow insert data, actual:{}, time consumption:{} ms, flush rate:{}/s",
        processorName, MemUtils.bytesCntToStr(insertSize), timeInterval,
        MemUtils.bytesCntToStr(insertSize / timeInterval * 1000));
    writePositionInfo(insertIO.getPos(), 0);
  }

  public void flush(FileSchema fileSchema, IMemTable memTable) throws IOException {
    if (memTable != null && !memTable.isEmpty()) {
      insertIO.toTail();
      long lastPosition = insertIO.getPos();
      MemTableFlushUtil.flushMemTable(fileSchema, insertIO, memTable);
      List<ChunkGroupMetaData> rowGroupMetaDatas = insertIO.getChunkGroupMetaDatas();
      appendInsertMetadatas.addAll(rowGroupMetaDatas);
      if (!rowGroupMetaDatas.isEmpty()) {
        insertIO.getWriter().write(BytesUtils.longToBytes(lastPosition));
        TsDeviceMetadata tsDeviceMetadata = new TsDeviceMetadata();
        tsDeviceMetadata.setChunkGroupMetadataList(rowGroupMetaDatas);
        long start = insertIO.getPos();
        tsDeviceMetadata.serializeTo(insertIO.getWriter());
        long end = insertIO.getPos();
        insertIO.getWriter().write(BytesUtils.intToBytes((int) (end - start)));
        // clear the meta-data of insert IO
        insertIO.clearRowGroupMetadatas();
      }
    }
  }

  public void appendMetadatas() {
    if (!appendInsertMetadatas.isEmpty()) {
      for (ChunkGroupMetaData rowGroupMetaData : appendInsertMetadatas) {
        for (ChunkMetaData seriesChunkMetaData : rowGroupMetaData.getChunkMetaDataList()) {
          addInsertMetadata(rowGroupMetaData.getDeviceID(), seriesChunkMetaData.getMeasurementUid(),
              seriesChunkMetaData);
        }
      }
      appendInsertMetadatas.clear();
    }
  }

  public String getInsertFilePath() {
    return insertFilePath;
  }

  public File getInsertFile() {
    return insertFile;
  }

  public String getPositionFilePath() {
    return positionFilePath;
  }

  public File getUpdateDeleteFile() {
    return updateFile;
  }

  public void close() throws IOException {
    insertMetadatas.clear();
    // updateDeleteMetadatas.clear();
    insertIO.close();
    // updateDeleteIO.close();
  }

  public void deleteResource() throws IOException {
    // cleanDir(new File(parentPath, dataPath).getPath());
    FileUtils.forceDelete(new File(parentPath, dataPath));
  }

  private void cleanDir(String dir) throws IOException {
    File file = new File(dir);
    if (file.exists()) {
      if (file.isDirectory()) {
        for (File subFile : file.listFiles()) {
          cleanDir(subFile.getAbsolutePath());
        }
      }
      if (!file.delete()) {
        throw new IOException(String.format("The file %s can't be deleted", dir));
      }
    }
  }

  private void addInsertMetadata(String deviceId, String measurementId,
      ChunkMetaData chunkMetaData) {
    if (!insertMetadatas.containsKey(deviceId)) {
      insertMetadatas.put(deviceId, new HashMap<>());
    }
    if (!insertMetadatas.get(deviceId).containsKey(measurementId)) {
      insertMetadatas.get(deviceId).put(measurementId, new ArrayList<>());
    }
    insertMetadatas.get(deviceId).get(measurementId).add(chunkMetaData);
  }
}