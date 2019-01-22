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
package org.apache.iotdb.db.engine.bufferwrite;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.iotdb.tsfile.file.metadata.ChunkGroupMetaData;
import org.apache.iotdb.tsfile.file.metadata.ChunkMetaData;
import org.apache.iotdb.tsfile.file.metadata.TsDeviceMetadata;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.utils.BytesUtils;
import org.apache.iotdb.tsfile.utils.Pair;
import org.apache.iotdb.tsfile.write.schema.FileSchema;
import org.apache.iotdb.tsfile.write.writer.DefaultTsFileOutput;
import org.apache.iotdb.tsfile.write.writer.TsFileIOWriter;
import org.apache.iotdb.tsfile.write.writer.TsFileOutput;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A tsfile io writer that has the ability to restore an incomplete tsfile. <br/> An incomplete
 * tsfile represents the file which does not have tsfile metadata in the end. Besides, the last
 * Chunk group data may be broken. This class can slice off the broken Chunk group data, accept
 * writing new data, and finally write the tsfile metadata. <br/> There are two cases: (1) though
 * the tsfile loses the tsfile metadata in the end, a corresponding. restore file exists. (2) no
 * .restore file, and then the class has to traverse all the data for fixing the file.
 */
public class RestorableTsFileIOWriter extends TsFileIOWriter {

  private static final Logger LOGGER = LoggerFactory.getLogger(RestorableTsFileIOWriter.class);

  private static final int TS_METADATA_BYTE_SIZE = 4;
  private static final int TS_POSITION_BYTE_SIZE = 8;

  private static final String RESTORE_SUFFIX = ".restore";
  private static final String DEFAULT_MODE = "rw";

  private int lastFlushedChunkGroupIndex = 0;
  /**
   * chunk group metadata which are not serialized on disk (.restore file).
   */
  private List<ChunkGroupMetaData> append;

  /**
   * all chunk group metadata which have been serialized on disk.
   */
  private Map<String, Map<String, List<ChunkMetaData>>> metadatas;

  /**
   * unsealed data file.
   */
  private String insertFilePath;
  /**
   * corresponding index file.
   */
  private String restoreFilePath;

  private boolean isNewResource = false;

  RestorableTsFileIOWriter(String processorName, String insertFilePath) throws IOException {
    super();
    this.insertFilePath = insertFilePath;
    this.restoreFilePath = insertFilePath + RESTORE_SUFFIX;

    this.metadatas = new HashMap<>();

    File insertFile = new File(insertFilePath);
    File restoreFile = new File(restoreFilePath);
    if (insertFile.exists() && restoreFile.exists()) {
      // read restore file
      Pair<Long, List<ChunkGroupMetaData>> restoreInfo = readRestoreInfo();
      long position = restoreInfo.left;
      List<ChunkGroupMetaData> existedMetadatas = restoreInfo.right;
      // cut off tsfile
      this.out = new DefaultTsFileOutput(new FileOutputStream(insertFile, true));
      out.truncate(position);
      this.chunkGroupMetaDataList = existedMetadatas;
      lastFlushedChunkGroupIndex = chunkGroupMetaDataList.size();
      append = new ArrayList<>();
      // recovery the metadata
      recoverMetadata(existedMetadatas);
      LOGGER.info(
          "Recover the bufferwrite processor {}, the tsfile seriesPath is {}, "
              + "the position of last flush is {}, the size of rowGroupMetadata is {}",
          processorName, insertFilePath, position, existedMetadatas.size());
      isNewResource = false;
    } else {
      try {
        Files.deleteIfExists(insertFile.toPath());
      } catch (IOException e) {
        LOGGER.info("remove unsealed tsfile  failed: {}", e.getMessage());
      }
      try {
        Files.deleteIfExists(restoreFile.toPath());
      } catch (IOException e) {
        LOGGER.info("remove unsealed tsfile restore file failed: {}", e.getMessage());
      }

      this.out = new DefaultTsFileOutput(new FileOutputStream(insertFile));
      this.chunkGroupMetaDataList = new ArrayList<>();
      lastFlushedChunkGroupIndex = chunkGroupMetaDataList.size();
      append = new ArrayList<>();
      startFile();
      isNewResource = true;
      writeRestoreInfo();
    }

  }

  private void recoverMetadata(List<ChunkGroupMetaData> rowGroupMetaDatas) {
    // TODO it is better if we can consider the problem caused by deletion
    // and re-create time series here.
    for (ChunkGroupMetaData rowGroupMetaData : rowGroupMetaDatas) {
      String deviceId = rowGroupMetaData.getDeviceID();
      if (!metadatas.containsKey(deviceId)) {
        metadatas.put(deviceId, new HashMap<>());
      }
      for (ChunkMetaData chunkMetaData : rowGroupMetaData.getChunkMetaDataList()) {
        String measurementId = chunkMetaData.getMeasurementUid();
        if (!metadatas.get(deviceId).containsKey(measurementId)) {
          metadatas.get(deviceId).put(measurementId, new ArrayList<>());
        }
        metadatas.get(deviceId).get(measurementId).add(chunkMetaData);
      }
    }
  }

  private void writeRestoreInfo() throws IOException {
    long lastPosition = this.getPos();

    // TODO: no need to create a TsRowGroupBlockMetadata, flush RowGroupMetadata one by one is ok
    TsDeviceMetadata tsDeviceMetadata = new TsDeviceMetadata();
    this.getAppendedRowGroupMetadata();
    tsDeviceMetadata.setChunkGroupMetadataList(this.append);
    RandomAccessFile out = null;
    out = new RandomAccessFile(restoreFilePath, DEFAULT_MODE);
    try {
      if (out.length() > 0) {
        out.seek(out.length() - TS_POSITION_BYTE_SIZE);
      }
      ByteArrayOutputStream baos = new ByteArrayOutputStream();
      tsDeviceMetadata.serializeTo(baos);
      // write metadata size using int
      int metadataSize = baos.size();
      out.write(BytesUtils.intToBytes(metadataSize));
      // write metadata
      out.write(baos.toByteArray());
      // write tsfile position using byte[8] which is a long
      byte[] lastPositionBytes = BytesUtils.longToBytes(lastPosition);
      out.write(lastPositionBytes);
    } finally {
      out.close();
    }
  }

  /**
   * This is a private method. (It is default now for Unit Test only)
   *
   * @return a pair, whose left Long value is the tail position of the last complete Chunk Group in
   * the unsealed file's position, and the right List value is the ChunkGroupMetadata of all
   * complete Chunk Group in the same file.
   * @throws IOException if errors when reading restoreFile.
   */
  Pair<Long, List<ChunkGroupMetaData>> readRestoreInfo() throws IOException {
    byte[] lastPostionBytes = new byte[TS_POSITION_BYTE_SIZE];
    List<ChunkGroupMetaData> groupMetaDatas = new ArrayList<>();
    RandomAccessFile randomAccessFile = null;
    randomAccessFile = new RandomAccessFile(restoreFilePath, DEFAULT_MODE);
    try {
      long fileLength = randomAccessFile.length();
      // read tsfile position
      long point = randomAccessFile.getFilePointer();
      while (point + TS_POSITION_BYTE_SIZE < fileLength) {
        byte[] metadataSizeBytes = new byte[TS_METADATA_BYTE_SIZE];
        randomAccessFile.read(metadataSizeBytes);
        int metadataSize = BytesUtils.bytesToInt(metadataSizeBytes);
        byte[] thriftBytes = new byte[metadataSize];
        randomAccessFile.read(thriftBytes);
        ByteArrayInputStream inputStream = new ByteArrayInputStream(thriftBytes);
        TsDeviceMetadata tsDeviceMetadata = TsDeviceMetadata.deserializeFrom(inputStream);
        groupMetaDatas.addAll(tsDeviceMetadata.getChunkGroups());
        point = randomAccessFile.getFilePointer();
      }
      // read the tsfile position information using byte[8] which is a long.
      randomAccessFile.read(lastPostionBytes);
      long lastPosition = BytesUtils.bytesToLong(lastPostionBytes);
      return new Pair<>(lastPosition, groupMetaDatas);
    } finally {
      randomAccessFile.close();
    }
  }

  /**
   * get chunks' metadata from memory.
   *
   * @param deviceId the device id
   * @param measurementId the sensor id
   * @param dataType the value type
   * @return chunks' metadata
   */
  List<ChunkMetaData> getMetadatas(String deviceId, String measurementId, TSDataType dataType) {
    List<ChunkMetaData> chunkMetaDatas = new ArrayList<>();
    if (metadatas.containsKey(deviceId)) {
      if (metadatas.get(deviceId).containsKey(measurementId)) {
        for (ChunkMetaData chunkMetaData : metadatas.get(deviceId).get(measurementId)) {
          // filter: if a device'sensor is defined as float type, and data has been persistent.
          // Then someone deletes the timeseries and recreate it with Int type. We have to ignore
          // all the stale data.
          if (dataType.equals(chunkMetaData.getTsDataType())) {
            chunkMetaDatas.add(chunkMetaData);
          }
        }
      }
    }
    return chunkMetaDatas;
  }

  String getInsertFilePath() {
    return insertFilePath;
  }

  String getRestoreFilePath() {
    return restoreFilePath;
  }

  boolean isNewResource() {
    return isNewResource;
  }

  void setNewResource(boolean isNewResource) {
    this.isNewResource = isNewResource;
  }

  public void flush() throws IOException {
    writeRestoreInfo();
  }

  /**
   * add all appendChunkGroupMetadatas into memory. After calling this method, other classes can
   * read these metadata.
   */
  void appendMetadata() {
    if (!append.isEmpty()) {
      for (ChunkGroupMetaData rowGroupMetaData : append) {
        for (ChunkMetaData chunkMetaData : rowGroupMetaData.getChunkMetaDataList()) {
          addInsertMetadata(rowGroupMetaData.getDeviceID(), chunkMetaData.getMeasurementUid(),
              chunkMetaData);
        }
      }
      append.clear();
    }
  }

  private void addInsertMetadata(String deviceId, String measurementId,
      ChunkMetaData chunkMetaData) {
    if (!metadatas.containsKey(deviceId)) {
      metadatas.put(deviceId, new HashMap<>());
    }
    if (!metadatas.get(deviceId).containsKey(measurementId)) {
      metadatas.get(deviceId).put(measurementId, new ArrayList<>());
    }
    metadatas.get(deviceId).get(measurementId).add(chunkMetaData);
  }

  @Override
  public void endFile(FileSchema schema) throws IOException {
    super.endFile(schema);
    try {
      Files.delete(Paths.get(restoreFilePath));
    } catch (IOException e) {
      LOGGER.info("delete restore file {} failed, because {}.", restoreFilePath, e.getMessage());
    }
  }

  /**
   * get all the chunkGroups' metadata which are appended after the last calling of this method, or
   * after the class instance is initialized if this is the first time to call the method.
   *
   * @return a list of chunkgroup metadata
   */
  private List<ChunkGroupMetaData> getAppendedRowGroupMetadata() {
    if (lastFlushedChunkGroupIndex < chunkGroupMetaDataList.size()) {
      append.clear();
      append.addAll(chunkGroupMetaDataList
          .subList(lastFlushedChunkGroupIndex, chunkGroupMetaDataList.size()));
      lastFlushedChunkGroupIndex = chunkGroupMetaDataList.size();
    }
    return append;
  }

  /**
   * see {@link java.nio.channels.FileChannel#truncate(long)}.
   */
  public void truncate(long position) throws IOException {
    out.truncate(position);
  }

  /**
   * just for test.
   *
   * @return the output
   */
  TsFileOutput getOutput() {
    return out;
  }

}