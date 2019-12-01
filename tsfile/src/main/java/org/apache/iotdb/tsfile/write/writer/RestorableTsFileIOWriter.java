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

package org.apache.iotdb.tsfile.write.writer;

import java.io.File;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.iotdb.tsfile.common.conf.TSFileConfig;
import org.apache.iotdb.tsfile.file.metadata.ChunkGroupMetaData;
import org.apache.iotdb.tsfile.file.metadata.ChunkMetaData;
import org.apache.iotdb.tsfile.file.metadata.TsDeviceMetadataIndex;
import org.apache.iotdb.tsfile.file.metadata.TsFileMetaData;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.fileSystem.FSFactoryProducer;
import org.apache.iotdb.tsfile.read.TsFileCheckStatus;
import org.apache.iotdb.tsfile.read.TsFileSequenceReader;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * a restorable tsfile.
 */
public class RestorableTsFileIOWriter extends TsFileIOWriter {

  private static final Logger logger = LoggerFactory
      .getLogger(RestorableTsFileIOWriter.class);

  private long truncatedPosition = -1;
  private Map<String, MeasurementSchema> knownSchemas = new HashMap<>();

  private int lastFlushedChunkGroupIndex = 0;

  private boolean crashed;

  /**
   * all chunk group metadata which have been serialized on disk.
   */
  private Map<String, Map<String, List<ChunkMetaData>>> metadatas = new HashMap<>();


  long getTruncatedPosition() {
    return truncatedPosition;
  }

  /**
   * @param file a given tsfile path you want to (continue to) write
   * @throws IOException if write failed, or the file is broken but autoRepair==false.
   */
  public RestorableTsFileIOWriter(File file) throws IOException {
    this.file = file;
    this.out = FSFactoryProducer.getFileOutputFactory().getTsFileOutput(file.getPath(), true);

    // file doesn't exist
    if (file.length() == 0) {
      startFile();
      return;
    }

    if (file.exists()) {
      try (TsFileSequenceReader reader = new TsFileSequenceReader(file.getAbsolutePath(), false)) {

        // this tsfile is complete
        if (reader.isComplete()) {
          crashed = false;
          canWrite = false;
          out.close();
          return;
        }

        // uncompleted file
        truncatedPosition = reader.selfCheck(knownSchemas, chunkGroupMetaDataList, true);
        totalChunkNum = reader.getTotalChunkNum();
        if (truncatedPosition == TsFileCheckStatus.INCOMPATIBLE_FILE) {
          out.close();
          throw new IOException(
              String.format("%s is not in TsFile format.", file.getAbsolutePath()));
        } else if (truncatedPosition == TsFileCheckStatus.ONLY_MAGIC_HEAD) {
          crashed = true;
          out.truncate(TSFileConfig.MAGIC_STRING.getBytes().length + TSFileConfig.VERSION_NUMBER.getBytes().length);
        } else {
          crashed = true;
          //remove broken data
          out.truncate(truncatedPosition);
        }
      }
    }
  }

  @Override
  public Map<String, MeasurementSchema> getKnownSchema() {
    return knownSchemas;
  }


  /**
   * For query.
   *
   * get chunks' metadata from memory.
   *
   * @param deviceId the device id
   * @param measurementId the sensor id
   * @param dataType the value type
   * @return chunks' metadata
   */
  public List<ChunkMetaData> getVisibleMetadataList(String deviceId, String measurementId, TSDataType dataType) {
    List<ChunkMetaData> chunkMetaDataList = new ArrayList<>();
    if (metadatas.containsKey(deviceId) && metadatas.get(deviceId).containsKey(measurementId)) {
      for (ChunkMetaData chunkMetaData : metadatas.get(deviceId).get(measurementId)) {
        // filter: if a device'sensor is defined as float type, and data has been persistent.
        // Then someone deletes the timeseries and recreate it with Int type. We have to ignore
        // all the stale data.
        if (dataType == null || dataType.equals(chunkMetaData.getDataType())) {
          chunkMetaDataList.add(chunkMetaData);
        }
      }
    }
    return chunkMetaDataList;
  }


  /**
   * add all appendChunkGroupMetadatas into memory. After calling this method, other classes can
   * read these metadata.
   */
  public void makeMetadataVisible() {

    List<ChunkGroupMetaData> newlyFlushedMetadataList = getAppendedRowGroupMetadata();

    if (!newlyFlushedMetadataList.isEmpty()) {
      for (ChunkGroupMetaData rowGroupMetaData : newlyFlushedMetadataList) {
        String deviceId = rowGroupMetaData.getDeviceID();
        for (ChunkMetaData chunkMetaData : rowGroupMetaData.getChunkMetaDataList()) {
          String measurementId = chunkMetaData.getMeasurementUid();
          if (!metadatas.containsKey(deviceId)) {
            metadatas.put(deviceId, new HashMap<>());
          }
          if (!metadatas.get(deviceId).containsKey(measurementId)) {
            metadatas.get(deviceId).put(measurementId, new ArrayList<>());
          }
          metadatas.get(deviceId).get(measurementId).add(chunkMetaData);

        }
      }
    }
  }

  public boolean hasCrashed() {
    return crashed;
  }

  /**
   * get all the chunkGroups' metadata which are appended after the last calling of this method, or
   * after the class instance is initialized if this is the first time to call the method.
   *
   * @return a list of ChunkGroupMetadata
   */
  private List<ChunkGroupMetaData> getAppendedRowGroupMetadata() {
    List<ChunkGroupMetaData> append = new ArrayList<>();
    if (lastFlushedChunkGroupIndex < chunkGroupMetaDataList.size()) {
      append.addAll(chunkGroupMetaDataList
          .subList(lastFlushedChunkGroupIndex, chunkGroupMetaDataList.size()));
      lastFlushedChunkGroupIndex = chunkGroupMetaDataList.size();
    }
    return append;
  }

  /**
   * Given a TsFile, generate a writable RestorableTsFileIOWriter. That is, for a complete TsFile,
   * the function erases all FileMetadata and supports writing new data; For a incomplete TsFile,
   * the function supports writing new data directly. However, it is more efficient using the
   * construction function of RestorableTsFileIOWriter, if the tsfile is incomplete.
   *
   * @param file a TsFile
   * @return a writable RestorableTsFileIOWriter
   */
  public static RestorableTsFileIOWriter getWriterForAppendingDataOnCompletedTsFile(File file)
      throws IOException {
    long position = file.length();

    try (TsFileSequenceReader reader = new TsFileSequenceReader(file.getAbsolutePath(), false)) {
      // this tsfile is complete
      if (reader.isComplete()) {
        reader.loadMetadataSize();
        TsFileMetaData metaData = reader.readFileMetadata();
        for (TsDeviceMetadataIndex deviceMetadata : metaData.getDeviceMap().values()) {
          if (position > deviceMetadata.getOffset()) {
            position = deviceMetadata.getOffset();
          }
        }
      }
    }

    if (position != file.length()) {
      // if the file is complete, we will remove all file metadatas
      try (FileChannel channel = FileChannel
          .open(Paths.get(file.getAbsolutePath()), StandardOpenOption.WRITE)) {
        channel.truncate(position - 1);//remove the last marker.
      }
    }
    return new RestorableTsFileIOWriter(file);
  }

  public void addSchema(MeasurementSchema schema) {
    knownSchemas.put(schema.getMeasurementId(), schema);
  }
}
