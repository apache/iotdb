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

package org.apache.iotdb.tsfile.write.writer;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.iotdb.tsfile.common.conf.TSFileConfig;
import org.apache.iotdb.tsfile.file.metadata.ChunkGroupMetaData;
import org.apache.iotdb.tsfile.file.metadata.ChunkMetaData;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.TsFileCheckStatus;
import org.apache.iotdb.tsfile.read.TsFileSequenceReader;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * a restorable tsfile which do not depend on a restore file.
 */
public class NativeRestorableIOWriter extends TsFileIOWriter {

  private static final Logger LOGGER = LoggerFactory
      .getLogger(NativeRestorableIOWriter.class);

  private long truncatedPosition = -1;
  private Map<String, MeasurementSchema> knownSchemas = new HashMap<>();

  private int lastFlushedChunkGroupIndex = 0;

  /**
   * all chunk group metadata which have been serialized on disk.
   */
  private Map<String, Map<String, List<ChunkMetaData>>> metadatas;


  long getTruncatedPosition() {
    return truncatedPosition;
  }

  public NativeRestorableIOWriter(File file) throws IOException {
    this(file, false);
  }

  /**
   * @param file a given tsfile path you want to (continue to) write
   * @param append if true, then the file can support appending data even though the file is complete (i.e., tail magic string exists)
   * @throws IOException if write failed, or the file is broken but autoRepair==false.
   */
  public NativeRestorableIOWriter(File file, boolean append) throws IOException {
    super();
    this.out = new DefaultTsFileOutput(file, true);
    if (file.length() == 0) {
      //this is a new file
      return;
    }
    if (file.exists()) {
      try (TsFileSequenceReader reader = new TsFileSequenceReader(file.getAbsolutePath(), false)) {
        if (reader.isComplete() && !append) {
          canWrite = false;
          out.close();
          return;
        }
        truncatedPosition = reader.selfCheck(knownSchemas, chunkGroupMetaDataList, !append);
        if (truncatedPosition == TsFileCheckStatus.COMPLETE_FILE && !append) {
            this.canWrite = false;
            out.close();
        } else if (truncatedPosition == TsFileCheckStatus.INCOMPATIBLE_FILE) {
          out.close();
          throw new IOException(
              String.format("%s is not in TsFile format.", file.getAbsolutePath()));
        } else if (truncatedPosition == TsFileCheckStatus.ONLY_MAGIC_HEAD) {
          out.truncate(TSFileConfig.MAGIC_STRING.length());
        } else {
          //remove broken data
          out.truncate(truncatedPosition + 1);
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
  public List<ChunkMetaData> getVisibleMetadatas(String deviceId, String measurementId, TSDataType dataType) {
    List<ChunkMetaData> chunkMetaDatas = new ArrayList<>();
    if (metadatas.containsKey(deviceId) && metadatas.get(deviceId).containsKey(measurementId)) {
      for (ChunkMetaData chunkMetaData : metadatas.get(deviceId).get(measurementId)) {
        // filter: if a device'sensor is defined as float type, and data has been persistent.
        // Then someone deletes the timeseries and recreate it with Int type. We have to ignore
        // all the stale data.
        if (dataType.equals(chunkMetaData.getTsDataType())) {
          chunkMetaDatas.add(chunkMetaData);
        }
      }
    }
    return chunkMetaDatas;
  }


  /**
   * add all appendChunkGroupMetadatas into memory. After calling this method, other classes can
   * read these metadata.
   */
  public void makeMetadataVisible() {

    List<ChunkGroupMetaData> newlyFlushedMetadatas = getAppendedRowGroupMetadata();

    if (!newlyFlushedMetadatas.isEmpty()) {
      for (ChunkGroupMetaData rowGroupMetaData : newlyFlushedMetadatas) {
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


  /**
   * get all the chunkGroups' metadata which are appended after the last calling of this method, or
   * after the class instance is initialized if this is the first time to call the method.
   *
   * @return a list of chunkgroup metadata
   */
  private List<ChunkGroupMetaData> getAppendedRowGroupMetadata() {
    List<ChunkGroupMetaData> append = new ArrayList<>();
    if (lastFlushedChunkGroupIndex < chunkGroupMetaDataList.size()) {
      append.addAll(chunkGroupMetaDataList.subList(lastFlushedChunkGroupIndex, chunkGroupMetaDataList.size()));
      lastFlushedChunkGroupIndex = chunkGroupMetaDataList.size();
    }
    return append;
  }


}
