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
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import org.apache.iotdb.tsfile.file.metadata.ChunkGroupMetaData;
import org.apache.iotdb.tsfile.file.metadata.ChunkMetaData;
import org.apache.iotdb.tsfile.file.metadata.TsDeviceMetadata;
import org.apache.iotdb.tsfile.file.metadata.TsDeviceMetadataIndex;
import org.apache.iotdb.tsfile.file.metadata.TsFileMetaData;
import org.apache.iotdb.tsfile.read.TsFileSequenceReader;
import org.apache.iotdb.tsfile.read.common.Path;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;

/**
 * ForceAppendTsFileWriter opens a COMPLETE TsFile, reads and truncate its metadata to support
 * appending new data.
 */
public class ForceAppendTsFileWriter extends TsFileIOWriter{

  private Map<String, MeasurementSchema> knownSchemas = new HashMap<>();

  public ForceAppendTsFileWriter(File file) throws IOException {
    this.out = new DefaultTsFileOutput(file, true);
    this.file = file;

    // file doesn't exist
    if (file.length() == 0 || !file.exists()) {
      throw new IOException("File " + file.getPath() + " is not a complete TsFile");
    }

    try (TsFileSequenceReader reader = new TsFileSequenceReader(file.getAbsolutePath(), true)) {

      // this tsfile is not complete
      if (!reader.isComplete()) {
        throw new IOException("File " + file.getPath() + " is not a complete TsFile");
      }
      TsFileMetaData fileMetaData = reader.readFileMetadata();
      Map<String, TsDeviceMetadataIndex> deviceMap = fileMetaData.getDeviceMap();
      long firstDeviceMetaPos = Long.MAX_VALUE;
      for (Entry<String, TsDeviceMetadataIndex> deviceMetadataEntry : deviceMap.entrySet()) {
        TsDeviceMetadataIndex deviceMetadataIndex = deviceMetadataEntry.getValue();
        TsDeviceMetadata tsDeviceMetadata = reader
            .readTsDeviceMetaData(deviceMetadataIndex);
        chunkGroupMetaDataList.addAll(tsDeviceMetadata.getChunkGroupMetaDataList());
        firstDeviceMetaPos = firstDeviceMetaPos > deviceMetadataIndex.getOffset() ?
            deviceMetadataIndex.getOffset() : firstDeviceMetaPos;
      }
      // truncate metadata and marker
      out.truncate(firstDeviceMetaPos - 1);
      knownSchemas = fileMetaData.getMeasurementSchema();
    }
  }

  /**
   * Remove such ChunkMetadata that its startTime is not in chunkStartTimes
   * @param chunkStartTimes
   */
  public void filterChunks(Map<Path, List<Long>> chunkStartTimes) {
    Map<Path, Integer> startTimeIdxes = new HashMap<>();
    chunkStartTimes.forEach((p, t) -> startTimeIdxes.put(p, 0));

    Iterator<ChunkGroupMetaData> chunkGroupMetaDataIterator = chunkGroupMetaDataList.iterator();
    while (chunkGroupMetaDataIterator.hasNext()) {
      ChunkGroupMetaData chunkGroupMetaData = chunkGroupMetaDataIterator.next();
      String deviceId = chunkGroupMetaData.getDeviceID();
      int chunkNum = chunkGroupMetaData.getChunkMetaDataList().size();
      Iterator<ChunkMetaData> chunkMetaDataIterator =
          chunkGroupMetaData.getChunkMetaDataList().iterator();
      while (chunkMetaDataIterator.hasNext()) {
        ChunkMetaData chunkMetaData = chunkMetaDataIterator.next();
        Path path = new Path(deviceId, chunkMetaData.getMeasurementUid());
        int startTimeIdx = startTimeIdxes.get(path);
        List<Long> pathChunkStartTimes = chunkStartTimes.get(path);
        boolean chunkValid = startTimeIdx < pathChunkStartTimes.size()
            && pathChunkStartTimes.get(startTimeIdx) == chunkMetaData.getStartTime();
        if (!chunkValid) {
          chunkGroupMetaDataIterator.remove();
          chunkNum--;
          invalidChunkNum++;
        } else {
          startTimeIdxes.put(path, startTimeIdx + 1);
        }
      }
      if (chunkNum == 0) {
        chunkGroupMetaDataIterator.remove();
      }
    }
  }

  @Override
  public Map<String, MeasurementSchema> getKnownSchema() {
    return knownSchemas;
  }
}
