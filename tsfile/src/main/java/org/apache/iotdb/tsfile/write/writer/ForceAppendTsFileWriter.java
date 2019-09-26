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
import java.util.Map;
import org.apache.iotdb.tsfile.exception.write.TsFileNotCompleteException;
import org.apache.iotdb.tsfile.file.metadata.TsDeviceMetadata;
import org.apache.iotdb.tsfile.file.metadata.TsDeviceMetadataIndex;
import org.apache.iotdb.tsfile.file.metadata.TsFileMetaData;
import org.apache.iotdb.tsfile.read.TsFileSequenceReader;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;

/**
 * ForceAppendTsFileWriter opens a COMPLETE TsFile, reads and truncate its metadata to support
 * appending new data.
 */
public class ForceAppendTsFileWriter extends TsFileIOWriter{

  private Map<String, MeasurementSchema> knownSchemas;
  private long truncatePosition;

  public ForceAppendTsFileWriter(File file) throws IOException {
    this.out = new DefaultTsFileOutput(file, true);
    this.file = file;

    // file doesn't exist
    if (file.length() == 0 || !file.exists()) {
      throw new TsFileNotCompleteException("File " + file.getPath() + " is not a complete TsFile");
    }

    try (TsFileSequenceReader reader = new TsFileSequenceReader(file.getAbsolutePath(), true)) {

      // this tsfile is not complete
      if (!reader.isComplete()) {
        throw new TsFileNotCompleteException("File " + file.getPath() + " is not a complete TsFile");
      }
      TsFileMetaData fileMetaData = reader.readFileMetadata();
      Map<String, TsDeviceMetadataIndex> deviceMap = fileMetaData.getDeviceMap();
      long firstDeviceMetaPos = Long.MAX_VALUE;
      for (TsDeviceMetadataIndex deviceMetadataIndex : deviceMap.values()) {
        TsDeviceMetadata tsDeviceMetadata = reader
            .readTsDeviceMetaData(deviceMetadataIndex);
        chunkGroupMetaDataList.addAll(tsDeviceMetadata.getChunkGroupMetaDataList());
        firstDeviceMetaPos = firstDeviceMetaPos > deviceMetadataIndex.getOffset() ?
            deviceMetadataIndex.getOffset() : firstDeviceMetaPos;
      }
      // truncate metadata and marker
      truncatePosition = firstDeviceMetaPos - 1;
      knownSchemas = fileMetaData.getMeasurementSchema();

    }
  }

  public void doTruncate() throws IOException {
    out.truncate(truncatePosition);
  }

  public long getTruncatePosition() {
    return truncatePosition;
  }

  @Override
  public Map<String, MeasurementSchema> getKnownSchema() {
    return knownSchemas;
  }
}
