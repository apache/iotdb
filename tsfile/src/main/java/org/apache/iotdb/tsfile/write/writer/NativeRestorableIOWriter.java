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
import java.util.Map;
import org.apache.iotdb.tsfile.common.conf.TSFileConfig;
import org.apache.iotdb.tsfile.file.metadata.TsFileMetaData;
import org.apache.iotdb.tsfile.read.TsFileCheckStatus;
import org.apache.iotdb.tsfile.read.TsFileSequenceReader;
import org.apache.iotdb.tsfile.write.schema.FileSchema;
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

  long getTruncatedPosition() {
    return truncatedPosition;
  }

  public NativeRestorableIOWriter(File file) throws IOException {
    this(file, false);
  }

  /**
   * @param file a given tsfile path you want to (continue to) write
   * @param append if true, then the file can support appending data even though the file is complete (i.e., tail magic string exists)
   *  if false, whether the file can support appending data depends on whether the file is complete.
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
      //TODO try to use a cached reader rather than create a new reader.
      try (TsFileSequenceReader reader = new TsFileSequenceReader(file.getAbsolutePath(), false)) {
        if (reader.isComplete()) {
          handleCompleteFile(reader, append);
          return;
        }
        truncatedPosition = reader.selfCheck(knownSchemas, chunkGroupMetaDataList, !append);
        if (truncatedPosition == TsFileCheckStatus.COMPLETE_FILE ) {
            //actually, there is no way to access this code...
            handleCompleteFile(reader, append);
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

  private void handleCompleteFile(TsFileSequenceReader reader, boolean supportAppend) throws IOException {
    if (!supportAppend) {
      canWrite = false;
      out.close();
    }else {
      //remove the fileMeatadata
      reader.loadMetadataSize();
      //TODO try to use a cached reader rather than create a new reader.
      TsFileMetaData metaData = reader.readFileMetadata();
      knownSchemas = metaData.getMeasurementSchema();
      chunkGroupMetaDataList = reader.readAllChunkGroupMetaData(metaData);
      truncatedPosition = reader.getFirstTsDeviceMetadataPosition(metaData) - 1;
      out.truncate(truncatedPosition);
    }
  }

  @Override
  public Map<String, MeasurementSchema> getKnownSchema() {
    return knownSchemas;
  }

  @Override
  public void endFile(FileSchema schema) throws IOException {
    if (!canWrite) {
      return;
    }else {
        super.endFile(schema);
    }
  }

  public void endFile() throws IOException {
    super.endFile(new FileSchema(knownSchemas));
  }
}
