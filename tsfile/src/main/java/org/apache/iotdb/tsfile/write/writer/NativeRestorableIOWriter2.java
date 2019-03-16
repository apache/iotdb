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
import org.apache.iotdb.tsfile.file.MetaMarker;
import org.apache.iotdb.tsfile.file.footer.ChunkGroupFooter;
import org.apache.iotdb.tsfile.file.header.ChunkHeader;
import org.apache.iotdb.tsfile.file.header.PageHeader;
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
public class NativeRestorableIOWriter2 extends TsFileIOWriter {

  private static final Logger LOGGER = LoggerFactory
      .getLogger(NativeRestorableIOWriter2.class);

  private long truncatedPosition = -1;
  private Map<String, MeasurementSchema> knownSchemas = new HashMap<>();

  long getTruncatedPosition() {
    return truncatedPosition;
  }

  public NativeRestorableIOWriter2(File file) throws IOException {
    this(file, true);
  }

  /**
   * @param file a given tsfile path you want to (continue to) write
   * @param append whether append to write data in this file
   * @throws IOException if write failed, or the file is broken but autoRepair==false.
   */
  public NativeRestorableIOWriter2(File file, boolean append) throws IOException {
    super(file, true);
    try (TsFileSequenceReader reader = new TsFileSequenceReader(file.getAbsolutePath())){
      truncatedPosition = reader.selfCheck(knownSchemas, chunkGroupMetaDataList);
      if (truncatedPosition == TsFileCheckStatus.FILE_NOT_FOUND) {
        //it is ok.. because this is a new file.
      } else if (truncatedPosition == TsFileCheckStatus.COMPLETE_FILE) {
        if (!append) {
          throw new IOException(String
              .format("%s is a complete file but not in the append mode.", file.getAbsolutePath()));
        } else {
          //TODO remove filemetadata and then keep to write..
        }
      } else if (truncatedPosition == TsFileCheckStatus.ONLY_MAGIC_HEAD) {
        if (!append) {
          throw new IOException(String
              .format("%s is not complete and has nothing valuable data.", file.getAbsolutePath()));
        } else {
          //TODO keep to write
        }
      } else if (truncatedPosition == TsFileCheckStatus.INCOMPATIBLE_FILE) {
        throw new IOException(String.format("%s is not in TsFile format.", file.getAbsolutePath()));
      } else {
        out.truncate(truncatedPosition);
      }
    }
  }

  @Override
  public Map<String, MeasurementSchema> getKnownSchema() {
    return knownSchemas;
  }
}
