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
package org.apache.iotdb.db.utils;

import java.io.IOException;
import java.util.List;
import org.apache.iotdb.db.engine.storagegroup.TsFileResource;
import org.apache.iotdb.tsfile.file.metadata.ChunkGroupMetaData;
import org.apache.iotdb.tsfile.file.metadata.ChunkMetaData;
import org.apache.iotdb.tsfile.file.metadata.TsDeviceMetadata;
import org.apache.iotdb.tsfile.file.metadata.TsDeviceMetadataIndex;
import org.apache.iotdb.tsfile.file.metadata.TsFileMetaData;
import org.apache.iotdb.tsfile.read.TsFileSequenceReader;

public class FileLoaderUtils {

  private FileLoaderUtils() {
  }

  public static void checkTsFileResource(TsFileResource tsFileResource) throws IOException {
    if (!tsFileResource.fileExists()) {
      // .resource file does not exist, read file metadata and recover tsfile resource
      try (TsFileSequenceReader reader = new TsFileSequenceReader(
          tsFileResource.getFile().getAbsolutePath())) {
        TsFileMetaData metaData = reader.readFileMetadata();
        updateTsFileResource(metaData, reader, tsFileResource);
      }
      // write .resource file
      tsFileResource.serialize();
    } else {
      tsFileResource.deSerialize();
    }
  }

  public static void updateTsFileResource(TsFileMetaData metaData, TsFileSequenceReader reader,
      TsFileResource tsFileResource) throws IOException {
    for (TsDeviceMetadataIndex index : metaData.getDeviceMap().values()) {
      TsDeviceMetadata deviceMetadata = reader.readTsDeviceMetaData(index);
      List<ChunkGroupMetaData> chunkGroupMetaDataList = deviceMetadata
          .getChunkGroupMetaDataList();
      for (ChunkGroupMetaData chunkGroupMetaData : chunkGroupMetaDataList) {
        for (ChunkMetaData chunkMetaData : chunkGroupMetaData.getChunkMetaDataList()) {
          tsFileResource.updateStartTime(chunkGroupMetaData.getDeviceID(),
              chunkMetaData.getStartTime());
          tsFileResource
              .updateEndTime(chunkGroupMetaData.getDeviceID(), chunkMetaData.getEndTime());
        }
      }
    }
  }
}
