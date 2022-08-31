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

import org.apache.iotdb.tsfile.file.metadata.IChunkMetadata;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.common.Path;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class MemoryControlTsFileIOWriter extends TsFileIOWriter {
  private static final Logger LOG = LoggerFactory.getLogger(MemoryControlTsFileIOWriter.class);
  private long maxMetadataSize;
  private long currentChunkMetadataSize = 0L;
  private File chunkMetadataTempFile;
  private LocalTsFileOutput tempOutput;
  private List<Long> sortedSegmentPosition = new ArrayList<>();

  public static final String CHUNK_METADATA_TEMP_FILE_PREFIX = ".cmt";
  private static final byte VECTOR_TYPE = 1;
  private static final byte NORMAL_TYPE = 2;

  public MemoryControlTsFileIOWriter(File file, long maxMetadataSize) throws IOException {
    super(file);
    this.maxMetadataSize = maxMetadataSize;
    this.chunkMetadataTempFile = new File(file.getAbsoluteFile() + CHUNK_METADATA_TEMP_FILE_PREFIX);
  }

  @Override
  public void endCurrentChunk() {
    currentChunkMetadataSize += currentChunkMetadata.calculateRamSize();
    super.endCurrentChunk();
    if (currentChunkMetadataSize > maxMetadataSize) {
      // TODO: Sort and flush the chunk metadata to outside
      try {
        sortAndFlushChunkMetadata();
      } catch (IOException e) {
        LOG.error("Meets exception when flushing metadata to temp files", e);
      }
    }
  }

  private void sortAndFlushChunkMetadata() throws IOException {
    // group by series
    Map<Path, List<IChunkMetadata>> chunkMetadataListMap = groupChunkMetadataListBySeries();
    if (tempOutput == null) {
      tempOutput = new LocalTsFileOutput(new FileOutputStream(chunkMetadataTempFile));
    }
    sortedSegmentPosition.add(tempOutput.getPosition());
    for (Map.Entry<Path, List<IChunkMetadata>> entry : chunkMetadataListMap.entrySet()) {
      List<IChunkMetadata> iChunkMetadataList = entry.getValue();
      if (iChunkMetadataList.size() > 0
          && iChunkMetadataList.get(0).getDataType() == TSDataType.VECTOR) {
        // this is a vector chunk
        ReadWriteIOUtils.write(VECTOR_TYPE, tempOutput);
        ReadWriteIOUtils.write(chunkMetadataList.size(), tempOutput);
      } else {
        ReadWriteIOUtils.write(NORMAL_TYPE, tempOutput);
      }
      for (IChunkMetadata chunkMetadata : iChunkMetadataList) {
        chunkMetadata.serializeTo(tempOutput, true);
      }
    }
  }

  @Override
  public void endFile() {
    //		super.endFile();
  }
}
