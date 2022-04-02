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

package org.apache.iotdb.tsfile.read;

import org.apache.iotdb.tsfile.file.metadata.AlignedChunkMetadata;
import org.apache.iotdb.tsfile.file.metadata.ChunkMetadata;
import org.apache.iotdb.tsfile.file.metadata.IChunkMetadata;
import org.apache.iotdb.tsfile.read.common.Chunk;
import org.apache.iotdb.tsfile.read.reader.chunk.AlignedChunkReader;
import org.apache.iotdb.tsfile.utils.Pair;
import org.apache.iotdb.tsfile.write.schema.IMeasurementSchema;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

/**
 * This class return the AlignedChunkReader iteratively, each AlignedChunkReader reads one aligned
 * chunk group in a tsfile.
 */
public class TsFileAlignedSeriesReaderIterator {

  private TsFileSequenceReader reader;
  private List<AlignedChunkMetadata> alignedChunkMetadataList;
  private List<IMeasurementSchema> schemaList;

  private int curIdx = -1;

  public TsFileAlignedSeriesReaderIterator(
      TsFileSequenceReader reader,
      List<AlignedChunkMetadata> alignedChunkMetadataList,
      List<IMeasurementSchema> schemaList) {
    this.reader = reader;
    this.alignedChunkMetadataList = alignedChunkMetadataList;
    this.schemaList = schemaList;
  }

  public boolean hasNext() {
    return curIdx < alignedChunkMetadataList.size() - 1;
  }

  public Pair<AlignedChunkReader, Long> nextReader() throws IOException {
    AlignedChunkMetadata alignedChunkMetadata = alignedChunkMetadataList.get(++curIdx);
    IChunkMetadata timeChunkMetadata = alignedChunkMetadata.getTimeChunkMetadata();
    List<IChunkMetadata> valueChunkMetadataList = alignedChunkMetadata.getValueChunkMetadataList();
    int schemaIdx = 0;
    Chunk timeChunk = reader.readMemChunk((ChunkMetadata) timeChunkMetadata);
    Chunk[] valueChunks = new Chunk[schemaList.size()];
    long totalSize = 0;
    for (IChunkMetadata valueChunkMetadata : valueChunkMetadataList) {
      if (valueChunkMetadata == null) {
        continue;
      }
      while (!valueChunkMetadata
          .getMeasurementUid()
          .equals(schemaList.get(schemaIdx).getMeasurementId())) {
        schemaIdx++;
      }
      Chunk chunk = reader.readMemChunk((ChunkMetadata) valueChunkMetadata);
      valueChunks[schemaIdx++] = chunk;
      totalSize += chunk.getHeader().getSerializedSize() + chunk.getHeader().getDataSize();
    }

    AlignedChunkReader chunkReader =
        new AlignedChunkReader(timeChunk, Arrays.asList(valueChunks), null);

    return new Pair<>(chunkReader, totalSize);
  }
}
