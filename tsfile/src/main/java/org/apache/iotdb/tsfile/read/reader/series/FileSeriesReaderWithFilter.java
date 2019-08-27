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
package org.apache.iotdb.tsfile.read.reader.series;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import org.apache.iotdb.tsfile.file.metadata.ChunkMetaData;
import org.apache.iotdb.tsfile.file.metadata.TsDigest.StatisticType;
import org.apache.iotdb.tsfile.read.common.Chunk;
import org.apache.iotdb.tsfile.read.controller.ChunkLoader;
import org.apache.iotdb.tsfile.read.filter.DigestForFilter;
import org.apache.iotdb.tsfile.read.filter.basic.Filter;
import org.apache.iotdb.tsfile.read.reader.chunk.ChunkReaderWithFilter;

/**
 * Series reader is used to query one series of one TsFile,
 * and this reader has a filter operating on the same series.
 */
public class FileSeriesReaderWithFilter extends FileSeriesReader {

  private Filter filter;

  public FileSeriesReaderWithFilter(ChunkLoader chunkLoader,
      List<ChunkMetaData> chunkMetaDataList, Filter filter) {
    super(chunkLoader, chunkMetaDataList);
    this.filter = filter;
  }

  @Override
  protected void initChunkReader(ChunkMetaData chunkMetaData) throws IOException {
    Chunk chunk = chunkLoader.getChunk(chunkMetaData);
    this.chunkReader = new ChunkReaderWithFilter(chunk, filter);
  }

  @Override
  protected boolean chunkSatisfied(ChunkMetaData chunkMetaData) {
    ByteBuffer minValue = null;
    ByteBuffer maxValue = null;
    ByteBuffer[] statistics = chunkMetaData.getDigest().getStatistics();
    if (statistics != null) {
      minValue = statistics[StatisticType.min_value.ordinal()]; // note still CAN be null
      maxValue = statistics[StatisticType.max_value.ordinal()]; // note still CAN be null
    }

    DigestForFilter digest = new DigestForFilter(chunkMetaData.getStartTime(),
        chunkMetaData.getEndTime(), minValue, maxValue, chunkMetaData.getTsDataType());
    return filter.satisfy(digest);
  }

}
