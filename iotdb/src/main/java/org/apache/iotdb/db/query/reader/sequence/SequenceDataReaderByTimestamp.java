/**
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements.  See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership.  The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with the License.  You may obtain
 * a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied.  See the License for the specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iotdb.db.query.reader.sequence;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.iotdb.db.engine.querycontext.GlobalSortedSeriesDataSource;
import org.apache.iotdb.db.query.context.QueryContext;
import org.apache.iotdb.db.query.reader.mem.MemChunkReaderByTimestamp;
import org.apache.iotdb.db.query.reader.merge.EngineReaderByTimeStamp;
import org.apache.iotdb.tsfile.utils.Pair;

public class SequenceDataReaderByTimestamp implements EngineReaderByTimeStamp {

  private List<EngineReaderByTimeStamp> seriesReaders;
  private int nextSeriesReaderIndex;
  private EngineReaderByTimeStamp currentSeriesReader;

  private boolean hasCached;
  private Pair<Long, Object> cachedTimeValuePair;

  /**
   * init with globalSortedSeriesDataSource and filter.
   */
  public SequenceDataReaderByTimestamp(GlobalSortedSeriesDataSource sources, QueryContext context)
      throws IOException {
    seriesReaders = new ArrayList<>();

    nextSeriesReaderIndex = 0;

    // add reader for sealed TsFiles
    if (sources.hasSealedTsFiles()) {
      seriesReaders.add(
          new SealedTsFilesReaderByTimestamp(sources.getSeriesPath(), sources.getSealedTsFiles(),
              context));
    }

    // add reader for unSealed TsFile
    if (sources.hasUnsealedTsFile()) {
      seriesReaders.add(new UnSealedTsFilesReaderByTimestamp(sources.getUnsealedTsFile()));
    }

    // add data in memTable
    if (sources.hasRawSeriesChunk()) {
      seriesReaders.add(new MemChunkReaderByTimestamp(sources.getReadableChunk()));
    }

  }

  @Override
  public Object getValueInTimestamp(long timestamp) throws IOException {
    cachedTimeValuePair = getValueGtEqTimestamp(timestamp);
    if (cachedTimeValuePair == null || cachedTimeValuePair.left == timestamp) {
      return cachedTimeValuePair;
    } else {
      hasCached = true;
      return null;
    }
  }

  @Override
  public Pair<Long, Object> getValueGtEqTimestamp(long timestamp) throws IOException {
    if (hasCached && cachedTimeValuePair.left >= timestamp) {
      hasCached = false;
      return cachedTimeValuePair;
    }

    if (currentSeriesReader != null) {
      cachedTimeValuePair = currentSeriesReader.getValueGtEqTimestamp(timestamp);
      if (cachedTimeValuePair != null) {
        return cachedTimeValuePair;
      }
    }

    while (nextSeriesReaderIndex < seriesReaders.size()) {
      currentSeriesReader = seriesReaders.get(nextSeriesReaderIndex++);
      cachedTimeValuePair = currentSeriesReader.getValueGtEqTimestamp(timestamp);
      if (cachedTimeValuePair != null) {
        return cachedTimeValuePair;
      }
    }

    return null;
  }

  @Override
  public void close() throws IOException {
    for (EngineReaderByTimeStamp seriesReader : seriesReaders) {
      seriesReader.close();
    }
  }
}
