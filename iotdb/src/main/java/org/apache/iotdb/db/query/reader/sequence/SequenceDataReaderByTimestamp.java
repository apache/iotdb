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

package org.apache.iotdb.db.query.reader.sequence;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.iotdb.db.engine.querycontext.GlobalSortedSeriesDataSource;
import org.apache.iotdb.db.query.context.QueryContext;
import org.apache.iotdb.db.query.reader.mem.MemChunkReaderByTimestamp;
import org.apache.iotdb.db.query.reader.merge.EngineReaderByTimeStamp;

public class SequenceDataReaderByTimestamp implements EngineReaderByTimeStamp {

  private List<EngineReaderByTimeStamp> seriesReaders;
  private int nextSeriesReaderIndex;
  private EngineReaderByTimeStamp currentSeriesReader;

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

  /**
   * This method is used only in unit test.
   */
  public SequenceDataReaderByTimestamp(List<EngineReaderByTimeStamp> seriesReaders) {
    this.seriesReaders = seriesReaders;
    nextSeriesReaderIndex = 0;
  }


  @Override
  public Object getValueInTimestamp(long timestamp) throws IOException {
    Object value = null;
    if (currentSeriesReader != null) {
      value = currentSeriesReader.getValueInTimestamp(timestamp);
      if (value != null || currentSeriesReader.hasNext()) {
        return value;
      }
    }

    while (nextSeriesReaderIndex < seriesReaders.size()) {
      currentSeriesReader = seriesReaders.get(nextSeriesReaderIndex++);
      if (currentSeriesReader != null) {
        value = currentSeriesReader.getValueInTimestamp(timestamp);
        if (value != null || currentSeriesReader.hasNext()) {
          return value;
        }
      }
    }
    return value;
  }

  @Override
  public boolean hasNext() throws IOException {
    if (currentSeriesReader != null && currentSeriesReader.hasNext()) {
      return true;
    }
    while (nextSeriesReaderIndex < seriesReaders.size()) {
      currentSeriesReader = seriesReaders.get(nextSeriesReaderIndex++);
      if (currentSeriesReader != null && currentSeriesReader.hasNext()) {
        return true;
      }
    }
    return false;
  }
}
