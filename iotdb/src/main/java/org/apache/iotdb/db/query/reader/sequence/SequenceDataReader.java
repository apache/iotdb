/**
 * Copyright © 2019 Apache IoTDB(incubating) (dev@iotdb.apache.org)
 *
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
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.iotdb.db.query.reader.sequence;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.iotdb.db.engine.querycontext.GlobalSortedSeriesDataSource;
import org.apache.iotdb.db.query.reader.IReader;
import org.apache.iotdb.db.query.reader.mem.MemChunkReaderWithFilter;
import org.apache.iotdb.db.query.reader.mem.MemChunkReaderWithoutFilter;
import org.apache.iotdb.db.utils.TimeValuePair;
import org.apache.iotdb.tsfile.read.common.BatchData;
import org.apache.iotdb.tsfile.read.filter.basic.Filter;

/**
 * <p>
 * A reader for sequentially inserts data，including a list of sealedTsFile, unSealedTsFile and data
 * in MemTable.
 * </p>
 */
public class SequenceDataReader implements IReader {

  private List<IReader> seriesReaders;
  private boolean curReaderInitialized;
  private int nextSeriesReaderIndex;
  private IReader currentSeriesReader;

  /**
   * init with globalSortedSeriesDataSource and filter.
   */
  public SequenceDataReader(GlobalSortedSeriesDataSource sources, Filter filter)
      throws IOException {
    seriesReaders = new ArrayList<>();

    curReaderInitialized = false;
    nextSeriesReaderIndex = 0;

    // add reader for sealed TsFiles
    if (sources.hasSealedTsFiles()) {
      seriesReaders.add(
          new SealedTsFilesReader(sources.getSeriesPath(), sources.getSealedTsFiles(), filter));
    }

    // add reader for unSealed TsFile
    if (sources.hasUnsealedTsFile()) {
      seriesReaders.add(new UnSealedTsFileReader(sources.getUnsealedTsFile(), filter));
    }

    // add data in memTable
    if (sources.hasRawSeriesChunk()) {
      if (filter == null) {
        seriesReaders.add(new MemChunkReaderWithoutFilter(sources.getReadableChunk()));
      } else {
        seriesReaders.add(new MemChunkReaderWithFilter(sources.getReadableChunk(), filter));
      }
    }

  }

  @Override
  public boolean hasNext() throws IOException {
    if (curReaderInitialized && currentSeriesReader.hasNext()) {
      return true;
    } else {
      curReaderInitialized = false;
    }

    while (nextSeriesReaderIndex < seriesReaders.size()) {
      currentSeriesReader = seriesReaders.get(nextSeriesReaderIndex++);
      if (currentSeriesReader.hasNext()) {
        curReaderInitialized = true;
        return true;
      } else {
        curReaderInitialized = false;
      }
    }
    return false;
  }

  @Override
  public TimeValuePair next() throws IOException {
    return currentSeriesReader.next();
  }

  @Override
  public void skipCurrentTimeValuePair() throws IOException {
    next();
  }

  @Override
  public void close() throws IOException {
    for (IReader seriesReader : seriesReaders) {
      seriesReader.close();
    }
  }

  @Override
  public boolean hasNextBatch() {
    return false;
  }

  @Override
  public BatchData nextBatch() {
    return null;
  }

  @Override
  public BatchData currentBatch() {
    return null;
  }

}
