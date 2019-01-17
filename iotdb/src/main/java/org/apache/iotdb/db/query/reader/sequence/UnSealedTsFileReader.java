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
import org.apache.iotdb.db.engine.querycontext.UnsealedTsFile;
import org.apache.iotdb.db.query.control.FileReaderManager;
import org.apache.iotdb.db.query.reader.IReader;
import org.apache.iotdb.db.utils.TimeValuePair;
import org.apache.iotdb.db.utils.TimeValuePairUtils;
import org.apache.iotdb.tsfile.read.TsFileSequenceReader;
import org.apache.iotdb.tsfile.read.common.BatchData;
import org.apache.iotdb.tsfile.read.common.Path;
import org.apache.iotdb.tsfile.read.controller.ChunkLoader;
import org.apache.iotdb.tsfile.read.controller.ChunkLoaderImpl;
import org.apache.iotdb.tsfile.read.filter.basic.Filter;
import org.apache.iotdb.tsfile.read.reader.series.FileSeriesReader;
import org.apache.iotdb.tsfile.read.reader.series.FileSeriesReaderWithFilter;
import org.apache.iotdb.tsfile.read.reader.series.FileSeriesReaderWithoutFilter;

public class UnSealedTsFileReader implements IReader {

  protected Path seriesPath;
  private FileSeriesReader unSealedTsFileReader;
  private BatchData data;

  /**
   * Construct funtion for UnSealedTsFileReader.
   *
   * @param unsealedTsFile -param to initial
   * @param filter -filter
   */
  public UnSealedTsFileReader(UnsealedTsFile unsealedTsFile, Filter filter) throws IOException {

    TsFileSequenceReader unClosedTsFileReader = FileReaderManager.getInstance()
        .get(unsealedTsFile.getFilePath(),
            true);
    ChunkLoader chunkLoader = new ChunkLoaderImpl(unClosedTsFileReader);

    if (filter == null) {
      unSealedTsFileReader = new FileSeriesReaderWithoutFilter(chunkLoader,
          unsealedTsFile.getChunkMetaDataList());
    } else {
      unSealedTsFileReader = new FileSeriesReaderWithFilter(chunkLoader,
          unsealedTsFile.getChunkMetaDataList(),
          filter);
    }

  }

  @Override
  public boolean hasNext() throws IOException {
    if (data == null || !data.hasNext()) {
      if (!unSealedTsFileReader.hasNextBatch()) {
        return false;
      }
      data = unSealedTsFileReader.nextBatch();
    }

    return data.hasNext();
  }

  @Override
  public TimeValuePair next() {
    TimeValuePair timeValuePair = TimeValuePairUtils.getCurrentTimeValuePair(data);
    data.next();
    return timeValuePair;
  }

  @Override
  public void skipCurrentTimeValuePair() {
    data.next();
  }

  @Override
  public void close() throws IOException {
    if (unSealedTsFileReader != null) {
      unSealedTsFileReader.close();
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
