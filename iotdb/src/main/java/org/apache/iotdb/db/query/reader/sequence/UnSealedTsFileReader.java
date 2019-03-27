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
import java.util.Collections;
import java.util.List;
import org.apache.iotdb.db.engine.querycontext.UnsealedTsFile;
import org.apache.iotdb.db.query.control.FileReaderManager;
import org.apache.iotdb.db.query.reader.IAggregateReader;
import org.apache.iotdb.db.query.reader.IBatchReader;
import org.apache.iotdb.tsfile.file.header.PageHeader;
import org.apache.iotdb.tsfile.file.metadata.ChunkMetaData;
import org.apache.iotdb.tsfile.read.TsFileSequenceReader;
import org.apache.iotdb.tsfile.read.common.BatchData;
import org.apache.iotdb.tsfile.read.common.Path;
import org.apache.iotdb.tsfile.read.controller.ChunkLoader;
import org.apache.iotdb.tsfile.read.controller.ChunkLoaderImpl;
import org.apache.iotdb.tsfile.read.filter.basic.Filter;
import org.apache.iotdb.tsfile.read.reader.series.FileSeriesReader;
import org.apache.iotdb.tsfile.read.reader.series.FileSeriesReaderWithFilter;
import org.apache.iotdb.tsfile.read.reader.series.FileSeriesReaderWithoutFilter;

public class UnSealedTsFileReader implements IBatchReader, IAggregateReader {

  protected Path seriesPath;
  private FileSeriesReader unSealedReader;

  /**
   * Construct funtion for UnSealedTsFileReader.
   *
   * @param unsealedTsFile -param to initial
   * @param filter -filter
   * @param isReverse true-traverse chunks from behind forward; false-traverse chunks from front to
   * back;
   */
  public UnSealedTsFileReader(UnsealedTsFile unsealedTsFile, Filter filter, boolean isReverse)
      throws IOException {

    TsFileSequenceReader unClosedTsFileReader = FileReaderManager.getInstance()
        .get(unsealedTsFile.getFilePath(),
            false);
    ChunkLoader chunkLoader = new ChunkLoaderImpl(unClosedTsFileReader);

    List<ChunkMetaData> metaDataList = unsealedTsFile.getChunkMetaDataList();
    //reverse chunk metadata list if traversing chunks from behind forward
    if (isReverse && metaDataList != null && !metaDataList.isEmpty()) {
      Collections.reverse(metaDataList);
    }

    if (filter == null) {
      unSealedReader = new FileSeriesReaderWithoutFilter(chunkLoader, metaDataList);
    } else {
      unSealedReader = new FileSeriesReaderWithFilter(chunkLoader, metaDataList, filter);
    }
  }

  @Override
  public boolean hasNext() throws IOException {
    return unSealedReader.hasNextBatch();
  }

  @Override
  public void close() throws IOException {
    if (unSealedReader != null) {
      unSealedReader.close();
    }
  }

  @Override
  public BatchData nextBatch() throws IOException {
    return unSealedReader.nextBatch();
  }

  @Override
  public PageHeader nextPageHeader() throws IOException {
    return unSealedReader.nextPageHeader();
  }

  @Override
  public void skipPageData() {
    unSealedReader.skipPageData();
  }
}
