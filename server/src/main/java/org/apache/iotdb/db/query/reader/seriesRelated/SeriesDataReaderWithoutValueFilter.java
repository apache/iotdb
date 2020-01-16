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
package org.apache.iotdb.db.query.reader.seriesRelated;

import org.apache.iotdb.db.exception.StorageEngineException;
import org.apache.iotdb.db.query.context.QueryContext;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.statistics.Statistics;
import org.apache.iotdb.tsfile.read.common.BatchData;
import org.apache.iotdb.tsfile.read.common.Path;
import org.apache.iotdb.tsfile.read.filter.basic.Filter;

import java.io.IOException;

/*
 *This class extends some methods to implement skip reads :
 *  while(hasNextChunk()){
 *    if(canUseChunkStatistics()){
 *      Statistics statistics = currentChunkStatistics();
 *      doSomething...
 *      continue;
 *    }
 *
 *    while(hasNextPage()){
 *      if(canUseChunkStatistics()){
 *        Statistics statistics = currentPageStatistics();
 *        doSomething...
 *        continue;
 *      }
 *
 *      while(hasNextOverlappedPage()){
 *        nextOverlappedPage();
 *      }
 *    }
 *  }
 */
public class SeriesDataReaderWithoutValueFilter extends AbstractDataReader implements
    IAggregateReader {

  public SeriesDataReaderWithoutValueFilter(Path seriesPath, TSDataType dataType, Filter timeFilter,
      QueryContext context) throws StorageEngineException, IOException {
    super(seriesPath, dataType, timeFilter, context);
  }


  @Override
  public boolean hasNextChunk() throws IOException {
    return super.hasNextChunk();
  }

  public boolean canUseCurrentChunkStatistics() {
    return super.canUseChunkStatistics();
  }

  @Override
  public Statistics currentChunkStatistics() {
    return firstChunkMetaData.getStatistics();
  }

  @Override
  public void skipChunkData() {
    hasCachedFirstChunkMetadata = false;
    firstChunkMetaData = null;
  }

  @Override
  public boolean hasNextPage() throws IOException {
    return super.hasNextPage();
  }

  @Override
  public Statistics currentPageStatistics() throws IOException {
    if (overlappedPageReaders.isEmpty() || overlappedPageReaders.peek().data == null) {
      throw new IOException("No next page statistics.");
    }
    return overlappedPageReaders.peek().data.getStatistics();
  }

  @Override
  public void skipPageData() throws IOException {
    overlappedPageReaders.poll();
  }

  @Override
  public boolean hasNextOverlappedPage() throws IOException {
    return super.hasNextOverlappedPage();
  }

  @Override
  public BatchData nextOverlappedPage() throws IOException {
    return super.nextOverlappedPage();
  }

  @Override
  public void close() throws IOException {
    super.close();
  }
}
