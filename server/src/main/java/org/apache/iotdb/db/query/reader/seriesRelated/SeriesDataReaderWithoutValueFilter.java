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

import java.io.IOException;
import org.apache.iotdb.db.exception.StorageEngineException;
import org.apache.iotdb.db.query.context.QueryContext;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.statistics.Statistics;
import org.apache.iotdb.tsfile.read.common.Path;
import org.apache.iotdb.tsfile.read.filter.basic.Filter;

/*
 *This class extends some methods to implement skip reads :
 *  while(hasNextChunk()){
 *    if(canUseChunkStatistics()){
 *      Statistics statistics = currentChunkStatistics();
 *      doSomething...
 *      skipChunkData();
 *      break;
 *    }
 *
 *    while(hasNextPage()){
 *      if(canUseChunkStatistics()){
 *        Statistics statistics = currentPageStatistics();
 *        doSomething...
 *        skipPageData();
 *        break;
 *      }
 *
 *      while(hasNextBatch()){
 *        nextBatch();
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

  public boolean canUseChunkStatistics() {
    Statistics statistics = chunkMetaData.getStatistics();
    return overlappedChunkReader.isEmpty() && canUseStatistics(statistics);
  }

  @Override
  public Statistics currentChunkStatistics() {
    return chunkMetaData.getStatistics();
  }

  @Override
  public void skipChunkData() {
    hasCachedNextChunk = false;
  }


  @Override
  public boolean hasNextPage() throws IOException {
    return super.hasNextPage();
  }

  @Override
  public boolean canUsePageStatistics() {
    Statistics pageStatistics = currentPage.getStatistics();
    return !overlappedPages.isEmpty() && canUseStatistics(pageStatistics);
  }

  @Override
  public Statistics currentPageStatistics() {
    return currentPage.getStatistics();
  }

  @Override
  public void skipPageData() {
    hasCachedNextPage = false;
  }

  @Override
  public boolean hasNextBatch() throws IOException {
    return super.hasNextBatch();
  }

  protected boolean canUseStatistics(Statistics statistics) {
    return filter == null || filter.containStartEndTime(statistics.getStartTime(),
        statistics.getEndTime());
  }

  @Override
  public void close() {

  }
}
