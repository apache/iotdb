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
import org.apache.iotdb.db.engine.querycontext.QueryDataSource;
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
 *      skipCurrentChunk();
 *      continue;
 *    }
 *
 *    while(hasNextPage()){
 *      if(canUsePageStatistics()){
 *        Statistics statistics = currentPageStatistics();
 *        doSomething...
 *        skipCurrentPage();
 *        continue;
 *      }
 *
 *      while(hasNextOverlappedPage()){
 *        nextOverlappedPage();
 *      }
 *    }
 *  }
 */
public class SeriesReaderWithoutValueFilter extends AbstractSeriesReader implements
    IAggregateReader {

  private final Filter filter;

  public SeriesReaderWithoutValueFilter(Path seriesPath, TSDataType dataType, Filter timeFilter,
      QueryContext context, QueryDataSource queryDataSource) {
    super(seriesPath, dataType, context, queryDataSource.getSeqResources(),
        queryDataSource.getUnseqResources());

    this.filter = queryDataSource.setTTL(timeFilter);
  }


  @Override
  protected Filter getFilter() {
    return filter;
  }

  @Override
  public boolean canUseCurrentChunkStatistics() {
    Statistics chunkStatistics = super.currentChunkStatistics();
    return !super.isChunkOverlapped() && satisfyFilter(chunkStatistics);
  }

  @Override
  public boolean canUseCurrentPageStatistics() throws IOException {
    Statistics currentPageStatistics = super.currentPageStatistics();
    return !super.isPageOverlapped() && satisfyFilter(currentPageStatistics);
  }
}
