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
package org.apache.iotdb.db.query.reader.series;

import org.apache.iotdb.db.engine.querycontext.QueryDataSource;
import org.apache.iotdb.db.metadata.path.PartialPath;
import org.apache.iotdb.db.query.context.QueryContext;
import org.apache.iotdb.db.query.filter.TsFileFilter;
import org.apache.iotdb.db.utils.TestOnly;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.common.BatchData;
import org.apache.iotdb.tsfile.read.filter.basic.Filter;

import java.io.IOException;
import java.util.Set;

public class SeriesRawDataPrefetchReader implements ManagedSeriesReader {

  private final SeriesBitmapReader seriesReader;

  private boolean hasRemaining;
  private boolean managedByQueryManager;

  private BatchData batchData;
  private boolean hasCachedBatchData = false;

  public SeriesRawDataPrefetchReader(SeriesBitmapReader seriesReader) {
    this.seriesReader = seriesReader;
  }

  public SeriesRawDataPrefetchReader(
      PartialPath seriesPath,
      Set<String> allSensors,
      TSDataType dataType,
      QueryContext context,
      QueryDataSource dataSource,
      Filter timeFilter,
      Filter valueFilter,
      TsFileFilter fileFilter,
      boolean ascending) {
    this.seriesReader =
        seriesPath.createSeriesBitmapReader(
            allSensors,
            dataType,
            context,
            dataSource,
            timeFilter,
            valueFilter,
            fileFilter,
            ascending);
  }

  /**
   * This method overrides the AbstractDataReader.hasNextOverlappedPage for pause reads, to achieve
   * a continuous read
   */
  @Override
  public boolean hasNextBatch() throws IOException {

    if (hasCachedBatchData) {
      return true;
    }

    /*
     * consume page data firstly
     */
    if (readPageData()) {
      hasCachedBatchData = true;
      return true;
    }

    /*
     * consume chunk data secondly
     */
    if (readChunkData()) {
      hasCachedBatchData = true;
      return true;
    }

    /*
     * consume next file finally
     */
    while (seriesReader.hasNextFile()) {
      if (readChunkData()) {
        hasCachedBatchData = true;
        return true;
      }
    }
    return hasCachedBatchData;
  }

  @Override
  public BatchData nextBatch() throws IOException {
    if (hasCachedBatchData || hasNextBatch()) {
      hasCachedBatchData = false;
      return batchData;
    }
    throw new IOException("no next batch");
  }

  @Override
  public void close() throws IOException {
    // no resources need to close
  }

  @Override
  public boolean isManagedByQueryManager() {
    return managedByQueryManager;
  }

  @Override
  public void setManagedByQueryManager(boolean managedByQueryManager) {
    this.managedByQueryManager = managedByQueryManager;
  }

  @Override
  public boolean hasRemaining() {
    return hasRemaining;
  }

  @Override
  public void setHasRemaining(boolean hasRemaining) {
    this.hasRemaining = hasRemaining;
  }

  private boolean readChunkData() throws IOException {
    while (seriesReader.hasNextChunk()) {
      if (readPageData()) {
        return true;
      }
    }
    return false;
  }

  private boolean readPageData() throws IOException {
    while (seriesReader.hasNextPage()) {
      batchData = seriesReader.nextPage();
      if (!isEmpty(batchData)) {
        return true;
      }
    }
    return false;
  }

  private boolean isEmpty(BatchData batchData) {
    return batchData == null || !batchData.hasCurrent();
  }

  @TestOnly
  public SeriesBitmapReader getSeriesReader() {
    return seriesReader;
  }
}
