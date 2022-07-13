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
package org.apache.iotdb.db.engine.compaction.reader;

import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.db.engine.querycontext.QueryDataSource;
import org.apache.iotdb.db.metadata.path.AlignedPath;
import org.apache.iotdb.db.mpp.execution.fragment.FragmentInstanceContext;
import org.apache.iotdb.db.mpp.execution.operator.source.AlignedSeriesScanUtil;
import org.apache.iotdb.db.mpp.execution.operator.source.SeriesScanUtil;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.common.block.TsBlock;
import org.apache.iotdb.tsfile.read.filter.basic.Filter;

import java.io.IOException;
import java.util.Set;

public class SeriesDataBlockReader implements IDataBlockReader {

  private final SeriesScanUtil seriesScanUtil;

  private TsBlock tsBlock;

  private boolean hasCachedBatchData = false;

  public SeriesDataBlockReader(
      PartialPath seriesPath,
      Set<String> allSensors,
      TSDataType dataType,
      FragmentInstanceContext context,
      QueryDataSource dataSource,
      Filter timeFilter,
      Filter valueFilter,
      boolean ascending) {
    if (seriesPath instanceof AlignedPath) {
      this.seriesScanUtil =
          new AlignedSeriesScanUtil(
              seriesPath, allSensors, context, timeFilter, valueFilter, ascending);
    } else {
      this.seriesScanUtil =
          new SeriesScanUtil(
              seriesPath, allSensors, dataType, context, timeFilter, valueFilter, ascending);
    }
    this.seriesScanUtil.initQueryDataSource(dataSource);
  }

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
    while (seriesScanUtil.hasNextFile()) {
      if (readChunkData()) {
        hasCachedBatchData = true;
        return true;
      }
    }
    return hasCachedBatchData;
  }

  @Override
  public TsBlock nextBatch() throws IOException {
    if (hasCachedBatchData || hasNextBatch()) {
      hasCachedBatchData = false;
      return tsBlock;
    }
    throw new IOException("no next block");
  }

  @Override
  public void close() throws IOException {
    // no resources need to close
  }

  private boolean readChunkData() throws IOException {
    while (seriesScanUtil.hasNextChunk()) {
      if (readPageData()) {
        return true;
      }
    }
    return false;
  }

  private boolean readPageData() throws IOException {
    while (seriesScanUtil.hasNextPage()) {
      tsBlock = seriesScanUtil.nextPage();
      if (!isEmpty(tsBlock)) {
        return true;
      }
    }
    return false;
  }

  private boolean isEmpty(TsBlock tsBlock) {
    return tsBlock == null || tsBlock.isEmpty();
  }
}
