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

package org.apache.iotdb.db.storageengine.dataregion.compaction.execute.utils.reader;

import org.apache.iotdb.commons.path.AlignedFullPath;
import org.apache.iotdb.commons.path.IFullPath;
import org.apache.iotdb.commons.path.NonAlignedFullPath;
import org.apache.iotdb.commons.utils.TestOnly;
import org.apache.iotdb.db.queryengine.execution.fragment.FragmentInstanceContext;
import org.apache.iotdb.db.queryengine.execution.operator.source.AlignedSeriesScanUtil;
import org.apache.iotdb.db.queryengine.execution.operator.source.SeriesScanUtil;
import org.apache.iotdb.db.queryengine.plan.planner.plan.parameter.SeriesScanOptions;
import org.apache.iotdb.db.queryengine.plan.statement.component.Ordering;
import org.apache.iotdb.db.storageengine.dataregion.read.QueryDataSource;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResource;

import com.google.common.collect.Sets;
import org.apache.tsfile.read.common.block.TsBlock;

import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class SeriesDataBlockReader implements IDataBlockReader {

  private final SeriesScanUtil seriesScanUtil;

  private TsBlock tsBlock;

  private boolean hasCachedBatchData = false;

  public SeriesDataBlockReader(
      IFullPath seriesPath,
      Set<String> allSensors,
      FragmentInstanceContext context,
      QueryDataSource dataSource,
      boolean ascending) {
    SeriesScanOptions.Builder scanOptionsBuilder = new SeriesScanOptions.Builder();
    scanOptionsBuilder.withAllSensors(allSensors);

    if (seriesPath instanceof AlignedFullPath) {
      this.seriesScanUtil =
          new AlignedSeriesScanUtil(
              (AlignedFullPath) seriesPath,
              ascending ? Ordering.ASC : Ordering.DESC,
              scanOptionsBuilder.build(),
              context);
    } else if (seriesPath instanceof NonAlignedFullPath) {
      this.seriesScanUtil =
          new SeriesScanUtil(
              seriesPath,
              ascending ? Ordering.ASC : Ordering.DESC,
              scanOptionsBuilder.build(),
              context);
    } else {
      throw new IllegalArgumentException("Should call exact sub class!");
    }
    this.seriesScanUtil.initQueryDataSource(dataSource);
  }

  @TestOnly
  public SeriesDataBlockReader(
      IFullPath seriesPath,
      FragmentInstanceContext context,
      List<TsFileResource> seqFileResource,
      List<TsFileResource> unseqFileResource,
      boolean ascending) {
    SeriesScanOptions.Builder scanOptionsBuilder = new SeriesScanOptions.Builder();
    if (seriesPath instanceof AlignedFullPath) {
      scanOptionsBuilder.withAllSensors(
          new HashSet<>(((AlignedFullPath) seriesPath).getMeasurementList()));
      this.seriesScanUtil =
          new AlignedSeriesScanUtil(
              (AlignedFullPath) seriesPath,
              ascending ? Ordering.ASC : Ordering.DESC,
              scanOptionsBuilder.build(),
              context);
    } else {
      scanOptionsBuilder.withAllSensors(
          Sets.newHashSet(((NonAlignedFullPath) seriesPath).getMeasurement()));
      this.seriesScanUtil =
          new SeriesScanUtil(
              seriesPath,
              ascending ? Ordering.ASC : Ordering.DESC,
              scanOptionsBuilder.build(),
              context);
    }

    QueryDataSource queryDataSource = new QueryDataSource(seqFileResource, unseqFileResource);
    seriesScanUtil.initQueryDataSource(queryDataSource);
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
