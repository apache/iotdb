/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
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
import org.apache.iotdb.db.engine.filenodeV2.TsFileResourceV2;
import org.apache.iotdb.db.engine.modification.Modification;
import org.apache.iotdb.db.engine.querycontext.GlobalSortedSeriesDataSourceV2;
import org.apache.iotdb.db.query.context.QueryContext;
import org.apache.iotdb.db.query.control.FileReaderManager;
import org.apache.iotdb.db.query.reader.IAggregateReader;
import org.apache.iotdb.db.utils.QueryUtils;
import org.apache.iotdb.tsfile.file.metadata.ChunkMetaData;
import org.apache.iotdb.tsfile.read.TsFileSequenceReader;
import org.apache.iotdb.tsfile.read.common.Path;
import org.apache.iotdb.tsfile.read.controller.ChunkLoader;
import org.apache.iotdb.tsfile.read.controller.ChunkLoaderImpl;
import org.apache.iotdb.tsfile.read.controller.MetadataQuerierByFileImpl;
import org.apache.iotdb.tsfile.read.filter.basic.Filter;
import org.apache.iotdb.tsfile.read.reader.series.FileSeriesReader;
import org.apache.iotdb.tsfile.read.reader.series.FileSeriesReaderWithFilter;
import org.apache.iotdb.tsfile.read.reader.series.FileSeriesReaderWithoutFilter;

public class SequenceDataReaderV2 extends IterateReader {

  private Path seriesPath;
  private boolean enableReverse;

  /**
   * init with globalSortedSeriesDataSource, filter, context and isReverse.
   *
   * @param sources data source
   * @param filter null if no filter
   * @param context query context
   * @param isReverse true-traverse chunks from behind forward, false-traverse chunks from front to
   * back.
   */
  public SequenceDataReaderV2(GlobalSortedSeriesDataSourceV2 sources, Filter filter,
      QueryContext context, boolean isReverse) throws IOException {
    super();
    this.seriesPath = sources.getSeriesPath();
    this.enableReverse = isReverse;

    for (TsFileResourceV2 tsFileResource : sources.getQueryTsFiles()) {
      if (tsFileResource.isClosed()) {
        constructSealedTsFileReader(tsFileResource, filter, context, seriesReaders);
      } else {
        seriesReaders.add(
            new UnSealedTsFileReaderV2(tsFileResource, filter, enableReverse));
      }
    }

  }

  public SequenceDataReaderV2(GlobalSortedSeriesDataSourceV2 sources, Filter filter,
      QueryContext context) throws IOException {
    this(sources, filter, context, false);

  }
  private void constructSealedTsFileReader(TsFileResourceV2 tsFileResource, Filter filter,
      QueryContext context, List<IAggregateReader> readerList)
      throws IOException {
    if (singleTsFileSatisfied(tsFileResource, filter)) {
      readerList.add(
          new FileSeriesIAggregateReader(initSingleTsFileReader(tsFileResource, filter, context)));
    }

  }

  private boolean singleTsFileSatisfied(TsFileResourceV2 tsfile, Filter filter) {

    if (filter == null) {
      return true;
    }

    long startTime = tsfile.getStartTimeMap().get(seriesPath.getDevice());
    long endTime = tsfile.getEndTimeMap().get(seriesPath.getDevice());
    return filter.satisfyStartEndTime(startTime, endTime);
  }

  private FileSeriesReader initSingleTsFileReader(TsFileResourceV2 tsfile, Filter filter,
      QueryContext context)
      throws IOException {

    // to avoid too many opened files
    TsFileSequenceReader tsFileReader = FileReaderManager.getInstance()
        .get(tsfile.getFile().getPath(), true);

    MetadataQuerierByFileImpl metadataQuerier = new MetadataQuerierByFileImpl(tsFileReader);
    List<ChunkMetaData> metaDataList = metadataQuerier.getChunkMetaDataList(seriesPath);

    List<Modification> pathModifications = context.getPathModifications(tsfile.getModFile(),
        seriesPath.getFullPath());
    if (!pathModifications.isEmpty()) {
      QueryUtils.modifyChunkMetaData(metaDataList, pathModifications);
    }

    ChunkLoader chunkLoader = new ChunkLoaderImpl(tsFileReader);

    if (enableReverse) {
      Collections.reverse(metaDataList);
    }

    FileSeriesReader seriesReader;
    if (filter == null) {
      seriesReader = new FileSeriesReaderWithoutFilter(chunkLoader, metaDataList);
    } else {
      seriesReader = new FileSeriesReaderWithFilter(chunkLoader, metaDataList, filter);
    }
    return seriesReader;
  }
}
