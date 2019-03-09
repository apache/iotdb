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
import java.util.ArrayList;
import java.util.List;
import org.apache.iotdb.db.engine.filenode.IntervalFileNode;
import org.apache.iotdb.db.engine.modification.Modification;
import org.apache.iotdb.db.query.context.QueryContext;
import org.apache.iotdb.db.query.control.FileReaderManager;
import org.apache.iotdb.db.query.reader.IAggregateReader;
import org.apache.iotdb.db.query.reader.IBatchReader;
import org.apache.iotdb.db.utils.QueryUtils;
import org.apache.iotdb.tsfile.file.header.PageHeader;
import org.apache.iotdb.tsfile.file.metadata.ChunkMetaData;
import org.apache.iotdb.tsfile.read.TsFileSequenceReader;
import org.apache.iotdb.tsfile.read.common.BatchData;
import org.apache.iotdb.tsfile.read.common.Path;
import org.apache.iotdb.tsfile.read.controller.ChunkLoader;
import org.apache.iotdb.tsfile.read.controller.ChunkLoaderImpl;
import org.apache.iotdb.tsfile.read.controller.MetadataQuerierByFileImpl;
import org.apache.iotdb.tsfile.read.filter.basic.Filter;
import org.apache.iotdb.tsfile.read.reader.series.FileSeriesReader;
import org.apache.iotdb.tsfile.read.reader.series.FileSeriesReaderWithFilter;
import org.apache.iotdb.tsfile.read.reader.series.FileSeriesReaderWithoutFilter;

public class SealedTsFilesReader implements IBatchReader, IAggregateReader {

  private Path seriesPath;
  private List<IntervalFileNode> sealedTsFiles;
  private int usedIntervalFileIndex;
  private FileSeriesReader seriesReader;
  private Filter filter;
  private QueryContext context;

  /**
   * init with seriesPath, sealedTsFiles, filter, context.
   */
  public SealedTsFilesReader(Path seriesPath, List<IntervalFileNode> sealedTsFiles, Filter filter,
      QueryContext context) {
    this(seriesPath, sealedTsFiles, context);
    this.filter = filter;

  }

  /**
   * init with seriesPath and sealedTsFiles.
   */
  public SealedTsFilesReader(Path seriesPath, List<IntervalFileNode> sealedTsFiles,
      QueryContext context) {
    this.seriesPath = seriesPath;
    this.sealedTsFiles = sealedTsFiles;
    this.usedIntervalFileIndex = 0;
    this.seriesReader = null;
    this.context = context;
  }

  /**
   * init with seriesReader and queryContext.
   */
  public SealedTsFilesReader(FileSeriesReader seriesReader, QueryContext context) {
    this.seriesReader = seriesReader;
    sealedTsFiles = new ArrayList<>();
    this.context = context;
  }

  @Override
  public boolean hasNext() throws IOException {

    // try to get next batch data from current reader
    if (seriesReader != null && seriesReader.hasNextBatch()) {
      return true;
    }

    // init until reach a satisfied reader
    while (usedIntervalFileIndex < sealedTsFiles.size()) {
      // try to get next batch data from next reader
      IntervalFileNode fileNode = sealedTsFiles.get(usedIntervalFileIndex++);
      if (singleTsFileSatisfied(fileNode)) {
        initSingleTsFileReader(fileNode, context);
      } else {
        continue;
      }

      if (seriesReader.hasNextBatch()) {
        return true;
      }
    }

    return false;
  }

  private boolean singleTsFileSatisfied(IntervalFileNode fileNode) {

    if (filter == null) {
      return true;
    }

    long startTime = fileNode.getStartTime(seriesPath.getDevice());
    long endTime = fileNode.getEndTime(seriesPath.getDevice());
    return filter.satisfyStartEndTime(startTime, endTime);
  }

  private void initSingleTsFileReader(IntervalFileNode fileNode, QueryContext context)
      throws IOException {

    // to avoid too many opened files
    TsFileSequenceReader tsFileReader = FileReaderManager.getInstance()
        .get(fileNode.getFilePath(), true);

    MetadataQuerierByFileImpl metadataQuerier = new MetadataQuerierByFileImpl(tsFileReader);
    List<ChunkMetaData> metaDataList = metadataQuerier.getChunkMetaDataList(seriesPath);

    List<Modification> pathModifications = context.getPathModifications(fileNode.getModFile(),
        seriesPath.getFullPath());
    if (pathModifications.size() > 0) {
      QueryUtils.modifyChunkMetaData(metaDataList, pathModifications);
    }

    ChunkLoader chunkLoader = new ChunkLoaderImpl(tsFileReader);

    if (filter == null) {
      seriesReader = new FileSeriesReaderWithoutFilter(chunkLoader, metaDataList);
    } else {
      seriesReader = new FileSeriesReaderWithFilter(chunkLoader, metaDataList, filter);
    }
  }

  @Override
  public BatchData nextBatch() throws IOException {
    return seriesReader.nextBatch();
  }

  @Override
  public void close() throws IOException {
    if (seriesReader != null) {
      seriesReader.close();
    }
  }

  @Override
  public PageHeader nextPageHeader() throws IOException {
    return null;
  }

  @Override
  public void skipPageData() throws IOException {
    seriesReader.skipPageData();
  }
}
