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
import org.apache.iotdb.db.engine.modification.Modification;
import org.apache.iotdb.db.engine.storagegroup.TsFileResource;
import org.apache.iotdb.db.query.context.QueryContext;
import org.apache.iotdb.db.query.control.FileReaderManager;
import org.apache.iotdb.db.query.reader.sequence.adapter.FileSeriesReaderAdapter;
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

/**
 * batch reader of data in: 1) sealed tsfile. 2) unsealed tsfile, which include data on disk of
 * unsealed file and in memtables that will be flushing to unsealed tsfile.
 */
public class SequenceSeriesReader extends IterateReader {

  private Path seriesPath;
  /**
   * Is reverse the sequence of tsfiles(include sealed and unsealed tsfile) and chunks in tsfiles.
   * True-traverse chunks from behind forward. False-traverse chunks from front to back.
   */
  private boolean enableReverse;

  private List<TsFileResource> seqResources;
  private Filter filter;
  private QueryContext context;


  /**
   * init with globalSortedSeriesDataSource, filter, context and isReverse.
   *
   * @param seriesPath data source
   * @param seqResources sealed and unsealed tsfile resources
   * @param filter null if no filter
   * @param context query context
   * @param isReverse true-traverse chunks from behind forward, false-traverse chunks from front to
   * back.
   */
  public SequenceSeriesReader(Path seriesPath, List<TsFileResource> seqResources,
      Filter filter, QueryContext context, boolean isReverse) {
    super(seqResources.size());
    if (isReverse) {
      Collections.reverse(seqResources);
    }
    this.seqResources = seqResources;
    this.seriesPath = seriesPath;
    this.enableReverse = isReverse;
    this.context = context;
    this.filter = filter;
  }

  /**
   * traverse chunks from front to back.
   */
  public SequenceSeriesReader(Path seriesPath, List<TsFileResource> seqResources,
      Filter timeFilter, QueryContext context) {
    this(seriesPath, seqResources, timeFilter, context, false);
  }

  @Override
  public boolean constructNextReader(int idx) throws IOException {
    TsFileResource tsFileResource = seqResources.get(idx);
    if (tsFileResource.isClosed()) {
      if (singleTsFileSatisfied(tsFileResource, filter)) {
        currentSeriesReader = new FileSeriesReaderAdapter(
            initSealedTsFileReader(tsFileResource, filter, context));
        return true;
      } else {
        return false;
      }
    } else {
      currentSeriesReader = new UnSealedTsFileReader(tsFileResource, filter, enableReverse);
      return true;
    }
  }

  /**
   * check if skip the tsfile.
   *
   * @param tsfile tsfile resource.
   * @param filter filter condition. If no filter, the filed is null.
   */
  private boolean singleTsFileSatisfied(TsFileResource tsfile, Filter filter) {

    if (filter == null) {
      return true;
    }

    long startTime = tsfile.getStartTimeMap().get(seriesPath.getDevice());
    long endTime = tsfile.getEndTimeMap().get(seriesPath.getDevice());
    return filter.satisfyStartEndTime(startTime, endTime);
  }

  private FileSeriesReader initSealedTsFileReader(TsFileResource tsfile, Filter filter,
      QueryContext context)
      throws IOException {

    // to avoid too many opened files
    TsFileSequenceReader tsFileReader = FileReaderManager.getInstance()
        .get(tsfile, true);

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
