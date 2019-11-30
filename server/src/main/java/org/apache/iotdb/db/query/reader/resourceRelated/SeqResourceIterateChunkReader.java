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
package org.apache.iotdb.db.query.reader.resourceRelated;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import org.apache.iotdb.db.engine.cache.DeviceMetaDataCache;
import org.apache.iotdb.db.engine.modification.Modification;
import org.apache.iotdb.db.engine.storagegroup.TsFileResource;
import org.apache.iotdb.db.query.context.QueryContext;
import org.apache.iotdb.db.query.control.FileReaderManager;
import org.apache.iotdb.db.query.reader.IAggregateChunkReader;
import org.apache.iotdb.db.query.reader.fileRelated.FileSeriesChunkReaderAdapter;
import org.apache.iotdb.db.query.reader.fileRelated.UnSealedTsFileIterateChunkReader;
import org.apache.iotdb.db.query.reader.universal.IterateChunkReader;
import org.apache.iotdb.db.utils.QueryUtils;
import org.apache.iotdb.tsfile.file.metadata.ChunkMetaData;
import org.apache.iotdb.tsfile.read.TsFileSequenceReader;
import org.apache.iotdb.tsfile.read.common.Path;
import org.apache.iotdb.tsfile.read.controller.ChunkLoaderImpl;
import org.apache.iotdb.tsfile.read.controller.IChunkLoader;
import org.apache.iotdb.tsfile.read.filter.basic.Filter;
import org.apache.iotdb.tsfile.read.reader.series.FileSeriesChunkReader;


public class SeqResourceIterateChunkReader extends IterateChunkReader {

  private Path seriesPath;
  private boolean enableReverse;

  private List<TsFileResource> seqResources;
  private Filter filter;
  private QueryContext context;


  public SeqResourceIterateChunkReader(Path seriesPath, List<TsFileResource> seqResources,
      Filter filter, QueryContext context, boolean isReverse) {
    super(seqResources.size());
    this.seriesPath = seriesPath;
    this.enableReverse = isReverse;
    if (isReverse) {
      Collections.reverse(seqResources);
    }
    this.seqResources = seqResources;
    this.filter = filter;
    this.context = context;
  }

  public SeqResourceIterateChunkReader(Path seriesPath, List<TsFileResource> seqResources,
      Filter timeFilter, QueryContext context) {
    this(seriesPath, seqResources, timeFilter, context, false);
  }

  @Override
  public boolean constructNextReader(int idx) throws IOException {
    TsFileResource tsFileResource = seqResources.get(idx);
    if (tsFileResource.isClosed()) {
      if (isTsFileNotSatisfied(tsFileResource, filter)) {
        return false;
      }
      currentSeriesReader = initSealedTsFileReader(tsFileResource, filter, context);
      return true;
    } else {
      // an unsealed sequence TsFile's endTimeMap size may be equal to 0 or greater than 0
      // If endTimeMap size is 0, conservatively assume that this TsFile might satisfy this filter.
      // If endTimeMap size is not 0, call isTsFileNotSatisfied to check.
      if (tsFileResource.getEndTimeMap().size() != 0) {
        if (isTsFileNotSatisfied(tsFileResource, filter)) {
          return false;
        }
      }
      currentSeriesReader = new UnSealedTsFileIterateChunkReader(tsFileResource, filter,
          enableReverse);
      return true;
    }
  }

  /**
   * Returns true if the start and end time of the series data in this sequence TsFile do not
   * satisfy the filter condition. Returns false if satisfy.
   * <p>
   * This method is used to in <code>constructNextReader</code> to check whether this TsFile can be
   * skipped.
   *
   * @param tsFile the TsFileResource corresponding to this TsFile
   * @param filter filter condition. Null if no filter.
   * @return True if the TsFile's start and end time do not satisfy the filter condition; False if
   * satisfy.
   */
  private boolean isTsFileNotSatisfied(TsFileResource tsFile, Filter filter) {
    if (filter == null) {
      return false;
    }
    long startTime = tsFile.getStartTimeMap().get(seriesPath.getDevice());
    long endTime = tsFile.getEndTimeMap().get(seriesPath.getDevice());
    return !filter.satisfyStartEndTime(startTime, endTime);
  }

  private IAggregateChunkReader initSealedTsFileReader(TsFileResource sealedTsFile, Filter filter,
      QueryContext context) throws IOException {
    // prepare metaDataList
    List<ChunkMetaData> metaDataList = DeviceMetaDataCache.getInstance()
        .get(sealedTsFile, seriesPath);
    List<Modification> pathModifications = context.getPathModifications(sealedTsFile.getModFile(),
        seriesPath.getFullPath());
    if (!pathModifications.isEmpty()) {
      QueryUtils.modifyChunkMetaData(metaDataList, pathModifications);
    }

    if (enableReverse) {
      Collections.reverse(metaDataList);
    }
    // prepare chunkLoader
    TsFileSequenceReader tsFileReader = FileReaderManager.getInstance()
        .get(sealedTsFile, true);
    IChunkLoader chunkLoader = new ChunkLoaderImpl(tsFileReader);

    // init fileSeriesReader
    FileSeriesChunkReader fileSeriesReader;
    fileSeriesReader = new FileSeriesChunkReader(chunkLoader, metaDataList, filter);
    return new FileSeriesChunkReaderAdapter(fileSeriesReader);
  }
}