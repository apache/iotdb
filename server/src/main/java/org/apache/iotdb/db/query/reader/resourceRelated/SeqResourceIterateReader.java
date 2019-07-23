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
package org.apache.iotdb.db.query.reader.resourceRelated;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import org.apache.iotdb.db.engine.modification.Modification;
import org.apache.iotdb.db.engine.storagegroup.TsFileResource;
import org.apache.iotdb.db.query.context.QueryContext;
import org.apache.iotdb.db.query.control.FileReaderManager;
import org.apache.iotdb.db.query.reader.IAggregateReader;
import org.apache.iotdb.db.query.reader.fileRelated.FileSeriesReaderAdapter;
import org.apache.iotdb.db.query.reader.fileRelated.UnSealedTsFileIterateReader;
import org.apache.iotdb.db.query.reader.universal.IterateReader;
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
 * To read a chronologically ordered list of sequence TsFiles, this class extends {@link
 * IterateReader} to implements <code>IAggregateReader</code> for the TsFiles.
 * <p>
 * Notes: 1) The list of sequence TsFiles is in strict chronological order. 2) The data in a
 * sequence TsFile is also organized in chronological order. 3) A sequence TsFile can be either
 * sealed or unsealed. 4) An unsealed sequence TsFile consists of two parts of data in chronological
 * order: data that has been flushed to disk and data in the flushing memtable list.
 * <p>
 * This class is used in {@link org.apache.iotdb.db.query.reader.seriesRelated.SeriesReaderWithoutValueFilter}.
 */
public class SeqResourceIterateReader extends IterateReader {

  private Path seriesPath;

  /**
   * Whether the reverse order is enabled.
   * <p>
   * True to iterate over the list of sequence TsFiles and chunks in TsFiles in reverse
   * chronological order (from newest to oldest); False to iterate in chronological order (from
   * oldest to newest).
   */
  private boolean enableReverse;

  private List<TsFileResource> seqResources;
  private Filter filter;
  private QueryContext context;

  /**
   * Constructor function.
   * <p>
   * <code>IterateReader</code> is used to iterate over the chronologically ordered list of
   * sequence TsFiles. Therefore, this method calls the parent class <code>IterateReader</code>'s
   * constructor to set <code>readerSize</code> to be the size of the TsFile list. Readers for the
   * TsFiles are created in order later in the method <code>constructNextReader</code>.
   *
   * @param seriesPath the path of the series data
   * @param seqResources a list of sequence TsFile resources in chronological order
   * @param filter filter condition. Null if no filter.
   * @param context query context
   * @param isReverse True to iterate over data in reverse chronological order (from newest to
   * oldest); False to iterate over data in chronological order (from oldest to newest).
   */
  public SeqResourceIterateReader(Path seriesPath, List<TsFileResource> seqResources,
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

  public SeqResourceIterateReader(Path seriesPath, List<TsFileResource> seqResources,
      Filter timeFilter, QueryContext context) {
    this(seriesPath, seqResources, timeFilter, context, false);
  }

  /**
   * If the idx-th TsFile in the <code>seqResources</code> might satisfy this <code>filter</code>,
   * then construct <code>IAggregateReader</code> for it, assign to <code>currentSeriesReader</code>
   * and return true. Otherwise, return false.
   *
   * @param idx the index of the TsFile in the resource list
   * @return True if the reader is constructed; False if not.
   */
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
      currentSeriesReader = new UnSealedTsFileIterateReader(tsFileResource, filter,
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

  private IAggregateReader initSealedTsFileReader(TsFileResource sealedTsFile, Filter filter,
      QueryContext context) throws IOException {
    // prepare metaDataList
    TsFileSequenceReader tsFileReader = FileReaderManager.getInstance()
        .get(sealedTsFile.getFile().getPath(), true);
    MetadataQuerierByFileImpl metadataQuerier = new MetadataQuerierByFileImpl(tsFileReader);
    List<ChunkMetaData> metaDataList = metadataQuerier.getChunkMetaDataList(seriesPath);
    List<Modification> pathModifications = context.getPathModifications(sealedTsFile.getModFile(),
        seriesPath.getFullPath());
    if (!pathModifications.isEmpty()) {
      QueryUtils.modifyChunkMetaData(metaDataList, pathModifications);
    }

    if (enableReverse) {
      Collections.reverse(metaDataList);
    }
    // prepare chunkLoader
    ChunkLoader chunkLoader = new ChunkLoaderImpl(tsFileReader);

    // init fileSeriesReader
    FileSeriesReader fileSeriesReader;
    if (filter == null) {
      fileSeriesReader = new FileSeriesReaderWithoutFilter(chunkLoader, metaDataList);
    } else {
      fileSeriesReader = new FileSeriesReaderWithFilter(chunkLoader, metaDataList, filter);
    }
    return new FileSeriesReaderAdapter(fileSeriesReader);
  }
}