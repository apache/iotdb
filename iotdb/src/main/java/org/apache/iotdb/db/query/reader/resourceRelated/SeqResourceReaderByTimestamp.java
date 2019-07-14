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
import java.util.List;
import org.apache.iotdb.db.engine.modification.Modification;
import org.apache.iotdb.db.engine.storagegroup.TsFileResource;
import org.apache.iotdb.db.query.context.QueryContext;
import org.apache.iotdb.db.query.control.FileReaderManager;
import org.apache.iotdb.db.query.reader.IReaderByTimestamp;
import org.apache.iotdb.db.query.reader.chunkRelated.FileSeriesReaderByTimestampAdapter;
import org.apache.iotdb.db.query.reader.chunkRelated.UnSealedTsFileReaderByTimestamp;
import org.apache.iotdb.db.utils.QueryUtils;
import org.apache.iotdb.tsfile.file.metadata.ChunkMetaData;
import org.apache.iotdb.tsfile.read.TsFileSequenceReader;
import org.apache.iotdb.tsfile.read.common.Path;
import org.apache.iotdb.tsfile.read.controller.ChunkLoader;
import org.apache.iotdb.tsfile.read.controller.ChunkLoaderImpl;
import org.apache.iotdb.tsfile.read.controller.MetadataQuerierByFileImpl;
import org.apache.iotdb.tsfile.read.reader.series.FileSeriesReaderByTimestamp;

/**
 * To read a chronologically ordered list of sequence TsFiles by timestamp, this class implements
 * <code>IReaderByTimestamp</code> for the TsFiles.
 * <p>
 * Notes: 1) The list of sequence TsFiles is in strict chronological order. 2) The data in a
 * sequence TsFile is also organized in chronological order. 3) A sequence TsFile can be either
 * sealed or unsealed. 4) An unsealed sequence TsFile consists of two parts of data in chronological
 * order: data that has been flushed to disk and data in the flushing memtable list.
 * <p>
 * This class is used in {@link org.apache.iotdb.db.query.reader.seriesRelated.SeriesReaderByTimestamp}.
 */
// TODO need confirm that whether there can be at most one unsealed TsFile in the end.
//  especially because getValueInTimestamp can only function correctly based on this assumption.

public class SeqResourceReaderByTimestamp implements IReaderByTimestamp {

  protected Path seriesPath;
  private List<TsFileResource> seqResources;
  private QueryContext context;
  private int nextIntervalFileIndex;
  private IReaderByTimestamp seriesReader;

  /**
   * Constructor function.
   * <p>
   *
   * @param seriesPath the path of the series data
   * @param seqResources a list of sequence TsFile resources in chronological order
   * @param context query context
   */
  public SeqResourceReaderByTimestamp(Path seriesPath, List<TsFileResource> seqResources,
      QueryContext context) {
    this.seriesPath = seriesPath;
    this.seqResources = seqResources;
    this.context = context;
    this.nextIntervalFileIndex = 0;
    this.seriesReader = null;
  }

  @Override
  public Object getValueInTimestamp(long timestamp) throws IOException {
    Object value = null;
    if (seriesReader != null) {
      value = seriesReader.getValueInTimestamp(timestamp);
      // if get value or no value in this timestamp but has next, return.
      if (value != null || seriesReader.hasNext()) {
        return value;
      }
    }

    constructReader(timestamp);
    // TODO need confirm that whether there can be at most one unsealed TsFile in the end.
    //  especially because getValueInTimestamp can only function correctly based on this assumption.
    if (seriesReader != null) {
      value = seriesReader.getValueInTimestamp(timestamp);
      if (value != null || seriesReader.hasNext()) {
        return value;
      }
    }
    return value;
  }

  @Override
  public boolean hasNext() throws IOException {
    if (seriesReader != null && seriesReader.hasNext()) {
      return true;
    }

    while (nextIntervalFileIndex < seqResources.size()) {
      TsFileResource tsFileResource = seqResources.get(nextIntervalFileIndex++);
      if (tsFileResource.isClosed()) {
        seriesReader = initSealedTsFileReaderByTimestamp(tsFileResource, context);
      } else {
        seriesReader = new UnSealedTsFileReaderByTimestamp(tsFileResource);
      }
      if (seriesReader.hasNext()) {
        return true;
      }
    }
    return false;
  }

  /**
   * Iterates through the chronologically ordered list of sequence TsFiles according to this
   * timestamp to find the next TsFile resource to create reader for.
   * <p>
   * Because the data in these TsFile resources is chronologically globally ordered, there exists at
   * most one TsFile resource that overlaps this timestamp. Therefore, the iterate strategy is to
   * find the sealed TsFile that overlaps this timestamp. If no such sealed TsFile exists, use the
   * last unsealed TsFile if exists.
   *
   * @param timestamp get value in this timestamp
   */
  // TODO need confirm that whether there can be at most one unsealed TsFile in the end.
  //  especially because getValueInTimestamp can only function correctly based on this assumption.
  private void constructReader(long timestamp) throws IOException {
    while (nextIntervalFileIndex < seqResources.size()) {
      TsFileResource tsFileResource = seqResources.get(nextIntervalFileIndex++);
      if (tsFileResource.isClosed()) {
        if (sealedTsFileSatisfied(tsFileResource, timestamp)) {
          seriesReader = initSealedTsFileReaderByTimestamp(tsFileResource, context);
          return;
        }
      } else {
        seriesReader = new UnSealedTsFileReaderByTimestamp(tsFileResource);
        return;
      }
    }
  }

  private boolean sealedTsFileSatisfied(TsFileResource sealedTsFile, long timestamp) {
    return sealedTsFile.getEndTimeMap().get(seriesPath.getDevice()) >= timestamp;
  }

  private IReaderByTimestamp initSealedTsFileReaderByTimestamp(TsFileResource sealedTsFile,
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
    // prepare chunkLoader
    ChunkLoader chunkLoader = new ChunkLoaderImpl(tsFileReader);

    return new FileSeriesReaderByTimestampAdapter(
        new FileSeriesReaderByTimestamp(chunkLoader, metaDataList));
  }
}