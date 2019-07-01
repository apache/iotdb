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

import org.apache.iotdb.db.engine.storagegroup.TsFileResource;
import org.apache.iotdb.db.engine.modification.Modification;
import org.apache.iotdb.db.query.context.QueryContext;
import org.apache.iotdb.db.query.control.FileReaderManager;
import org.apache.iotdb.db.query.reader.IReaderByTimeStamp;
import org.apache.iotdb.db.query.reader.sequence.adapter.SeriesReaderByTimestampAdapter;
import org.apache.iotdb.db.utils.QueryUtils;
import org.apache.iotdb.tsfile.file.metadata.ChunkMetaData;
import org.apache.iotdb.tsfile.read.TsFileSequenceReader;
import org.apache.iotdb.tsfile.read.common.Path;
import org.apache.iotdb.tsfile.read.controller.ChunkLoader;
import org.apache.iotdb.tsfile.read.controller.ChunkLoaderImpl;
import org.apache.iotdb.tsfile.read.controller.MetadataQuerierByFileImpl;
import org.apache.iotdb.tsfile.read.reader.series.FileSeriesReaderByTimestamp;

import java.io.IOException;
import java.util.List;

/**
 * IReaderByTimeStamp of data in: 1) sealed tsfile. 2) unsealed tsfile, which include data on disk of
 * unsealed file and in memtables that will be flushing to unsealed tsfile.
 */
public class SequenceSeriesReaderByTimestamp implements IReaderByTimeStamp {

  protected Path seriesPath;
  private List<TsFileResource> tsFileResourceV2List;
  private int nextIntervalFileIndex;
  protected IReaderByTimeStamp seriesReader;
  private QueryContext context;

  /**
   * init with seriesPath and tsfile list which include sealed tsfile and unseadled tsfile.
   */
  public SequenceSeriesReaderByTimestamp(Path seriesPath,
                                         List<TsFileResource> tsFileResourceList,
                                         QueryContext context) {
    this.seriesPath = seriesPath;
    this.tsFileResourceV2List = tsFileResourceList;
    this.nextIntervalFileIndex = 0;
    this.seriesReader = null;
    this.context = context;
  }

  @Override
  public Object getValueInTimestamp(long timestamp) throws IOException {
    Object value = null;
    if (seriesReader != null) {
      value = seriesReader.getValueInTimestamp(timestamp);
      // if get value or no value in this timestamp, return.
      if (value != null || seriesReader.hasNext()) {
        return value;
      }
    }
    constructReader(timestamp);
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
    while (nextIntervalFileIndex < tsFileResourceV2List.size()) {
      initSealedTsFileReader(tsFileResourceV2List.get(nextIntervalFileIndex), context);
      nextIntervalFileIndex++;
      if (seriesReader.hasNext()) {
        return true;
      }
    }
    return false;
  }

  /**
   * construct reader with the file that might overlap this timestamp.
   */
  private void constructReader(long timestamp) throws IOException {
    while (nextIntervalFileIndex < tsFileResourceV2List.size()) {
      TsFileResource tsFile = tsFileResourceV2List.get(nextIntervalFileIndex);
      nextIntervalFileIndex++;
      // init unsealed tsfile.
      if (!tsFile.isClosed()) {
        initUnSealedTsFileReader(tsFile);
        return;
      }
      // init sealed tsfile.
      if (singleTsFileSatisfied(tsFile, timestamp)) {
        initSealedTsFileReader(tsFile, context);
        return;
      }
    }
  }

  /**
   * Judge whether the file should be skipped.
   */
  private boolean singleTsFileSatisfied(TsFileResource fileNode, long timestamp) {
    if (fileNode.isClosed()) {
      return fileNode.getEndTimeMap().get(seriesPath.getDevice()) >= timestamp;
    }
    return true;
  }

  private void initUnSealedTsFileReader(TsFileResource tsFile)
          throws IOException {
    seriesReader = new UnSealedTsFileReaderByTimestamp(tsFile);
  }

  private void initSealedTsFileReader(TsFileResource fileNode, QueryContext context)
          throws IOException {

    // to avoid too many opened files
    TsFileSequenceReader tsFileReader = FileReaderManager.getInstance()
            .get(fileNode.getFile().getPath(), true);

    MetadataQuerierByFileImpl metadataQuerier = new MetadataQuerierByFileImpl(tsFileReader);
    List<ChunkMetaData> metaDataList = metadataQuerier.getChunkMetaDataList(seriesPath);

    List<Modification> pathModifications = context.getPathModifications(fileNode.getModFile(),
            seriesPath.getFullPath());
    if (!pathModifications.isEmpty()) {
      QueryUtils.modifyChunkMetaData(metaDataList, pathModifications);
    }
    ChunkLoader chunkLoader = new ChunkLoaderImpl(tsFileReader);

    seriesReader = new SeriesReaderByTimestampAdapter(
            new FileSeriesReaderByTimestamp(chunkLoader, metaDataList));
  }
}
