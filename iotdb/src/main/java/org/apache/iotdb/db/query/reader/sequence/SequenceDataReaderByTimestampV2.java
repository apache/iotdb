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
import java.util.List;
import org.apache.iotdb.db.engine.filenodeV2.TsFileResourceV2;
import org.apache.iotdb.db.engine.modification.Modification;
import org.apache.iotdb.db.query.context.QueryContext;
import org.apache.iotdb.db.query.control.FileReaderManager;
import org.apache.iotdb.db.query.reader.merge.EngineReaderByTimeStamp;
import org.apache.iotdb.db.utils.QueryUtils;
import org.apache.iotdb.tsfile.file.metadata.ChunkMetaData;
import org.apache.iotdb.tsfile.read.TsFileSequenceReader;
import org.apache.iotdb.tsfile.read.common.Path;
import org.apache.iotdb.tsfile.read.controller.ChunkLoader;
import org.apache.iotdb.tsfile.read.controller.ChunkLoaderImpl;
import org.apache.iotdb.tsfile.read.controller.MetadataQuerierByFileImpl;
import org.apache.iotdb.tsfile.read.reader.series.SeriesReaderByTimestamp;

public class SequenceDataReaderByTimestampV2 implements EngineReaderByTimeStamp {

  protected Path seriesPath;
  private List<TsFileResourceV2> tsFileResourceV2List;
  private int nextIntervalFileIndex;
  protected EngineReaderByTimeStamp seriesReader;
  private QueryContext context;

  /**
   * init with seriesPath and sealedTsFiles.
   */
  public SequenceDataReaderByTimestampV2(Path seriesPath,
      List<TsFileResourceV2> tsFileResourceV2List,
      QueryContext context) {
    this.seriesPath = seriesPath;
    this.tsFileResourceV2List = tsFileResourceV2List;
    this.nextIntervalFileIndex = 0;
    this.seriesReader = null;
    this.context = context;
  }

  @Override
  public Object getValueInTimestamp(long timestamp) throws IOException {
    Object value = null;
    if (seriesReader != null) {
      value = seriesReader.getValueInTimestamp(timestamp);
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
      TsFileResourceV2 tsFile = tsFileResourceV2List.get(nextIntervalFileIndex);
      nextIntervalFileIndex++;
      if (!tsFile.isClosed()) {
        initUnSealedTsFileReader(tsFile, context);
        break;
      }
      if (singleTsFileSatisfied(tsFile, timestamp)) {
        initSealedTsFileReader(tsFile, context);
        break;
      }
    }
  }

  /**
   * Judge whether the file should be skipped.
   */
  private boolean singleTsFileSatisfied(TsFileResourceV2 fileNode, long timestamp) {
    if (fileNode.isClosed()) {
      return fileNode.getEndTimeMap().get(seriesPath.getDevice()) >= timestamp;
    }
    return true;
  }

  private void initUnSealedTsFileReader(TsFileResourceV2 tsFile, QueryContext context)
      throws IOException {
    seriesReader = new UnSealedTsFilesReaderByTimestampV2(tsFile);
  }

  private void initSealedTsFileReader(TsFileResourceV2 fileNode, QueryContext context)
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

    seriesReader = new FileSeriesByTimestampIAggregateReader(
        new SeriesReaderByTimestamp(chunkLoader, metaDataList));
  }
}
