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
import org.apache.iotdb.db.engine.filenode.IntervalFileNode;
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

public class SealedTsFilesReaderByTimestamp implements EngineReaderByTimeStamp {

  private Path seriesPath;
  private List<IntervalFileNode> sealedTsFiles;
  private int nextIntervalFileIndex;
  private SeriesReaderByTimestamp seriesReader;
  private QueryContext context;

  /**
   * init with seriesPath and sealedTsFiles.
   */
  public SealedTsFilesReaderByTimestamp(Path seriesPath, List<IntervalFileNode> sealedTsFiles,
      QueryContext context) {
    this.seriesPath = seriesPath;
    this.sealedTsFiles = sealedTsFiles;
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
    while (nextIntervalFileIndex < sealedTsFiles.size()) {
      initSingleTsFileReader(sealedTsFiles.get(nextIntervalFileIndex), context);
      nextIntervalFileIndex++;
      if(seriesReader.hasNext()){
        return true;
      }
    }
    return false;
  }

  @Override
  public void close() throws IOException {
    // file streams are managed uniformly.
  }

  // construct reader from the file that might overlap this timestamp
  private void constructReader(long timestamp) throws IOException {
    while (nextIntervalFileIndex < sealedTsFiles.size()) {
      if (singleTsFileSatisfied(sealedTsFiles.get(nextIntervalFileIndex), timestamp)) {
        initSingleTsFileReader(sealedTsFiles.get(nextIntervalFileIndex), context);
      }
      nextIntervalFileIndex++;
    }
  }

  /**
   * Judge whether the file should be skipped.
   */
  private boolean singleTsFileSatisfied(IntervalFileNode fileNode, long timestamp) {
    long endTime = fileNode.getEndTime(seriesPath.getDevice());
    return endTime >= timestamp;
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
    if (!pathModifications.isEmpty()) {
      QueryUtils.modifyChunkMetaData(metaDataList, pathModifications);
    }
    ChunkLoader chunkLoader = new ChunkLoaderImpl(tsFileReader);

    seriesReader = new SeriesReaderByTimestamp(chunkLoader, metaDataList);

  }

}
