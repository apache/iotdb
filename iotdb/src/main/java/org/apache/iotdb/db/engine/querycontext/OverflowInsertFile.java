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
package org.apache.iotdb.db.engine.querycontext;

import java.util.List;
import org.apache.iotdb.tsfile.file.metadata.ChunkMetaData;

public class OverflowInsertFile {

  private String filePath;

  // seriesChunkMetadata of selected series
  private List<ChunkMetaData> timeSeriesChunkMetaData;

  public OverflowInsertFile() {
    //allowed to do nothing
  }

  public OverflowInsertFile(String path, List<ChunkMetaData> timeSeriesChunkMetaData) {
    this.filePath = path;
    this.timeSeriesChunkMetaData = timeSeriesChunkMetaData;
  }

  public String getFilePath() {
    return filePath;
  }

  public List<ChunkMetaData> getChunkMetaDataList() {
    return timeSeriesChunkMetaData;
  }

  public void setTimeSeriesChunkMetaData(List<ChunkMetaData> timeSeriesChunkMetaData) {
    this.timeSeriesChunkMetaData = timeSeriesChunkMetaData;
  }
}
