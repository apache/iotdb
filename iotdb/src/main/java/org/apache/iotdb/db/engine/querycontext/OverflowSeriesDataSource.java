/**
 * Copyright © 2019 Apache IoTDB(incubating) (dev@iotdb.apache.org)
 *
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
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.iotdb.db.engine.querycontext;

import java.util.List;
import org.apache.iotdb.db.engine.memtable.TimeValuePairSorter;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.common.Path;

public class OverflowSeriesDataSource {

  private Path seriesPath;
  private TSDataType dataType;
  // overflow tsfile
  private List<OverflowInsertFile> overflowInsertFileList;
  // unSeq mem-table
  private TimeValuePairSorter readableMemChunk;

  public OverflowSeriesDataSource(Path seriesPath) {
    this.seriesPath = seriesPath;
  }

  public OverflowSeriesDataSource(Path seriesPath, TSDataType dataType,
      List<OverflowInsertFile> overflowInsertFileList, TimeValuePairSorter readableMemChunk) {
    this.seriesPath = seriesPath;
    this.dataType = dataType;
    this.overflowInsertFileList = overflowInsertFileList;
    this.readableMemChunk = readableMemChunk;
  }

  public List<OverflowInsertFile> getOverflowInsertFileList() {
    return overflowInsertFileList;
  }

  public void setOverflowInsertFileList(List<OverflowInsertFile> overflowInsertFileList) {
    this.overflowInsertFileList = overflowInsertFileList;
  }

  public TimeValuePairSorter getReadableMemChunk() {
    return readableMemChunk;
  }

  public void setReadableMemChunk(TimeValuePairSorter rawChunk) {
    this.readableMemChunk = rawChunk;
  }

  public Path getSeriesPath() {
    return seriesPath;
  }

  public void setSeriesPath(Path seriesPath) {
    this.seriesPath = seriesPath;
  }

  public TSDataType getDataType() {
    return dataType;
  }

  public boolean hasRawChunk() {
    return readableMemChunk != null && !readableMemChunk.isEmpty();
  }
}
