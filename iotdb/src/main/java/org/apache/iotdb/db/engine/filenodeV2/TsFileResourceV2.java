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
package org.apache.iotdb.db.engine.filenodeV2;

import java.io.File;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.iotdb.db.engine.modification.ModificationFile;
import org.apache.iotdb.db.engine.querycontext.ReadOnlyMemChunk;
import org.apache.iotdb.tsfile.file.metadata.ChunkMetaData;

public class TsFileResourceV2 {

  private File file;

  /**
   * device -> start time
   */
  private Map<String, Long> startTimeMap;

  /**
   * device -> end time
   * null if it's an unsealed tsfile
   */
  private Map<String, Long> endTimeMap;

  private UnsealedTsFileProcessorV2 processor;

  private transient ModificationFile modFile;

  private boolean closed = false;

  /**
   * Chunk metadata list of unsealed tsfile
   */
  private List<ChunkMetaData> chunkMetaDatas;


  /**
   * Mem chunk data
   */
  private ReadOnlyMemChunk readOnlyMemChunk;

  public TsFileResourceV2(File file, UnsealedTsFileProcessorV2 processor) {
    this.file = file;
    this.startTimeMap = new HashMap<>();
    this.endTimeMap = new HashMap<>();
    this.processor = processor;
  }

  public TsFileResourceV2(File file,
      ReadOnlyMemChunk readOnlyMemChunk,
      List<ChunkMetaData> chunkMetaDatas) {
    this.file = file;
    this.chunkMetaDatas = chunkMetaDatas;
    this.readOnlyMemChunk = readOnlyMemChunk;
  }

  public void updateStartTime(String device, long time) {
    startTimeMap.putIfAbsent(device, time);
    long startTime = startTimeMap.get(device);
    if (time < startTimeMap.get(device)) {
      startTimeMap.put(device, startTime);
    }
  }

  public List<ChunkMetaData> getChunkMetaDatas() {
    return chunkMetaDatas;
  }

  public ReadOnlyMemChunk getReadOnlyMemChunk() {
    return readOnlyMemChunk;
  }

  public ModificationFile getModFile() {
    return modFile;
  }

  public File getFile() {
    return file;
  }

  public long getFileSize() {
    return file.length();
  }

  public Map<String, Long> getStartTimeMap() {
    return startTimeMap;
  }

  public void setEndTimeMap(Map<String, Long> endTimeMap) {
    this.endTimeMap = endTimeMap;
  }

  public Map<String, Long> getEndTimeMap() {
    return endTimeMap;
  }

  public boolean isClosed() {
    return closed;
  }

  public void setClosed(boolean closed) {
    this.closed = closed;
  }

  public UnsealedTsFileProcessorV2 getUnsealedFileProcessor() {
    return processor;
  }
}
