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
package org.apache.iotdb.db.metadata.tagSchemaRegion.tagIndex.response;

import org.apache.iotdb.db.metadata.tagSchemaRegion.tagIndex.file.entry.TiFile;
import org.apache.iotdb.db.metadata.tagSchemaRegion.tagIndex.memtable.MemChunk;
import org.apache.iotdb.db.metadata.tagSchemaRegion.tagIndex.memtable.MemChunkGroup;
import org.apache.iotdb.db.metadata.tagSchemaRegion.tagIndex.memtable.MemTable;
import org.apache.iotdb.lsm.response.BaseResponse;

import java.util.HashMap;
import java.util.Map;

public class FlushResponse extends BaseResponse<Map<Integer, TiFile>> {

  Map<MemTable, Long> memTableIndexMap;

  Map<MemChunkGroup, Long> tagKeyOffsetMap;

  Map<MemChunk, Long> memChunkOffsetMap;

  public FlushResponse() {
    memChunkOffsetMap = new HashMap<>();
    tagKeyOffsetMap = new HashMap<>();
    memTableIndexMap = new HashMap<>();
  }

  public Map<MemTable, Long> getMemTableIndexMap() {
    return memTableIndexMap;
  }

  public void setMemTableIndexMap(Map<MemTable, Long> memTableIndexMap) {
    this.memTableIndexMap = memTableIndexMap;
  }

  public Map<MemChunkGroup, Long> getTagKeyOffsetMap() {
    return tagKeyOffsetMap;
  }

  public void setTagKeyOffsetMap(Map<MemChunkGroup, Long> tagKeyOffsetMap) {
    this.tagKeyOffsetMap = tagKeyOffsetMap;
  }

  public Map<MemChunk, Long> getMemChunkOffsetMap() {
    return memChunkOffsetMap;
  }

  public void setMemChunkOffsetMap(Map<MemChunk, Long> memChunkOffsetMap) {
    this.memChunkOffsetMap = memChunkOffsetMap;
  }

  public Long getMemTableIndex(MemTable memTable) {
    return memTableIndexMap.get(memTable);
  }

  public Long getTagKeyOffset(MemChunkGroup memChunkGroup) {
    return tagKeyOffsetMap.get(memChunkGroup);
  }

  public void addChunkOffset(MemChunk memNode, long offset) {
    memChunkOffsetMap.put(memNode, offset);
  }

  public Long getChunkOffset(MemChunk memNode) {
    return memChunkOffsetMap.get(memNode);
  }

  public void addTagKeyOffset(MemChunkGroup memNode, Long offset) {
    tagKeyOffsetMap.put(memNode, offset);
  }
}
