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
package org.apache.iotdb.lsm.context.requestcontext;

import org.apache.iotdb.db.metadata.tagSchemaRegion.tagIndex.memtable.MemChunk;
import org.apache.iotdb.db.metadata.tagSchemaRegion.tagIndex.memtable.MemChunkGroup;
import org.apache.iotdb.db.metadata.tagSchemaRegion.tagIndex.memtable.MemTable;
import org.apache.iotdb.lsm.strategy.RBFSAccessStrategy;

import java.util.Map;

/**
 * represents the context of a flush request, this class can be extended to implement a custom
 * context
 */
public class FlushRequestContext extends RequestContext {

  Map<MemTable, Integer> memTableIndexMap;

  Map<MemChunkGroup, Integer> memChunkGroupIndexMap;

  Map<MemChunk, Integer> memChunkIndexMap;

  public FlushRequestContext() {
    super();
    // use the reverse breadth-first traversal strategy to access memory nodes
    accessStrategy = new RBFSAccessStrategy();
  }

  public Map<MemTable, Integer> getMemTableIndexMap() {
    return memTableIndexMap;
  }

  public void setMemTableIndexMap(Map<MemTable, Integer> memTableIndexMap) {
    this.memTableIndexMap = memTableIndexMap;
  }

  public Map<MemChunkGroup, Integer> getMemChunkGroupIndexMap() {
    return memChunkGroupIndexMap;
  }

  public void setMemChunkGroupIndexMap(Map<MemChunkGroup, Integer> memChunkGroupIndexMap) {
    this.memChunkGroupIndexMap = memChunkGroupIndexMap;
  }

  public Map<MemChunk, Integer> getMemChunkIndexMap() {
    return memChunkIndexMap;
  }

  public void setMemChunkIndexMap(Map<MemChunk, Integer> memChunkIndexMap) {
    this.memChunkIndexMap = memChunkIndexMap;
  }

  public Integer getMemTableIndex(MemTable memTable) {
    return memTableIndexMap.get(memTable);
  }

  public Integer getMemChunkGroupIndex(MemChunkGroup memChunkGroup) {
    return memChunkGroupIndexMap.get(memChunkGroup);
  }
}
