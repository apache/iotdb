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
package org.apache.iotdb.db.metadata.tagSchemaRegion.tagIndex.flush;

import org.apache.iotdb.db.metadata.tagSchemaRegion.tagIndex.memtable.MemChunkGroup;
import org.apache.iotdb.db.metadata.tagSchemaRegion.tagIndex.memtable.MemTable;
import org.apache.iotdb.lsm.annotation.FlushProcessor;
import org.apache.iotdb.lsm.context.requestcontext.FlushRequestContext;
import org.apache.iotdb.lsm.levelProcess.FlushLevelProcessor;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/** flush for MemTable */
@FlushProcessor(level = 1)
public class MemTableFlush extends FlushLevelProcessor<MemTable, MemChunkGroup> {
  @Override
  public List<MemChunkGroup> getChildren(
      MemTable memNode, Object request, FlushRequestContext context) {
    Map<MemChunkGroup, Integer> memChunkGroupIndexMap = new HashMap<>();
    for (Map.Entry<String, MemChunkGroup> entry : memNode.getMemChunkGroupMap().entrySet()) {
      memChunkGroupIndexMap.put(entry.getValue(), context.getMemTableIndex(memNode));
    }
    context.setMemChunkGroupIndexMap(memChunkGroupIndexMap);
    return (List<MemChunkGroup>) memNode.getMemChunkGroupMap().values();
  }

  @Override
  public void flush(MemTable memNode, FlushRequestContext context) {}
}
