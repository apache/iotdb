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

import org.apache.iotdb.db.metadata.tagSchemaRegion.tagIndex.memtable.MemChunk;
import org.apache.iotdb.db.metadata.tagSchemaRegion.tagIndex.memtable.MemChunkGroup;
import org.apache.iotdb.db.metadata.tagSchemaRegion.tagIndex.response.FlushResponse;
import org.apache.iotdb.lsm.annotation.FlushProcessor;
import org.apache.iotdb.lsm.context.requestcontext.FlushRequestContext;
import org.apache.iotdb.lsm.levelProcess.FlushLevelProcessor;
import org.apache.iotdb.lsm.request.IFlushRequest;
import org.apache.iotdb.lsm.sstable.bplustree.writer.BPlusTreeWriter;
import org.apache.iotdb.lsm.sstable.fileIO.FileOutput;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/** flush for MemChunkGroup */
@FlushProcessor(level = 1)
public class MemChunkGroupFlush extends FlushLevelProcessor<MemChunkGroup, MemChunk> {

  @Override
  public List<MemChunk> getChildren(
      MemChunkGroup memNode, IFlushRequest request, FlushRequestContext context) {
    return (List<MemChunk>) memNode.getMemChunkGroupMap().values();
  }

  @Override
  public void flush(MemChunkGroup memNode, IFlushRequest request, FlushRequestContext context)
      throws IOException {
    List<MemChunk> memChunks = getChildren(memNode, null, context);
    Map<MemChunk, String> memChunkGroupMapReverse =
        memNode.getMemChunkGroupMap().entrySet().stream()
            .collect(HashMap::new, (m, v) -> m.put(v.getValue(), v.getKey()), HashMap::putAll);
    Map<String, Long> tagValueToOffset = new HashMap<>();
    FlushResponse flushResponse = context.getResponse();
    for (MemChunk memChunk : memChunks) {
      tagValueToOffset.put(
          memChunkGroupMapReverse.get(memChunk), flushResponse.getChunkOffset(memChunk));
    }
    FileOutput fileOutput = context.getFileOutput();
    BPlusTreeWriter bPlusTreeWriter = new BPlusTreeWriter(fileOutput);
    Long offset = bPlusTreeWriter.write(tagValueToOffset, false);
    flushResponse.addTagKeyOffset(memNode, offset);
  }
}
