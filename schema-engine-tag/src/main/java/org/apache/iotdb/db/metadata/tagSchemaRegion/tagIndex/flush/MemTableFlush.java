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

import org.apache.iotdb.db.metadata.tagSchemaRegion.tagIndex.file.entry.BloomFilter;
import org.apache.iotdb.db.metadata.tagSchemaRegion.tagIndex.memtable.MemChunk;
import org.apache.iotdb.db.metadata.tagSchemaRegion.tagIndex.memtable.MemChunkGroup;
import org.apache.iotdb.db.metadata.tagSchemaRegion.tagIndex.memtable.MemTable;
import org.apache.iotdb.db.metadata.tagSchemaRegion.tagIndex.request.FlushRequest;
import org.apache.iotdb.db.metadata.tagSchemaRegion.tagIndex.response.FlushResponse;
import org.apache.iotdb.lsm.annotation.FlushProcessor;
import org.apache.iotdb.lsm.context.requestcontext.FlushRequestContext;
import org.apache.iotdb.lsm.levelProcess.FlushLevelProcessor;
import org.apache.iotdb.lsm.sstable.bplustree.writer.BPlusTreeWriter;
import org.apache.iotdb.lsm.sstable.fileIO.FileOutput;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/** flush for MemTable */
@FlushProcessor(level = 0)
public class MemTableFlush extends FlushLevelProcessor<MemTable, MemChunkGroup, FlushRequest> {
  @Override
  public List<MemChunkGroup> getChildren(
      MemTable memNode, FlushRequest request, FlushRequestContext context) {
    return new ArrayList<>(memNode.getMemChunkGroupMap().values());
  }

  @Override
  public void flush(MemTable memNode, FlushRequest flushRequest, FlushRequestContext context)
      throws IOException {
    List<MemChunkGroup> memChunkGroups = getChildren(memNode, null, context);
    Map<MemChunkGroup, String> memChunkGroupMapReverse =
        memNode.getMemChunkGroupMap().entrySet().stream()
            .collect(HashMap::new, (m, v) -> m.put(v.getValue(), v.getKey()), HashMap::putAll);
    Map<String, Long> tagKeyToOffset = new HashMap<>();
    FlushResponse flushResponse = context.getResponse();
    for (MemChunkGroup memChunkGroup : memChunkGroups) {
      tagKeyToOffset.put(
          memChunkGroupMapReverse.get(memChunkGroup), flushResponse.getTagKeyOffset(memChunkGroup));
    }
    FileOutput fileOutput = context.getFileOutput();
    BPlusTreeWriter bPlusTreeWriter = new BPlusTreeWriter(fileOutput);
    bPlusTreeWriter.write(tagKeyToOffset, false);
    List<String> tagKeyAndValues = getTagKeyAndValues(memNode);
    BloomFilter bloomFilter = BloomFilter.getEmptyBloomFilter(0.05, 3);
    for (String key : tagKeyAndValues) {
      bloomFilter.add(key);
    }
    fileOutput.write(bloomFilter);
    fileOutput.flush();
  }

  private List<String> getTagKeyAndValues(MemTable memNode) {
    List<String> tagKeyAndValues = new ArrayList<>();
    for (Map.Entry<String, MemChunkGroup> entry : memNode.getMemChunkGroupMap().entrySet()) {
      String tagKey = entry.getKey();
      for (Map.Entry<String, MemChunk> tagValueEntry :
          entry.getValue().getMemChunkMap().entrySet()) {
        String tagValue = tagValueEntry.getKey();
        tagKeyAndValues.add(tagKey + tagValue);
      }
    }
    return tagKeyAndValues;
  }
}
