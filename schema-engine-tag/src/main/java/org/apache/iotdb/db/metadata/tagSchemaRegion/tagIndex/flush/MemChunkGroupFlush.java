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

import org.apache.iotdb.db.metadata.tagSchemaRegion.tagIndex.file.index.OffsetIndex;
import org.apache.iotdb.db.metadata.tagSchemaRegion.tagIndex.memtable.MemChunk;
import org.apache.iotdb.db.metadata.tagSchemaRegion.tagIndex.memtable.MemChunkGroup;
import org.apache.iotdb.db.metadata.tagSchemaRegion.tagIndex.request.FlushRequest;
import org.apache.iotdb.db.metadata.tagSchemaRegion.tagIndex.response.FlushResponse;
import org.apache.iotdb.lsm.annotation.FlushProcessor;
import org.apache.iotdb.lsm.context.requestcontext.FlushRequestContext;
import org.apache.iotdb.lsm.levelProcess.FlushLevelProcessor;
import org.apache.iotdb.lsm.sstable.fileIO.SSTableOutputStream;
import org.apache.iotdb.lsm.sstable.index.IndexType;

import java.io.IOException;
import java.util.Collection;
import java.util.Map;

/** flush for MemChunkGroup */
@FlushProcessor(level = 1)
public class MemChunkGroupFlush extends FlushLevelProcessor<MemChunkGroup, MemChunk, FlushRequest> {

  @Override
  public Collection<MemChunk> getChildren(
      MemChunkGroup memNode, FlushRequest request, FlushRequestContext context) {
    return memNode.getMemChunkMap().values();
  }

  @Override
  public void flush(MemChunkGroup memNode, FlushRequest request, FlushRequestContext context)
      throws IOException {
    OffsetIndex tagValueToOffset = new OffsetIndex(IndexType.BPlusTree);
    FlushResponse flushResponse = context.getResponse();
    for (Map.Entry<String, MemChunk> entry : memNode.getMemChunkMap().entrySet()) {
      tagValueToOffset.put(entry.getKey(), flushResponse.getChunkOffset(entry.getValue()));
    }
    SSTableOutputStream fileOutput = context.getFileOutput();
    Long offset = tagValueToOffset.serialize(fileOutput);
    flushResponse.addTagKeyOffset(memNode, offset);
  }
}
