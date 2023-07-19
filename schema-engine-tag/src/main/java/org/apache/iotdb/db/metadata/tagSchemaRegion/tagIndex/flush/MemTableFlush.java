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

import org.apache.iotdb.db.metadata.tagSchemaRegion.tagIndex.file.entry.TiFileHeader;
import org.apache.iotdb.db.metadata.tagSchemaRegion.tagIndex.file.index.OffsetIndex;
import org.apache.iotdb.db.metadata.tagSchemaRegion.tagIndex.memtable.MemChunkGroup;
import org.apache.iotdb.db.metadata.tagSchemaRegion.tagIndex.memtable.MemTable;
import org.apache.iotdb.db.metadata.tagSchemaRegion.tagIndex.request.FlushRequest;
import org.apache.iotdb.db.metadata.tagSchemaRegion.tagIndex.response.FlushResponse;
import org.apache.iotdb.lsm.annotation.FlushProcessor;
import org.apache.iotdb.lsm.context.requestcontext.FlushRequestContext;
import org.apache.iotdb.lsm.levelProcess.FlushLevelProcessor;
import org.apache.iotdb.lsm.sstable.fileIO.SSTableOutputStream;
import org.apache.iotdb.lsm.sstable.index.IndexType;
import org.apache.iotdb.lsm.util.BloomFilter;

import java.io.File;
import java.io.IOException;
import java.util.Collection;
import java.util.Map;

/** flush for MemTable */
@FlushProcessor(level = 0)
public class MemTableFlush extends FlushLevelProcessor<MemTable, MemChunkGroup, FlushRequest> {
  @Override
  public Collection<MemChunkGroup> getChildren(
      MemTable memNode, FlushRequest request, FlushRequestContext context) {
    return memNode.getMemChunkGroupMap().values();
  }

  @Override
  public void flush(MemTable memNode, FlushRequest flushRequest, FlushRequestContext context)
      throws IOException {
    if (memNode.getDeletionList() != null && memNode.getDeletionList().size() != 0) {
      flushDeletionList(memNode, flushRequest, context);
    }
    OffsetIndex tagKeyToOffset = new OffsetIndex(IndexType.BPlusTree);
    FlushResponse flushResponse = context.getResponse();
    for (Map.Entry<String, MemChunkGroup> entry : memNode.getMemChunkGroupMap().entrySet()) {
      tagKeyToOffset.put(entry.getKey(), flushResponse.getTagKeyOffset(entry.getValue()));
    }
    SSTableOutputStream fileOutput = context.getFileOutput();
    TiFileHeader tiFileHeader = new TiFileHeader();
    tiFileHeader.setTagKeyIndexOffset(tagKeyToOffset.serialize(fileOutput));
    BloomFilter bloomFilter = BloomFilter.getEmptyBloomFilter(0.05, 3);
    addToBloomFilter(bloomFilter, memNode);
    tiFileHeader.setBloomFilterOffset(fileOutput.write(bloomFilter));
    fileOutput.write(tiFileHeader);
    fileOutput.flush();
  }

  private void flushDeletionList(
      MemTable memNode, FlushRequest flushRequest, FlushRequestContext context) throws IOException {
    File deletionFile =
        new File(flushRequest.getFlushDirPath(), flushRequest.getFlushDeletionFileName());
    if (!deletionFile.exists()) {
      deletionFile.createNewFile();
    }
    SSTableOutputStream fileOutput = new SSTableOutputStream(deletionFile);
    for (Integer deletion : memNode.getDeletionList()) {
      fileOutput.writeInt(deletion);
    }
    fileOutput.flush();
    fileOutput.close();
  }

  private void addToBloomFilter(BloomFilter bloomFilter, MemTable memNode) {
    for (Map.Entry<String, MemChunkGroup> entry : memNode.getMemChunkGroupMap().entrySet()) {
      String tagKey = entry.getKey();
      Collection<String> tagValues = entry.getValue().getMemChunkMap().keySet();
      bloomFilter.add(tagKey, tagValues);
    }
  }
}
