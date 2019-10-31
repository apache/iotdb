/*
 *
 *  * Licensed to the Apache Software Foundation (ASF) under one
 *  * or more contributor license agreements.  See the NOTICE file
 *  * distributed with this work for additional information
 *  * regarding copyright ownership.  The ASF licenses this file
 *  * to you under the Apache License, Version 2.0 (the
 *  * "License"); you may not use this file except in compliance
 *  * with the License.  You may obtain a copy of the License at
 *  *
 *  *     http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing,
 *  * software distributed under the License is distributed on an
 *  * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  * KIND, either express or implied.  See the License for the
 *  * specific language governing permissions and limitations
 *  * under the License.
 *
 */

package org.apache.iotdb.db.engine.merge.util;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Deque;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.iotdb.tsfile.file.metadata.ChunkMetaData;
import org.apache.iotdb.tsfile.read.TsFileSequenceReader;
import org.apache.iotdb.tsfile.read.common.Chunk;

/**
 * SharedQueueProvider read the Chunks by the order of their disk offsets and store them in a queue,
 * which reduces random seeks.
 */
public class SharedMapChunkProvider implements ChunkProvider{

  private static final int MAX_SIZE = 512;

  private Map<ChunkMetaData, Chunk> map = new ConcurrentHashMap<>();
  private List<ChunkMetaData> allChunkMetadataList;
  private TsFileSequenceReader reader;

  public SharedMapChunkProvider(List<ChunkMetaData>[] chunkMetadataLists, TsFileSequenceReader reader) {
    allChunkMetadataList = new ArrayList<>();
    for (List<ChunkMetaData> chunkMetaDataList : chunkMetadataLists) {
      allChunkMetadataList.addAll(chunkMetaDataList);
    }
    allChunkMetadataList.sort(Comparator.comparing(ChunkMetaData::getOffsetOfChunkHeader));
    this.reader = reader;
    ChunkProviderExecutor.getINSTANCE().submit(new ProviderTask());
  }


  public Chunk require(ChunkMetaData chunkMetaData) throws InterruptedException {
    while (true) {
      synchronized (map) {
        Chunk chunk = map.get(chunkMetaData);
        if (chunk == null) {
          // the queue is empty or the peek is not what we want, wait until a new entry is ready
          map.wait();
          continue;
        }
        // the entry contains what we want
        map.remove(chunkMetaData);
        // notify other appliers and the provider that the head is taken away
        map.notifyAll();
        return chunk;
      }
    }
  }

  private class ProviderTask implements Callable<Void> {

    @Override
    public Void call() throws Exception {
      for (ChunkMetaData chunkMetaData : allChunkMetadataList) {
        Chunk chunk = reader.readMemChunk(chunkMetaData);

        synchronized (map) {
          int size = map.size();
          if (size >= MAX_SIZE) {
            // wait until some entry is taken
            map.wait();
            map.put(chunkMetaData, chunk);
            map.notifyAll();
          } else {
            // notify appliers that a new entry is available
            map.put(chunkMetaData, chunk);
            map.notifyAll();
          }
        }
      }
      return null;
    }
  }
}