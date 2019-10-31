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

package org.apache.iotdb.db.engine.merge.util;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Deque;
import java.util.List;
import java.util.concurrent.Callable;
import org.apache.iotdb.tsfile.file.metadata.ChunkMetaData;
import org.apache.iotdb.tsfile.read.TsFileSequenceReader;
import org.apache.iotdb.tsfile.read.common.Chunk;

/**
 * SharedQueueProvider read the Chunks by the order of their disk offsets and store them in a queue,
 * which reduces random seeks.
 */
public class SharedQueueChunkProvider implements ChunkProvider{

  private static final int QUEUE_MAX_SIZE = 64;

  private Deque<Entry> queue = new ArrayDeque<>();
  private List<ChunkMetaData> allChunkMetadataList;
  private TsFileSequenceReader reader;

  public SharedQueueChunkProvider(List<ChunkMetaData>[] chunkMetadataLists, TsFileSequenceReader reader) {
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
      synchronized (queue) {
        Entry head = queue.peek();
        if (head == null || head.meta != chunkMetaData) {
          // the queue is empty or the peek is not what we want, wait until a new entry is ready
          queue.wait();
          continue;
        }
        // the entry contains what we want
        queue.poll();
        // notify other appliers and the provider that the head is taken away
        queue.notifyAll();
        return head.chunk;
      }
    }
  }

  private static class Entry {

    Entry(ChunkMetaData meta, Chunk chunk) {
      this.meta = meta;
      this.chunk = chunk;
    }

    private ChunkMetaData meta;
    private Chunk chunk;
  }

  private class ProviderTask implements Callable<Void> {

    @Override
    public Void call() throws Exception {
      for (ChunkMetaData chunkMetaData : allChunkMetadataList) {
        Chunk chunk = reader.readMemChunk(chunkMetaData);

        synchronized (queue) {
          int size = queue.size();
          if (size >= QUEUE_MAX_SIZE) {
            // wait until some entry is taken
            queue.wait();
            queue.addLast(new Entry(chunkMetaData, chunk));
            queue.notifyAll();
          } else {
            queue.addLast(new Entry(chunkMetaData, chunk));
            // notify appliers that a new entry is available
            queue.notifyAll();
          }
        }
      }
      return null;
    }
  }
}