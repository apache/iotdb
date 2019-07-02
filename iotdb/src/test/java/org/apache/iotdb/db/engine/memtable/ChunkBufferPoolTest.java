/**
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
package org.apache.iotdb.db.engine.memtable;

import java.util.concurrent.ConcurrentLinkedQueue;
import org.apache.iotdb.tsfile.file.metadata.enums.CompressionType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.apache.iotdb.tsfile.write.chunk.ChunkBuffer;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class ChunkBufferPoolTest {

  private ConcurrentLinkedQueue<ChunkBuffer> chunkBuffers;
  private Thread thread = new ReturnThread();

  @Before
  public void setUp() throws Exception {
    chunkBuffers = new ConcurrentLinkedQueue();
    thread.start();
  }

  @After
  public void tearDown() throws Exception {
    thread.interrupt();
  }

  @Test
  public void testGetAndRelease() {
    for (int i = 0; i < 10; i++) {
      ChunkBuffer chunk = ChunkBufferPool.getInstance().getEmptyChunkBuffer("test case",
          new MeasurementSchema("node", TSDataType.INT32, TSEncoding.PLAIN,
              CompressionType.SNAPPY));
      chunkBuffers.add(chunk);
    }
  }

  class ReturnThread extends Thread {

    @Override
    public void run() {
      while (true) {
        if(isInterrupted()){
          break;
        }
        ChunkBuffer chunkBuffer = chunkBuffers.poll();
        if (chunkBuffer == null) {
          try {
            Thread.sleep(10);
          } catch (InterruptedException e) {
          }
          continue;
        }
        try {
          Thread.sleep(10);
        } catch (InterruptedException e) {
        }
        chunkBuffers.remove(chunkBuffer);
        ChunkBufferPool.getInstance().putBack(chunkBuffer);
      }
    }
  }


}
