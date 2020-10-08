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
package org.apache.iotdb.db.engine.memtable;

import java.util.concurrent.ConcurrentLinkedQueue;
import org.apache.iotdb.db.rescon.MemTablePool;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class MemTablePoolTest {

  private ConcurrentLinkedQueue<IMemTable> memTables;
  private Thread thread = new ReturnThread();
  private volatile boolean isFinished = false;

  @Before
  public void setUp() throws Exception {
    memTables = new ConcurrentLinkedQueue();
    thread.start();
  }

  @After
  public void tearDown() throws Exception {
    isFinished = true;
    thread.join();
  }

  @Test
  public void testGetAndRelease() {
    long time = System.currentTimeMillis();
    for (int i = 0; i < 10; i++) {
      IMemTable memTable = MemTablePool.getInstance().getAvailableMemTable("test case");
      memTables.add(memTable);
    }
    time -= System.currentTimeMillis();
    System.out.println("memtable pool use deque and synchronized consume:" + time);
  }

  class ReturnThread extends Thread {

    @Override
    public void run() {
      while (true) {
        if (isInterrupted()) {
          break;
        }
        IMemTable memTable = memTables.poll();
        if (memTable == null) {
          if (isFinished) {
            break;
          }
          try {
            Thread.sleep(10);
          } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
          }
          continue;
        }
        memTables.remove(memTable);
        MemTablePool.getInstance().putBack(memTable, "test case");
      }
    }
  }

}