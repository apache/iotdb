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

package org.apache.iotdb.db.storageengine.dataregion.flush;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

/** The temporary memory reserved for a flush (reduces the MemTable budget while it runs). */
public class FlushMemoryEstimateTest {

  private static final long MB = 1024 * 1024;

  @Test
  public void manySeriesReserveTwiceTheQueuedShare() {
    // 300 series, a queue of 10 encoded chunks: 10 series' share of the memtable, doubled for the
    // growth slack of the chunk buffers
    assertEquals(
        30 * MB / 300 * 10 * 2, MemTableFlushTask.estimateTemporaryMemory(30 * MB, 300, 10, true));
  }

  @Test
  public void fewSeriesReserveAtMostTwiceTheMemTable() {
    // 3 series cannot fill 10 queue slots: at most the memtable itself is queued
    assertEquals(104 * MB, MemTableFlushTask.estimateTemporaryMemory(52 * MB, 3, 10, true));
    assertEquals(104 * MB, MemTableFlushTask.estimateTemporaryMemory(52 * MB, 1, 10, true));
    assertEquals(104 * MB, MemTableFlushTask.estimateTemporaryMemory(52 * MB, 10, 10, true));
  }

  @Test
  public void noReservationWithoutABoundedQueueOrSeries() {
    assertEquals(0, MemTableFlushTask.estimateTemporaryMemory(52 * MB, 3, 10, false));
    assertEquals(0, MemTableFlushTask.estimateTemporaryMemory(52 * MB, 0, 10, true));
  }
}
