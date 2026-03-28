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

package org.apache.iotdb.confignode.procedure.impl.subscription.runtime;

import org.apache.iotdb.confignode.procedure.impl.subscription.consumer.runtime.CommitProgressSyncProcedure;
import org.apache.iotdb.rpc.subscription.payload.poll.RegionProgress;
import org.apache.iotdb.rpc.subscription.payload.poll.WriterId;
import org.apache.iotdb.rpc.subscription.payload.poll.WriterProgress;

import org.junit.Test;

import java.lang.reflect.Method;
import java.util.LinkedHashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;

public class SubscriptionProgressMergeTest {

  @Test
  public void testCommitProgressSyncProcedureMergesPerWriterByMax() throws Exception {
    final RegionProgress left =
        createRegionProgress(
            "1_1",
            new WriterId("1_1", 7, 1L),
            new WriterProgress(100L, 10L),
            new WriterId("1_1", 8, 1L),
            new WriterProgress(90L, 9L));
    final RegionProgress right =
        createRegionProgress(
            "1_1",
            new WriterId("1_1", 7, 1L),
            new WriterProgress(95L, 8L),
            new WriterId("1_1", 8, 1L),
            new WriterProgress(110L, 11L));

    final RegionProgress merged =
        invokeMergeRegionProgress(CommitProgressSyncProcedure.class, left, right);

    assertEquals(
        new WriterProgress(100L, 10L), merged.getWriterPositions().get(new WriterId("1_1", 7, 1L)));
    assertEquals(
        new WriterProgress(110L, 11L), merged.getWriterPositions().get(new WriterId("1_1", 8, 1L)));
  }

  @Test
  public void testLeaderChangeProcedureMergesPerWriterByMax() throws Exception {
    final RegionProgress left =
        createRegionProgress(
            "1_2",
            new WriterId("1_2", 9, 3L),
            new WriterProgress(200L, 20L),
            new WriterId("1_2", 10, 3L),
            new WriterProgress(150L, 15L));
    final RegionProgress right =
        createRegionProgress(
            "1_2",
            new WriterId("1_2", 9, 3L),
            new WriterProgress(220L, 18L),
            new WriterId("1_2", 10, 3L),
            new WriterProgress(140L, 14L));

    final RegionProgress merged =
        invokeMergeRegionProgress(SubscriptionHandleLeaderChangeProcedure.class, left, right);

    assertEquals(
        new WriterProgress(220L, 18L), merged.getWriterPositions().get(new WriterId("1_2", 9, 3L)));
    assertEquals(
        new WriterProgress(150L, 15L),
        merged.getWriterPositions().get(new WriterId("1_2", 10, 3L)));
  }

  private static RegionProgress invokeMergeRegionProgress(
      final Class<?> clazz, final RegionProgress left, final RegionProgress right)
      throws Exception {
    final Method method =
        clazz.getDeclaredMethod("mergeRegionProgress", RegionProgress.class, RegionProgress.class);
    method.setAccessible(true);
    return (RegionProgress) method.invoke(null, left, right);
  }

  private static RegionProgress createRegionProgress(
      final String regionId,
      final WriterId firstWriterId,
      final WriterProgress firstWriterProgress,
      final WriterId secondWriterId,
      final WriterProgress secondWriterProgress) {
    final Map<WriterId, WriterProgress> writerPositions = new LinkedHashMap<>();
    writerPositions.put(firstWriterId, firstWriterProgress);
    writerPositions.put(secondWriterId, secondWriterProgress);
    return new RegionProgress(writerPositions);
  }
}
