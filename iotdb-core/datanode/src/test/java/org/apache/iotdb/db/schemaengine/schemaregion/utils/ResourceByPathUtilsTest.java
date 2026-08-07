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
package org.apache.iotdb.db.schemaengine.schemaregion.utils;

import org.apache.iotdb.commons.path.MeasurementPath;
import org.apache.iotdb.db.queryengine.execution.fragment.QueryContext;
import org.apache.iotdb.db.storageengine.dataregion.memtable.IWritableMemChunk;
import org.apache.iotdb.db.utils.datastructure.TVList;

import org.apache.tsfile.enums.TSDataType;
import org.junit.Assert;
import org.junit.Test;

import java.util.Collections;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class ResourceByPathUtilsTest {

  @Test
  public void testFlushingQueryLocksTemporaryTVListBeforeRegistration() throws Exception {
    TVList candidate = TVList.newList(TSDataType.INT64);
    candidate.putLong(2, 2);
    candidate.putLong(1, 1);
    Assert.assertFalse(candidate.isSorted());

    QueryContext previousQuery = new QueryContext(1, false);
    candidate.lockQueryList();
    try {
      candidate.getQueryContextSet().add(previousQuery);
    } finally {
      candidate.unlockQueryList();
    }

    TVList temporaryList = candidate.cloneForFlushSort();
    IWritableMemChunk memChunk = mock(IWritableMemChunk.class);
    when(memChunk.getSortedList()).thenReturn(Collections.emptyList());
    when(memChunk.getWorkingTVList()).thenReturn(candidate);
    CountDownLatch temporaryListInitialized = new CountDownLatch(1);
    when(memChunk.initWorkingListForFlushIfNecessary(candidate, true))
        .thenAnswer(
            ignored -> {
              temporaryListInitialized.countDown();
              return temporaryList;
            });

    ResourceByPathUtils resourceByPathUtils =
        ResourceByPathUtils.getResourceInstance(
            new MeasurementPath("root.test.d.s", TSDataType.INT64));
    QueryContext currentQuery = new QueryContext(2, false);
    ExecutorService executor = Executors.newSingleThreadExecutor();
    Future<Map<TVList, Integer>> result = null;
    try {
      temporaryList.lockQueryList();
      try {
        result =
            executor.submit(
                () ->
                    resourceByPathUtils.prepareTvListMapForQuery(
                        currentQuery, memChunk, false, null, null));
        Assert.assertTrue(temporaryListInitialized.await(3, TimeUnit.SECONDS));
        Future<Map<TVList, Integer>> blockedResult = result;
        Assert.assertThrows(
            TimeoutException.class, () -> blockedResult.get(200, TimeUnit.MILLISECONDS));
      } finally {
        temporaryList.unlockQueryList();
      }

      Map<TVList, Integer> tvListQueryMap = result.get(3, TimeUnit.SECONDS);
      Assert.assertTrue(tvListQueryMap.containsKey(temporaryList));
      temporaryList.lockQueryList();
      try {
        Assert.assertTrue(temporaryList.getQueryContextSet().contains(currentQuery));
      } finally {
        temporaryList.unlockQueryList();
      }
    } finally {
      if (result != null) {
        result.cancel(true);
      }
      executor.shutdownNow();
      executor.awaitTermination(3, TimeUnit.SECONDS);
    }
  }
}
