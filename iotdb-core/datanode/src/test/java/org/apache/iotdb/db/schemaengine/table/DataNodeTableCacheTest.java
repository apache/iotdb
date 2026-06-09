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

package org.apache.iotdb.db.schemaengine.table;

import org.junit.Assert;
import org.junit.Test;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.Semaphore;

public class DataNodeTableCacheTest {

  @Test
  public void testInterruptedFetchDoesNotReleaseUnacquiredPermit() throws Exception {
    DataNodeTableCache cache = DataNodeTableCache.getInstance();
    Semaphore semaphore = getFetchTableSemaphore(cache);
    int permitsBeforeFetch = semaphore.availablePermits();

    Thread.currentThread().interrupt();
    try {
      getTablesInConfigNode(cache);

      Assert.assertEquals(permitsBeforeFetch, semaphore.availablePermits());
      Assert.assertTrue(Thread.currentThread().isInterrupted());
    } finally {
      Thread.interrupted();
    }
  }

  private Semaphore getFetchTableSemaphore(DataNodeTableCache cache) throws Exception {
    Field semaphoreField = DataNodeTableCache.class.getDeclaredField("fetchTableSemaphore");
    semaphoreField.setAccessible(true);
    return (Semaphore) semaphoreField.get(cache);
  }

  private void getTablesInConfigNode(DataNodeTableCache cache) throws Exception {
    Method method = DataNodeTableCache.class.getDeclaredMethod("getTablesInConfigNode", Map.class);
    method.setAccessible(true);
    method.invoke(cache, Collections.emptyMap());
  }
}
