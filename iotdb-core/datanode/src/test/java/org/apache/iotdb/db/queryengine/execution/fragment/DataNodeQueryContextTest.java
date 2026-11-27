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

package org.apache.iotdb.db.queryengine.execution.fragment;

import org.apache.iotdb.db.queryengine.plan.relational.metadata.fetcher.cache.TableDeviceSchemaCache;
import org.apache.iotdb.db.queryengine.plan.relational.metadata.fetcher.cache.TableId;
import org.apache.iotdb.db.schemaengine.lease.MetadataLeaseManager;

import org.apache.tsfile.file.metadata.IDeviceID;
import org.apache.tsfile.file.metadata.StringArrayDeviceID;
import org.apache.tsfile.read.TimeValuePair;
import org.apache.tsfile.utils.TsPrimitiveType;
import org.junit.Assert;
import org.junit.Test;

public class DataNodeQueryContextTest {

  @Test
  public void testInvalidateDeclaredLastCachesOnFailure() {
    MetadataLeaseManager.getInstance().recoveryLeaseForTest(true);
    final TableDeviceSchemaCache cache = TableDeviceSchemaCache.getInstance();
    final IDeviceID device = new StringArrayDeviceID("root.__query_context_test_db.d1");
    final String database = "root.__query_context_test_db";
    final TimeValuePair value = new TimeValuePair(10L, new TsPrimitiveType.TsInt(10));
    final DataNodeQueryContext context = new DataNodeQueryContext();

    try {
      cache.declareLastCache(database, device, new String[] {"s1"}, false);
      cache.putBackLastCache(
          database, device, new String[] {"s1"}, null, new TimeValuePair[] {value}, false);
      Assert.assertEquals(value, cache.getLastEntry(null, device, "s1"));

      context.addDeclaredDevice(null, device);
      context.invalidateDeclaredLastCaches();

      Assert.assertTrue(context.isLastCacheInvalidationStarted());
      Assert.assertNull(cache.getLastEntry(null, device, "s1"));
    } finally {
      cache.invalidateDeviceEntry(new TableId(null, device.getTableName()), device);
    }
  }
}
