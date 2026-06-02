/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iotdb.db.storageengine.dataregion.compaction.utils;

import org.apache.iotdb.commons.exception.IllegalPathException;
import org.apache.iotdb.commons.exception.MetadataException;
import org.apache.iotdb.db.exception.StorageEngineException;
import org.apache.iotdb.db.queryengine.plan.analyze.cache.schema.DataNodeTTLCache;
import org.apache.iotdb.db.schemaengine.lease.MetadataLeaseManager;
import org.apache.iotdb.db.storageengine.dataregion.compaction.AbstractCompactionTest;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.utils.MultiTsFileDeviceIterator;
import org.apache.iotdb.db.storageengine.dataregion.read.control.FileReaderManager;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResource;

import org.apache.tsfile.exception.write.WriteProcessException;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

/**
 * Compaction physically deletes data older than its TTL window. A DataNode partitioned from the
 * ConfigNode may hold a stale TTL (it could have missed a ConfigNode TTL update while partitioned),
 * and a too-short stale TTL would make compaction permanently delete data that a missed
 * TTL-increase says to keep. While the metadata lease is fenced, compaction must therefore fall
 * back to an infinite TTL (delete nothing by TTL); real TTL deletion resumes after the lease
 * recovers and the cache resyncs.
 */
public class MultiTsFileDeviceIteratorLeaseTest extends AbstractCompactionTest {

  private final String oldThreadName = Thread.currentThread().getName();

  @Before
  public void setUp()
      throws IOException, WriteProcessException, MetadataException, InterruptedException {
    super.setUp();
    Thread.currentThread().setName("pool-1-IoTDB-Compaction-Worker-1");
  }

  @After
  public void tearDown() throws IOException, StorageEngineException {
    DataNodeTTLCache.getInstance().clearAllTTLForTree();
    // Restore the process-wide lease singleton so other tests in this JVM are unaffected.
    MetadataLeaseManager.getInstance().recordConfigNodeHeartbeat();
    super.tearDown();
    for (final TsFileResource tsFileResource : seqResources) {
      FileReaderManager.getInstance().closeFileAndRemoveReader(tsFileResource.getTsFileID());
    }
    Thread.currentThread().setName(oldThreadName);
  }

  @Test
  public void activeLeaseAppliesRealTtlInCompaction()
      throws MetadataException, IOException, WriteProcessException {
    registerTimeseriesInMManger(1, 1, false);
    createFiles(1, 1, 1, 100, 0, 0, 50, 50, false, true);
    DataNodeTTLCache.getInstance().setTTLForTree(COMPACTION_TEST_SG + ".**", 100_000L);

    MetadataLeaseManager.getInstance().recordConfigNodeHeartbeat();
    try (final MultiTsFileDeviceIterator it = new MultiTsFileDeviceIterator(seqResources)) {
      Assert.assertTrue(it.hasNextDevice());
      it.nextDevice();
      Assert.assertNotEquals(Long.MAX_VALUE, it.getTTLForCurrentDevice());
      Assert.assertNotEquals(Long.MIN_VALUE, it.getTimeLowerBoundForCurrentDevice());
    }
  }

  @Test
  public void fencedLeaseUsesInfiniteTtlInCompaction()
      throws MetadataException, IOException, WriteProcessException, IllegalPathException {
    registerTimeseriesInMManger(1, 1, false);
    createFiles(1, 1, 1, 100, 0, 0, 50, 50, false, true);
    // A finite TTL is configured, so without the fence fallback compaction would delete by it.
    DataNodeTTLCache.getInstance().setTTLForTree(COMPACTION_TEST_SG + ".**", 100_000L);

    MetadataLeaseManager.getInstance().expireLeaseForTest();
    try (final MultiTsFileDeviceIterator it = new MultiTsFileDeviceIterator(seqResources)) {
      Assert.assertTrue(it.hasNextDevice());
      it.nextDevice();
      Assert.assertEquals(
          "a fenced DataNode must use an infinite TTL in compaction so a stale TTL cannot delete data",
          Long.MAX_VALUE,
          it.getTTLForCurrentDevice());
      Assert.assertEquals(Long.MIN_VALUE, it.getTimeLowerBoundForCurrentDevice());
    }
  }
}
