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

package org.apache.iotdb.db.queryengine.plan.analyze.cache;

import org.apache.iotdb.common.rpc.thrift.TConsensusGroupId;
import org.apache.iotdb.common.rpc.thrift.TConsensusGroupType;
import org.apache.iotdb.common.rpc.thrift.TTimePartitionSlot;
import org.apache.iotdb.commons.exception.MetadataLeaseFencedException;
import org.apache.iotdb.commons.partition.DataPartitionQueryParam;
import org.apache.iotdb.db.auth.AuthorityChecker;
import org.apache.iotdb.db.queryengine.plan.analyze.cache.partition.PartitionCache;
import org.apache.iotdb.db.schemaengine.lease.MetadataLeaseManager;
import org.apache.iotdb.db.schemaengine.lease.MetadataLeaseTestUtils;
import org.apache.iotdb.rpc.TSStatusCode;

import org.apache.tsfile.file.metadata.IDeviceID;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

import static org.apache.iotdb.db.schemaengine.lease.MetadataLeaseTestUtils.T_FENCE_MS;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;

public class PartitionCacheLeaseTest {

  private final AtomicLong nowNanos = new AtomicLong(100_000_000_000L);

  private MetadataLeaseManager leaseManager;
  private PartitionCache partitionCache;
  private IDeviceID deviceID;
  private TConsensusGroupId consensusGroupId;

  @Before
  public void setUp() {
    nowNanos.set(100_000_000_000L);
    leaseManager = MetadataLeaseTestUtils.newManager(nowNanos);
    partitionCache = new TestingPartitionCache(leaseManager);
    deviceID = IDeviceID.Factory.DEFAULT_FACTORY.create("root.sg.d1");
    consensusGroupId = new TConsensusGroupId(TConsensusGroupType.DataRegion, 1);
  }

  @After
  public void tearDown() {
    partitionCache.invalidAllCache();
  }

  @Test
  public void fencedLeaseFailsClosedForPartitionCache() {
    nowNanos.addAndGet((T_FENCE_MS + 1) * 1_000_000L);

    assertLeaseFenced(
        () ->
            partitionCache.getDatabaseToDevice(
                Collections.singletonList(deviceID), false, false, AuthorityChecker.SUPER_USER));
    assertLeaseFenced(
        () ->
            partitionCache.getDeviceToDatabase(
                Collections.singletonList(deviceID), false, false, AuthorityChecker.SUPER_USER));
    assertLeaseFenced(
        () ->
            partitionCache.checkAndAutoCreateDatabase(
                "root.sg", false, AuthorityChecker.SUPER_USER));
    assertLeaseFenced(
        () -> partitionCache.getRegionReplicaSet(Collections.singletonList(consensusGroupId)));
    assertLeaseFenced(() -> partitionCache.getSchemaPartition(databaseToDeviceMap()));
    assertLeaseFenced(() -> partitionCache.getSchemaPartition("root.sg"));
    assertLeaseFenced(() -> partitionCache.getDataPartition(dataQueryMap()));
  }

  private static void assertLeaseFenced(final Runnable runnable) {
    final MetadataLeaseFencedException e =
        assertThrows(MetadataLeaseFencedException.class, runnable::run);
    assertEquals(TSStatusCode.METADATA_LEASE_FENCED.getStatusCode(), e.getErrorCode());
  }

  private Map<String, List<IDeviceID>> databaseToDeviceMap() {
    final Map<String, List<IDeviceID>> map = new HashMap<>();
    map.put("root.sg", Collections.singletonList(deviceID));
    return map;
  }

  private Map<String, List<DataPartitionQueryParam>> dataQueryMap() {
    final DataPartitionQueryParam param = new DataPartitionQueryParam();
    param.setDeviceID(deviceID);
    param.setTimePartitionSlotList(Collections.singletonList(new TTimePartitionSlot(0)));

    final Map<String, List<DataPartitionQueryParam>> map = new HashMap<>();
    map.put("root.sg", Collections.singletonList(param));
    return map;
  }

  private static class TestingPartitionCache extends PartitionCache {

    private final MetadataLeaseManager leaseManager;

    private TestingPartitionCache(final MetadataLeaseManager leaseManager) {
      this.leaseManager = leaseManager;
    }

    @Override
    protected void failIfMetadataLeaseFenced() {
      MetadataLeaseTestUtils.failIfMetadataLeaseFenced(leaseManager);
    }
  }
}
