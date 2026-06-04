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

package org.apache.iotdb.db.metadata.cache;

import org.apache.iotdb.commons.exception.IllegalPathException;
import org.apache.iotdb.commons.path.MeasurementPath;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.db.queryengine.common.schematree.ClusterSchemaTree;
import org.apache.iotdb.db.queryengine.plan.relational.metadata.fetcher.cache.TreeDeviceSchemaCacheManager;
import org.apache.iotdb.db.schemaengine.lease.MetadataLeaseManager;

import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.write.schema.MeasurementSchema;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.Collections;

/**
 * While the metadata lease is fenced, the tree-model schema cache must not be trusted: this
 * DataNode could have missed a ConfigNode cache-invalidation (e.g. a DELETE TIMESERIES / datatype
 * change) while partitioned, so a stale cached entry could validate a write or resolve a query
 * against schema that no longer exists. Because the cache is read-through, the fix is to report a
 * cache <em>miss</em> while fenced so the caller re-fetches from the authoritative, quorum-backed
 * SchemaRegion (more available than hard-failing: the op still succeeds whenever that quorum is
 * reachable).
 */
public class TreeDeviceSchemaCacheManagerLeaseTest {

  private TreeDeviceSchemaCacheManager manager;

  @Before
  public void setUp() throws IllegalPathException {
    manager = TreeDeviceSchemaCacheManager.getInstance();
    manager.cleanUp();
    final ClusterSchemaTree tree = new ClusterSchemaTree();
    tree.appendSingleMeasurement(
        new PartialPath("root.sg1.d1.s1"),
        new MeasurementSchema("s1", TSDataType.INT32),
        null,
        null,
        null,
        false);
    tree.setDatabases(Collections.singleton("root.sg1"));
    manager.put(tree);
  }

  @After
  public void tearDown() {
    manager.cleanUp();
    // Restore the process-wide lease singleton so other tests in this JVM are unaffected.
    MetadataLeaseManager.getInstance().recordConfigNodeHeartbeat();
  }

  @Test
  public void fencedLeaseForcesTreeSchemaCacheMiss() throws IllegalPathException {
    final PartialPath device1 = new PartialPath("root.sg1.d1");
    final String[] measurements = new String[] {"s1"};

    // Sanity: with an active lease the cached entry is served (a cache hit).
    MetadataLeaseManager.getInstance().recordConfigNodeHeartbeat();
    Assert.assertFalse(
        "an active lease should serve the cached tree schema",
        manager.get(device1, measurements).getAllDevices().isEmpty());
    Assert.assertFalse(
        manager
            .getMatchedNormalSchema(new MeasurementPath("root.sg1.d1.s1"))
            .getAllDevices()
            .isEmpty());

    // Fenced: every tree-schema lookup must report a miss so the caller re-fetches from the
    // authoritative SchemaRegion instead of trusting a possibly-stale cached entry.
    MetadataLeaseManager.getInstance().expireLeaseForTest();
    Assert.assertTrue(
        "a fenced lease must report a tree-schema cache miss (force re-fetch)",
        manager.get(device1, measurements).getAllDevices().isEmpty());
    Assert.assertTrue(
        manager
            .getMatchedNormalSchema(new MeasurementPath("root.sg1.d1.s1"))
            .getAllDevices()
            .isEmpty());
  }
}
