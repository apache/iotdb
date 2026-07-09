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

package org.apache.iotdb.db.queryengine.plan.relational.metadata.fetcher.cache;

import org.apache.iotdb.commons.exception.IllegalPathException;
import org.apache.iotdb.commons.exception.MetadataLeaseFencedException;
import org.apache.iotdb.commons.path.MeasurementPath;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.db.queryengine.common.schematree.ClusterSchemaTree;
import org.apache.iotdb.db.queryengine.plan.analyze.schema.ISchemaComputation;
import org.apache.iotdb.db.schemaengine.lease.MetadataLeaseManager;
import org.apache.iotdb.db.schemaengine.lease.MetadataLeaseTestUtils;
import org.apache.iotdb.rpc.TSStatusCode;

import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.write.schema.MeasurementSchema;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.Collections;
import java.util.concurrent.atomic.AtomicLong;

import static org.apache.iotdb.db.schemaengine.lease.MetadataLeaseTestUtils.T_FENCE_MS;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;

public class TreeDeviceSchemaCacheManagerLeaseTest {

  private final AtomicLong nowNanos = new AtomicLong(100_000_000_000L);

  private MetadataLeaseManager leaseManager;
  private TreeDeviceSchemaCacheManager manager;

  @Before
  public void setUp() throws IllegalPathException {
    nowNanos.set(100_000_000_000L);
    leaseManager = MetadataLeaseTestUtils.newManager(nowNanos);
    manager = new TestingTreeDeviceSchemaCacheManager(leaseManager);
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
  }

  @Test
  public void fencedLeaseFailsClosedForTreeSchemaCache() throws IllegalPathException {
    final PartialPath device1 = new PartialPath("root.sg1.d1");
    final String[] measurements = new String[] {"s1"};

    nowNanos.addAndGet((T_FENCE_MS + 1) * 1_000_000L);
    assertLeaseFenced(() -> manager.get(device1, measurements));
    assertLeaseFenced(
        () -> {
          try {
            manager.getMatchedNormalSchema(new MeasurementPath("root.sg1.d1.s1"));
          } catch (IllegalPathException e) {
            throw new RuntimeException(e);
          }
        });
    assertLeaseFenced(() -> manager.getMatchedTemplateSchema(device1));
    assertLeaseFenced(() -> manager.computeWithoutTemplate(Mockito.mock(ISchemaComputation.class)));
    assertLeaseFenced(() -> manager.computeWithTemplate(Mockito.mock(ISchemaComputation.class)));
    final ISchemaComputation logicalViewComputation = Mockito.mock(ISchemaComputation.class);
    Mockito.when(logicalViewComputation.hasLogicalViewNeedProcess()).thenReturn(true);
    assertLeaseFenced(() -> manager.computeSourceOfLogicalView(logicalViewComputation));
  }

  private static void assertLeaseFenced(final Runnable runnable) {
    final MetadataLeaseFencedException e =
        assertThrows(MetadataLeaseFencedException.class, runnable::run);
    assertEquals(TSStatusCode.METADATA_LEASE_FENCED.getStatusCode(), e.getErrorCode());
  }

  private static class TestingTreeDeviceSchemaCacheManager extends TreeDeviceSchemaCacheManager {

    private final MetadataLeaseManager leaseManager;

    private TestingTreeDeviceSchemaCacheManager(final MetadataLeaseManager leaseManager) {
      this.leaseManager = leaseManager;
    }

    @Override
    void failIfMetadataLeaseFenced() {
      MetadataLeaseTestUtils.failIfMetadataLeaseFenced(leaseManager);
    }
  }
}
