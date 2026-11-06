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

package org.apache.iotdb.db.pipe.agent.runtime;

import org.apache.iotdb.commons.consensus.SchemaRegionId;
import org.apache.iotdb.db.pipe.source.schemaregion.SchemaRegionListeningQueue;

import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;

public class PipeSchemaRegionListenerManagerTest {

  @Rule public final TemporaryFolder temporaryFolder = new TemporaryFolder();

  @Test
  public void testLeaderStateDoesNotCreateListener() {
    final PipeSchemaRegionListenerManager manager = new PipeSchemaRegionListenerManager();
    final SchemaRegionId schemaRegionId = new SchemaRegionId(1);

    manager.notifyLeaderReady(schemaRegionId);

    Assert.assertTrue(manager.isLeaderReady(schemaRegionId));
    Assert.assertTrue(manager.regionIds().isEmpty());
    Assert.assertNull(manager.listenerIfPresent(schemaRegionId));
  }

  @Test
  public void testCleanupUnusedClosedListener() {
    final PipeSchemaRegionListenerManager manager = new PipeSchemaRegionListenerManager();
    final SchemaRegionId schemaRegionId = new SchemaRegionId(2);

    final SchemaRegionListeningQueue listeningQueue = manager.listener(schemaRegionId);
    Assert.assertEquals(1, manager.increaseAndGetReferenceCount(schemaRegionId));

    listeningQueue.open();
    Assert.assertEquals(0, manager.decreaseAndGetReferenceCount(schemaRegionId));
    Assert.assertNotNull(manager.listenerIfPresent(schemaRegionId));

    listeningQueue.close();
    manager.cleanupListenerIfUnused(schemaRegionId);

    Assert.assertNull(manager.listenerIfPresent(schemaRegionId));
    Assert.assertTrue(manager.regionIds().isEmpty());
  }

  @Test
  public void testAutoCleanupAfterLastReferenceReleased() {
    final PipeSchemaRegionListenerManager manager = new PipeSchemaRegionListenerManager();
    final SchemaRegionId schemaRegionId = new SchemaRegionId(3);

    manager.increaseAndGetReferenceCount(schemaRegionId);

    Assert.assertEquals(0, manager.decreaseAndGetReferenceCount(schemaRegionId));
    Assert.assertNull(manager.listenerIfPresent(schemaRegionId));
    Assert.assertTrue(manager.regionIds().isEmpty());
  }

  @Test
  public void testCreateListenerForSnapshotLoadOnlyWhenSnapshotExists() throws IOException {
    final PipeSchemaRegionListenerManager manager = new PipeSchemaRegionListenerManager();
    final SchemaRegionId schemaRegionId = new SchemaRegionId(4);
    final File snapshotDir = temporaryFolder.newFolder();

    Assert.assertNull(manager.listenerForSnapshotLoad(schemaRegionId, snapshotDir));
    Assert.assertTrue(manager.regionIds().isEmpty());

    final SchemaRegionListeningQueue originalListener = manager.listener(schemaRegionId);
    originalListener.open();
    Assert.assertTrue(originalListener.createSnapshot(snapshotDir));
    originalListener.close();
    manager.clearSchemaRegionState(schemaRegionId);

    final SchemaRegionListeningQueue restoredListener =
        manager.listenerForSnapshotLoad(schemaRegionId, snapshotDir);
    Assert.assertNotNull(restoredListener);
    restoredListener.loadSnapshot(snapshotDir);
    Assert.assertTrue(restoredListener.isOpened());

    restoredListener.close();
    manager.clearSchemaRegionState(schemaRegionId);
  }
}
