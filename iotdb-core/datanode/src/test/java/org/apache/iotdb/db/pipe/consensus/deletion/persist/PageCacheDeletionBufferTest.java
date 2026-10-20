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

package org.apache.iotdb.db.pipe.consensus.deletion.persist;

import org.apache.iotdb.commons.consensus.index.impl.RecoverProgressIndex;
import org.apache.iotdb.commons.consensus.index.impl.SimpleProgressIndex;
import org.apache.iotdb.commons.path.MeasurementPath;
import org.apache.iotdb.commons.queryengine.plan.planner.plan.node.PlanNodeId;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.pipe.consensus.deletion.DeletionResource;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.write.DeleteDataNode;

import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

public class PageCacheDeletionBufferTest {

  @Rule public final TemporaryFolder temporaryFolder = new TemporaryFolder();

  @Test
  public void testDequeuedDeletionIsNotReportedAsFlushed() throws Exception {
    final CountDownLatch serializationStarted = new CountDownLatch(1);
    final CountDownLatch continueSerialization = new CountDownLatch(1);
    final int dataRegionId = 1;
    final DeleteDataNode deleteDataNode =
        new DeleteDataNode(
            new PlanNodeId("1"),
            Collections.singletonList(new MeasurementPath("root.vehicle.d2.s0")),
            50,
            150);
    deleteDataNode.setProgressIndex(
        new RecoverProgressIndex(
            IoTDBDescriptor.getInstance().getConfig().getDataNodeId(),
            new SimpleProgressIndex(0, 1)));
    final DeletionResource deletionResource =
        new DeletionResource(deleteDataNode, ignored -> {}, dataRegionId) {
          @Override
          public ByteBuffer serialize() {
            serializationStarted.countDown();
            try {
              continueSerialization.await();
            } catch (InterruptedException e) {
              Thread.currentThread().interrupt();
            }
            return super.serialize();
          }
        };
    final PageCacheDeletionBuffer deletionBuffer =
        new PageCacheDeletionBuffer(
            dataRegionId, temporaryFolder.newFolder("deletions").getAbsolutePath());

    deletionBuffer.start();
    try {
      deletionBuffer.registerDeletionResource(deletionResource);
      Assert.assertTrue(serializationStarted.await(10, TimeUnit.SECONDS));

      Assert.assertFalse(deletionBuffer.isAllDeletionFlushed());
    } finally {
      continueSerialization.countDown();
      deletionBuffer.close();
    }
    Assert.assertSame(DeletionResource.Status.SUCCESS, deletionResource.waitForResult());
  }
}
