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

package org.apache.iotdb.db.protocol.thrift.impl;

import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.service.ConsensusReadiness;
import org.apache.iotdb.mpp.rpc.thrift.TCreateSchemaRegionReq;
import org.apache.iotdb.rpc.TSStatusCode;

import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

public class ConsensusWaitTest {

  @BeforeClass
  public static void setUp() {
    IoTDBDescriptor.getInstance().getConfig().setDataNodeId(0);
  }

  @Test
  public void testRegionRequestBlocksUntilConsensusIsReady() throws Exception {
    TestConsensusReadiness readiness = new TestConsensusReadiness();
    DataNodeRegionManager regionManager = Mockito.mock(DataNodeRegionManager.class);
    DataNodeInternalRPCServiceImpl service =
        new DataNodeInternalRPCServiceImpl(readiness, 500, regionManager);
    TCreateSchemaRegionReq req = new TCreateSchemaRegionReq();
    TSStatus success = new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode());
    Mockito.when(regionManager.createSchemaRegion(null, null)).thenReturn(success);

    ExecutorService executor = Executors.newSingleThreadExecutor();
    try {
      Future<TSStatus> result = executor.submit(() -> service.createSchemaRegion(req));
      Assert.assertTrue(readiness.awaitEntered.await(1, TimeUnit.SECONDS));
      Assert.assertFalse(result.isDone());
      Mockito.verifyZeroInteractions(regionManager);

      readiness.markReady();

      Assert.assertSame(success, result.get(1, TimeUnit.SECONDS));
      Mockito.verify(regionManager).createSchemaRegion(null, null);
    } finally {
      executor.shutdownNow();
    }
  }

  @Test
  public void testTimeoutRejectsWithoutChangingRegionMetadata() {
    TestConsensusReadiness readiness = new TestConsensusReadiness();
    DataNodeRegionManager regionManager = Mockito.mock(DataNodeRegionManager.class);
    DataNodeInternalRPCServiceImpl service =
        new DataNodeInternalRPCServiceImpl(readiness, 20, regionManager);

    TSStatus status = service.createSchemaRegion(new TCreateSchemaRegionReq());

    assertConsensusNotInitialized(status);
    Assert.assertTrue(status.getMessage().contains("20"));
    Mockito.verifyZeroInteractions(regionManager);
  }

  @Test
  public void testAllRegionTopologyRequestsAreGuarded() throws Exception {
    ConsensusReadiness readiness =
        new ConsensusReadiness() {
          @Override
          public boolean isAllConsensusStarted() {
            return false;
          }

          @Override
          public boolean awaitAllConsensusStarted(long timeout, TimeUnit unit) {
            return false;
          }
        };
    DataNodeRegionManager regionManager = Mockito.mock(DataNodeRegionManager.class);
    DataNodeInternalRPCServiceImpl service =
        new DataNodeInternalRPCServiceImpl(readiness, 1, regionManager);

    assertConsensusNotInitialized(service.createSchemaRegion(null));
    assertConsensusNotInitialized(service.createDataRegion(null));
    assertConsensusNotInitialized(service.deleteRegion(null));
    assertConsensusNotInitialized(service.changeRegionLeader(null).getStatus());
    assertConsensusNotInitialized(service.createNewRegionPeer(null));
    assertConsensusNotInitialized(service.addRegionPeer(null));
    assertConsensusNotInitialized(service.removeRegionPeer(null));
    assertConsensusNotInitialized(service.deleteOldRegionPeer(null));
    assertConsensusNotInitialized(service.resetPeerList(null));
    assertConsensusNotInitialized(service.notifyRegionMigration(null));
    Mockito.verifyZeroInteractions(regionManager);
  }

  private static void assertConsensusNotInitialized(TSStatus status) {
    Assert.assertEquals(TSStatusCode.CONSENSUS_NOT_INITIALIZED.getStatusCode(), status.getCode());
    Assert.assertTrue(status.isSetMessage());
    Assert.assertFalse(status.getMessage().isEmpty());
  }

  private static class TestConsensusReadiness implements ConsensusReadiness {

    private final CountDownLatch ready = new CountDownLatch(1);
    private final CountDownLatch awaitEntered = new CountDownLatch(1);

    @Override
    public boolean isAllConsensusStarted() {
      return ready.getCount() == 0;
    }

    @Override
    public boolean awaitAllConsensusStarted(long timeout, TimeUnit unit)
        throws InterruptedException {
      awaitEntered.countDown();
      return ready.await(timeout, unit);
    }

    private void markReady() {
      ready.countDown();
    }
  }
}
