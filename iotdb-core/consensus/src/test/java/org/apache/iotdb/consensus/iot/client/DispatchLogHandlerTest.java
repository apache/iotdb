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

package org.apache.iotdb.consensus.iot.client;

import org.apache.iotdb.common.rpc.thrift.TEndPoint;
import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.audit.UserDataTransferAuditEvent;
import org.apache.iotdb.commons.audit.UserDataTransferAuditHandler;
import org.apache.iotdb.commons.audit.UserDataTransferProtectionMethod;
import org.apache.iotdb.commons.consensus.DataRegionId;
import org.apache.iotdb.consensus.common.Peer;
import org.apache.iotdb.consensus.config.IoTConsensusConfig;
import org.apache.iotdb.consensus.iot.IoTConsensusServerImpl;
import org.apache.iotdb.consensus.iot.logdispatcher.Batch;
import org.apache.iotdb.consensus.iot.logdispatcher.IndexController;
import org.apache.iotdb.consensus.iot.logdispatcher.LogDispatcher.LogDispatcherThread;
import org.apache.iotdb.consensus.iot.logdispatcher.LogDispatcherThreadMetrics;
import org.apache.iotdb.consensus.iot.logdispatcher.SyncStatus;
import org.apache.iotdb.consensus.iot.thrift.TLogEntry;
import org.apache.iotdb.consensus.iot.thrift.TSyncLogEntriesRes;

import org.apache.thrift.TApplicationException;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.mockito.ArgumentCaptor;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.same;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class DispatchLogHandlerTest {

  private static final TEndPoint SOURCE = new TEndPoint("127.0.0.1", 10740);
  private static final TEndPoint TARGET = new TEndPoint("127.0.0.2", 10740);

  @Test
  public void testRecordsSuccessRemoteFailureAndException() {
    final List<UserDataTransferAuditEvent> events = new ArrayList<>();
    final Batch batch = createBatch(true);

    DispatchLogHandler.recordTransferAttempt(
        events::add, batch, SOURCE, TARGET, UserDataTransferProtectionMethod.TLS, true, null, null);
    DispatchLogHandler.recordTransferAttempt(
        events::add,
        batch,
        SOURCE,
        TARGET,
        UserDataTransferProtectionMethod.TLS,
        false,
        "500",
        null);
    DispatchLogHandler.recordTransferAttempt(
        events::add,
        batch,
        SOURCE,
        TARGET,
        UserDataTransferProtectionMethod.TLS,
        false,
        null,
        new IOException());

    assertEquals(3, events.size());
    assertTrue(events.get(0).isSuccess());
    assertEquals(SOURCE, events.get(0).getInitiator());
    assertEquals(SOURCE, events.get(0).getSource());
    assertEquals(TARGET, events.get(0).getTarget());
    assertEquals(UserDataTransferProtectionMethod.TLS, events.get(0).getProtectionMethod());
    assertFalse(events.get(1).isSuccess());
    assertEquals("500", events.get(1).getError());
    assertEquals(IOException.class.getName(), events.get(2).getError());
  }

  @Test
  public void testSkipsBatchWithoutUserData() {
    final List<UserDataTransferAuditEvent> events = new ArrayList<>();

    DispatchLogHandler.recordTransferAttempt(
        events::add,
        createBatch(false),
        SOURCE,
        TARGET,
        UserDataTransferProtectionMethod.NONE,
        true,
        null,
        null);

    assertTrue(events.isEmpty());
  }

  @Test
  public void testSkipDoesNotInspectResponseStatuses() {
    final AtomicInteger statusAccessCount = new AtomicInteger();
    final TSyncLogEntriesRes response =
        new TSyncLogEntriesRes() {
          @Override
          public List<TSStatus> getStatuses() {
            statusAccessCount.incrementAndGet();
            return super.getStatuses();
          }
        };

    DispatchLogHandler.recordTransferAttempt(
        event -> {},
        createBatch(false),
        SOURCE,
        TARGET,
        UserDataTransferProtectionMethod.NONE,
        response);
    DispatchLogHandler.recordTransferAttempt(
        UserDataTransferAuditHandler.NO_OP,
        createBatch(true),
        SOURCE,
        TARGET,
        UserDataTransferProtectionMethod.NONE,
        response);

    assertEquals(0, statusAccessCount.get());
  }

  @Test
  public void testAuditHandlerFailureDoesNotEscape() {
    DispatchLogHandler.recordTransferAttempt(
        event -> {
          throw new IllegalStateException();
        },
        createBatch(true),
        SOURCE,
        TARGET,
        UserDataTransferProtectionMethod.NONE,
        true,
        null,
        null);

    DispatchLogHandler.recordTransferAttempt(
        new UserDataTransferAuditHandler() {
          @Override
          public void onAttempt(UserDataTransferAuditEvent event) {
            // Do nothing.
          }

          @Override
          public boolean isEnabled() {
            throw new IllegalStateException();
          }
        },
        createBatch(true),
        SOURCE,
        TARGET,
        UserDataTransferProtectionMethod.NONE,
        true,
        null,
        null);
  }

  @Rule public TemporaryFolder directory = new TemporaryFolder();

  @Test
  public void testCompressedProtocolFailureRetriesWithoutAdvancingIndex() throws Exception {
    verifyCompressionFailure(true);
  }

  @Test
  public void testExplicitDecodeErrorOverridesNonRetryStatus() throws Exception {
    verifyCompressionFailure(false);
  }

  private void verifyCompressionFailure(boolean protocolError) throws Exception {
    IoTConsensusConfig config = IoTConsensusConfig.newBuilder().build();
    Peer peer = new Peer(new DataRegionId(1), 2, TARGET);
    IndexController controller =
        new IndexController(directory.newFolder().getAbsolutePath(), peer, 0, 500);
    SyncStatus status = new SyncStatus(controller, config);
    Batch batch = createBatch(false);
    LogDispatcherThread thread = mock(LogDispatcherThread.class);
    IoTConsensusServerImpl impl = mock(IoTConsensusServerImpl.class);
    ScheduledExecutorService scheduler = mock(ScheduledExecutorService.class);
    when(thread.getConfig()).thenReturn(config);
    when(thread.getPeer()).thenReturn(peer);
    when(thread.getSyncStatus()).thenReturn(status);
    when(thread.getImpl()).thenReturn(impl);
    when(impl.getBackgroundTaskService()).thenReturn(scheduler);
    when(impl.getUserDataTransferAuditHandler()).thenReturn(UserDataTransferAuditHandler.NO_OP);
    status.addNextBatch(batch);
    try {
      DispatchLogHandler handler =
          new DispatchLogHandler(thread, mock(LogDispatcherThreadMetrics.class), batch);
      if (protocolError) {
        handler.markCompressedAttempt();
        handler.onError(new TApplicationException(TApplicationException.PROTOCOL_ERROR));
      } else {
        TSStatus error = new TSStatus(200).setMessage("decode failed");
        handler.onComplete(
            new TSyncLogEntriesRes(Collections.singletonList(error)).setLogCompressionError(error));
      }
      assertEquals(0, controller.getCurrentIndex());
      assertFalse(batch.isSynced());
      assertTrue(status.hasPendingBatches());
      // A later in-flight batch may ACK first. Its success must not advance past the rejected
      // batch or release either raw batch before the missing prefix has succeeded.
      Batch later = new Batch(config);
      later.addTLogEntry(new TLogEntry().setSearchIndex(2).setMemorySize(1));
      later.buildIndex();
      status.addNextBatch(later);
      DispatchLogHandler laterHandler =
          new DispatchLogHandler(thread, mock(LogDispatcherThreadMetrics.class), later);
      laterHandler.onComplete(new TSyncLogEntriesRes(Collections.singletonList(new TSStatus(200))));
      assertTrue(later.isSynced());
      assertEquals(0, controller.getCurrentIndex());
      assertEquals(2, status.getPendingBatches().size());
      ArgumentCaptor<Runnable> retry = ArgumentCaptor.forClass(Runnable.class);
      verify(scheduler).schedule(retry.capture(), anyLong(), eq(TimeUnit.MILLISECONDS));
      retry.getValue().run();
      verify(thread).sendBatchAsync(same(batch), same(handler));
      handler.onComplete(new TSyncLogEntriesRes(Collections.singletonList(new TSStatus(200))));
      assertEquals(2, controller.getCurrentIndex());
      assertFalse(status.hasPendingBatches());
      verify(thread, times(2)).updateSafelyDeletedSearchIndex();
    } finally {
      status.free();
    }
  }

  private static Batch createBatch(boolean containsUserData) {
    final Batch batch = new Batch(IoTConsensusConfig.newBuilder().build());
    batch.addTLogEntry(new TLogEntry().setSearchIndex(1).setMemorySize(1), containsUserData);
    batch.buildIndex();
    return batch;
  }
}
