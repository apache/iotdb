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

package org.apache.iotdb.db.pipe.agent.task.subtask.sink;

import org.apache.iotdb.commons.conf.CommonDescriptor;
import org.apache.iotdb.commons.pipe.agent.task.connection.UnboundedBlockingPendingQueue;
import org.apache.iotdb.commons.pipe.agent.task.progress.CommitterKey;
import org.apache.iotdb.commons.pipe.sink.protocol.PipeConnectorWithEventDiscard;
import org.apache.iotdb.pipe.api.PipeConnector;
import org.apache.iotdb.pipe.api.customizer.configuration.PipeConnectorRuntimeConfiguration;
import org.apache.iotdb.pipe.api.customizer.parameter.PipeParameterValidator;
import org.apache.iotdb.pipe.api.customizer.parameter.PipeParameters;
import org.apache.iotdb.pipe.api.event.Event;
import org.apache.iotdb.pipe.api.event.dml.insertion.TabletInsertionEvent;
import org.apache.iotdb.pipe.api.exception.PipeConnectionException;
import org.apache.iotdb.pipe.api.exception.PipeException;

import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.withSettings;

public class PipeSinkSubtaskTest {

  @Test
  public void testDiscardEventsOfPipeDelegatesToConnector() {
    final PipeConnector connector =
        mock(
            PipeConnector.class,
            withSettings().extraInterfaces(PipeConnectorWithEventDiscard.class));
    final UnboundedBlockingPendingQueue<?> pendingQueue = mock(UnboundedBlockingPendingQueue.class);

    final PipeSinkSubtask subtask =
        Mockito.spy(
            new PipeSinkSubtask(
                "PipeSinkSubtaskTest",
                System.currentTimeMillis(),
                "data_test",
                "data_test",
                0,
                (UnboundedBlockingPendingQueue) pendingQueue,
                connector));

    try {
      final CommitterKey committerKey = new CommitterKey("pipe", 1L, 1, -1);
      subtask.discardEventsOfPipe(committerKey);

      verify((PipeConnectorWithEventDiscard) connector).discardEventsOfPipe(committerKey);
    } finally {
      subtask.close();
    }
  }

  @Test
  public void testDiscardEventsOfPipeNotBlockedByConnectionRetry() throws Exception {
    final CountDownLatch handshakeEntered = new CountDownLatch(1);
    final CountDownLatch releaseHandshake = new CountDownLatch(1);
    final CountDownLatch discardEntered = new CountDownLatch(1);
    final AtomicBoolean discardDuringHandshake = new AtomicBoolean(false);
    final PipeConnector connector =
        new BlockingHandshakeConnector(
            handshakeEntered,
            releaseHandshake,
            new CountDownLatch(0),
            new AtomicBoolean(false),
            discardEntered,
            discardDuringHandshake);
    final UnboundedBlockingPendingQueue<?> pendingQueue = mock(UnboundedBlockingPendingQueue.class);

    final PipeSinkSubtask subtask =
        new PipeSinkSubtask(
            "PipeSinkSubtaskTest",
            System.currentTimeMillis(),
            "data_test",
            "data_test",
            0,
            (UnboundedBlockingPendingQueue) pendingQueue,
            connector);

    final Thread failureThread =
        new Thread(() -> subtask.onFailure(new PipeConnectionException("connection broken")));
    failureThread.start();
    Assert.assertTrue(handshakeEntered.await(5, TimeUnit.SECONDS));

    final CountDownLatch discardReturned = new CountDownLatch(1);
    final Thread discardThread =
        new Thread(
            () -> {
              try {
                subtask.discardEventsOfPipe(new CommitterKey("pipe", 1L, 1, -1));
              } finally {
                discardReturned.countDown();
              }
            });
    discardThread.start();

    try {
      Assert.assertTrue(discardReturned.await(5, TimeUnit.SECONDS));
      Assert.assertEquals(1L, discardEntered.getCount());
      Assert.assertFalse(discardDuringHandshake.get());
    } finally {
      releaseHandshake.countDown();
      failureThread.join(5000);
      Assert.assertTrue(discardEntered.await(1, TimeUnit.SECONDS));
      Assert.assertFalse(discardDuringHandshake.get());
      subtask.close();
    }
  }

  @Test
  public void testCloseNotConcurrentWithConnectionRetry() throws Exception {
    final int originalTimeout =
        CommonDescriptor.getInstance().getConfig().getDnConnectionTimeoutInMS();
    CommonDescriptor.getInstance().getConfig().setDnConnectionTimeoutInMS(30);

    final CountDownLatch handshakeEntered = new CountDownLatch(1);
    final CountDownLatch releaseHandshake = new CountDownLatch(1);
    final CountDownLatch closeEntered = new CountDownLatch(1);
    final AtomicBoolean closeDuringHandshake = new AtomicBoolean(false);
    final PipeConnector connector =
        new BlockingHandshakeConnector(
            handshakeEntered,
            releaseHandshake,
            closeEntered,
            closeDuringHandshake,
            new CountDownLatch(0),
            new AtomicBoolean(false));
    final UnboundedBlockingPendingQueue<?> pendingQueue = mock(UnboundedBlockingPendingQueue.class);

    final PipeSinkSubtask subtask =
        new PipeSinkSubtask(
            "PipeSinkSubtaskTest",
            System.currentTimeMillis(),
            "data_test",
            "data_test",
            0,
            (UnboundedBlockingPendingQueue) pendingQueue,
            connector);

    final Thread failureThread =
        new Thread(() -> subtask.onFailure(new PipeConnectionException("connection broken")));
    failureThread.start();

    try {
      Assert.assertTrue(handshakeEntered.await(5, TimeUnit.SECONDS));

      final long startTime = System.currentTimeMillis();
      subtask.close();

      Assert.assertTrue(System.currentTimeMillis() - startTime < 1000);
      Assert.assertFalse(closeEntered.await(100, TimeUnit.MILLISECONDS));
      Assert.assertFalse(closeDuringHandshake.get());
    } finally {
      releaseHandshake.countDown();
      Assert.assertTrue(closeEntered.await(1, TimeUnit.SECONDS));
      failureThread.join(5000);
      Assert.assertFalse(closeDuringHandshake.get());
      CommonDescriptor.getInstance().getConfig().setDnConnectionTimeoutInMS(originalTimeout);
    }
  }

  @Test
  public void testCloseDoesNotWaitForeverForConnectorClose() throws Exception {
    final int originalTimeout =
        CommonDescriptor.getInstance().getConfig().getDnConnectionTimeoutInMS();
    CommonDescriptor.getInstance().getConfig().setDnConnectionTimeoutInMS(30);

    final PipeConnector connector = mock(PipeConnector.class);
    final UnboundedBlockingPendingQueue<?> pendingQueue = mock(UnboundedBlockingPendingQueue.class);
    final CountDownLatch closeEntered = new CountDownLatch(1);
    final CountDownLatch releaseClose = new CountDownLatch(1);

    doAnswer(
            invocation -> {
              closeEntered.countDown();
              releaseClose.await(2, TimeUnit.SECONDS);
              return null;
            })
        .when(connector)
        .close();

    final PipeSinkSubtask subtask =
        new PipeSinkSubtask(
            "PipeSinkSubtaskTest",
            System.currentTimeMillis(),
            "data_test",
            "data_test",
            0,
            (UnboundedBlockingPendingQueue) pendingQueue,
            connector);

    try {
      final long startTime = System.currentTimeMillis();
      subtask.close();

      Assert.assertTrue(closeEntered.await(1, TimeUnit.SECONDS));
      Assert.assertTrue(System.currentTimeMillis() - startTime < 1000);
    } finally {
      releaseClose.countDown();
      CommonDescriptor.getInstance().getConfig().setDnConnectionTimeoutInMS(originalTimeout);
    }
  }

  @Test
  public void testTransferExceptionUsesDisplayTaskID() throws Exception {
    final PipeConnector connector = mock(PipeConnector.class);
    final UnboundedBlockingPendingQueue<Event> pendingQueue =
        mock(UnboundedBlockingPendingQueue.class);
    final Event event = mock(Event.class);

    when(pendingQueue.waitedPoll()).thenReturn(event);
    doThrow(new RuntimeException("No more authentication methods available"))
        .when(connector)
        .transfer(any(Event.class));

    final PipeSinkSubtask subtask =
        new PipeSinkSubtask(
            "data_{sink=TSFILE_REMOTE_SINK, sink.scp.password=Iotdb@2026}_1701687309493_0",
            1701687309493L,
            "data_{sink=TSFILE_REMOTE_SINK, sink.scp.password=Iotdb@2026}",
            "data_{sink=TSFILE_REMOTE_SINK, sink.scp.host=172.20.70.119}",
            0,
            pendingQueue,
            connector);

    try {
      subtask.executeOnce();
      Assert.fail();
    } catch (final PipeException e) {
      Assert.assertTrue(e.getMessage().contains("Exception in pipe transfer, subtask: data_{"));
      Assert.assertTrue(e.getMessage().contains("sink=TSFILE_REMOTE_SINK"));
      Assert.assertTrue(e.getMessage().contains("sink.scp.host=172.20.70.119"));
      Assert.assertTrue(e.getMessage().contains("No more authentication methods available"));
      Assert.assertFalse(e.getMessage().contains("sink.scp.password"));
      Assert.assertFalse(e.getMessage().contains("Iotdb@2026"));
    } finally {
      subtask.close();
    }
  }

  private static class BlockingHandshakeConnector
      implements PipeConnector, PipeConnectorWithEventDiscard {

    private final CountDownLatch handshakeEntered;
    private final CountDownLatch releaseHandshake;
    private final CountDownLatch closeEntered;
    private final AtomicBoolean closeDuringHandshake;
    private final CountDownLatch discardEntered;
    private final AtomicBoolean discardDuringHandshake;
    private volatile boolean handshaking;

    private BlockingHandshakeConnector(
        final CountDownLatch handshakeEntered, final CountDownLatch releaseHandshake) {
      this(
          handshakeEntered,
          releaseHandshake,
          new CountDownLatch(0),
          new AtomicBoolean(false),
          new CountDownLatch(0),
          new AtomicBoolean(false));
    }

    private BlockingHandshakeConnector(
        final CountDownLatch handshakeEntered,
        final CountDownLatch releaseHandshake,
        final CountDownLatch closeEntered,
        final AtomicBoolean closeDuringHandshake,
        final CountDownLatch discardEntered,
        final AtomicBoolean discardDuringHandshake) {
      this.handshakeEntered = handshakeEntered;
      this.releaseHandshake = releaseHandshake;
      this.closeEntered = closeEntered;
      this.closeDuringHandshake = closeDuringHandshake;
      this.discardEntered = discardEntered;
      this.discardDuringHandshake = discardDuringHandshake;
    }

    @Override
    public void validate(final PipeParameterValidator validator) throws Exception {
      // No-op
    }

    @Override
    public void customize(
        final PipeParameters parameters, final PipeConnectorRuntimeConfiguration configuration)
        throws Exception {
      // No-op
    }

    @Override
    public void handshake() throws Exception {
      handshaking = true;
      handshakeEntered.countDown();
      try {
        releaseHandshake.await(5, TimeUnit.SECONDS);
      } finally {
        handshaking = false;
      }
    }

    @Override
    public void heartbeat() throws Exception {
      // No-op
    }

    @Override
    public void transfer(final TabletInsertionEvent tabletInsertionEvent) throws Exception {
      // No-op
    }

    @Override
    public void transfer(final Event event) throws Exception {
      // No-op
    }

    @Override
    public void close() throws Exception {
      if (handshaking) {
        closeDuringHandshake.set(true);
      }
      closeEntered.countDown();
    }

    @Override
    public void discardEventsOfPipe(
        final String pipeName, final long creationTime, final int regionId) {
      if (handshaking) {
        discardDuringHandshake.set(true);
      }
      discardEntered.countDown();
    }
  }
}
