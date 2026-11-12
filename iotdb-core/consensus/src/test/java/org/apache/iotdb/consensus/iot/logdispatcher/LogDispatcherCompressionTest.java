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

package org.apache.iotdb.consensus.iot.logdispatcher;

import org.apache.iotdb.common.rpc.thrift.TEndPoint;
import org.apache.iotdb.commons.client.IClientManager;
import org.apache.iotdb.commons.client.exception.ClientManagerException;
import org.apache.iotdb.commons.consensus.DataRegionId;
import org.apache.iotdb.commons.memory.AtomicLongMemoryBlock;
import org.apache.iotdb.commons.memory.IMemoryBlock;
import org.apache.iotdb.consensus.IStateMachine;
import org.apache.iotdb.consensus.common.Peer;
import org.apache.iotdb.consensus.config.IoTConsensusConfig;
import org.apache.iotdb.consensus.iot.IoTConsensusServerImpl;
import org.apache.iotdb.consensus.iot.client.AsyncIoTConsensusServiceClient;
import org.apache.iotdb.consensus.iot.client.DispatchLogHandler;
import org.apache.iotdb.consensus.iot.thrift.TLogCompressionType;
import org.apache.iotdb.consensus.iot.thrift.TLogEntry;
import org.apache.iotdb.consensus.iot.thrift.TSyncLogEntriesReq;
import org.apache.iotdb.consensus.iot.thrift.TSyncLogEntriesRes;
import org.apache.iotdb.consensus.iot.util.FakeConsensusReqReader;

import org.apache.thrift.TException;
import org.apache.thrift.async.AsyncMethodCallback;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnit;
import org.mockito.junit.MockitoRule;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collections;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class LogDispatcherCompressionTest {
  @Rule public TemporaryFolder directory = new TemporaryFolder();
  private final IoTConsensusMemoryManager memory = IoTConsensusMemoryManager.getInstance();
  private IMemoryBlock previous;
  private LogDispatcher dispatcher;
  private LogDispatcher.LogDispatcherThread thread;
  @Rule public MockitoRule mocks = MockitoJUnit.rule();
  @Mock private IClientManager<TEndPoint, AsyncIoTConsensusServiceClient> clients;
  @Captor private ArgumentCaptor<AsyncMethodCallback<TSyncLogEntriesRes>> callback;
  private AsyncIoTConsensusServiceClient client;
  private DispatchLogHandler handler;
  private Batch batch;
  private Peer target;

  @Before
  public void setUp() throws Exception {
    previous = memory.getMemoryBlock();
    memory.setMemoryBlock(
        new AtomicLongMemoryBlock("sender-compression-test", null, 64L * 1024 * 1024));
    memory.reset();
    IoTConsensusConfig config =
        IoTConsensusConfig.newBuilder()
            .setReplication(
                IoTConsensusConfig.Replication.newBuilder()
                    .setLogCompressionType(TLogCompressionType.LZ4)
                    .build())
            .build();
    IoTConsensusServerImpl impl = mock(IoTConsensusServerImpl.class);
    IStateMachine stateMachine = mock(IStateMachine.class);
    when(impl.getStateMachine()).thenReturn(stateMachine);
    when(stateMachine.read(any())).thenReturn(mock(FakeConsensusReqReader.class));
    when(impl.getStorageDir()).thenReturn(directory.newFolder().getAbsolutePath());
    Peer self = new Peer(new DataRegionId(1), 1, new TEndPoint("127.0.0.1", 9000));
    target = new Peer(new DataRegionId(1), 2, new TEndPoint("127.0.0.1", 9001));
    when(impl.getThisNode()).thenReturn(self);
    when(impl.getConfiguration()).thenReturn(Collections.singletonList(self));
    client = mock(AsyncIoTConsensusServiceClient.class);
    when(clients.borrowClient(target.getEndpoint()))
        .thenAnswer(
            call -> {
              // Encoding is complete before obtaining a client, including its temporary
              // reservation.
              assertTrue(memory.getMemoryBlock().getUsedMemoryInBytes() > 0);
              return client;
            });
    dispatcher = new LogDispatcher(impl, clients);
    thread = dispatcher.new LogDispatcherThread(target, config, 0);
    handler = mock(DispatchLogHandler.class);
    batch = new Batch(config);
    batch.addTLogEntry(
        new TLogEntry(Collections.singletonList(ByteBuffer.allocate(16384)), 1, false, 16384));
    batch.buildIndex();
  }

  @After
  public void tearDown() {
    Thread.currentThread().interrupt();
    thread.run();
    thread.stop();
    dispatcher.stop();
    assertEquals(0, memory.getMemoryBlock().getUsedMemoryInBytes());
    memory.reset();
    memory.setMemoryBlock(previous);
  }

  @Test
  public void testSuccessReleasesWireMemoryAndPreservesRawBatch() throws Exception {
    verifyAsyncCompletion(false);
  }

  @Test
  public void testAsyncFailureReleasesWireMemoryAndPreservesRawBatch() throws Exception {
    verifyAsyncCompletion(true);
  }

  private void verifyAsyncCompletion(boolean failure) throws Exception {
    thread.sendBatchAsync(batch, handler);
    ArgumentCaptor<TSyncLogEntriesReq> request = ArgumentCaptor.forClass(TSyncLogEntriesReq.class);
    verify(client).syncLogEntries(request.capture(), callback.capture());
    verify(handler).markCompressedAttempt();
    assertTrue(request.getValue().getLogEntries().get(0).isSetCompressionInfo());
    assertTrue(memory.getMemoryBlock().getUsedMemoryInBytes() > 0);
    if (failure) {
      IOException error = new IOException();
      callback.getValue().onError(error);
      verify(handler).onError(error);
    } else {
      TSyncLogEntriesRes response = new TSyncLogEntriesRes();
      callback.getValue().onComplete(response);
      verify(handler).onComplete(response);
    }
    assertEquals(0, memory.getMemoryBlock().getUsedMemoryInBytes());
    assertEquals(16384, batch.getLogEntries().get(0).getData().get(0).remaining());
    assertTrue(!batch.getLogEntries().get(0).isSetCompressionInfo());
  }

  @Test
  public void testBorrowFailureReleasesWireMemory() throws Exception {
    ClientManagerException error = new ClientManagerException(new IOException());
    doThrow(error).when(clients).borrowClient(target.getEndpoint());
    thread.sendBatchAsync(batch, handler);
    verify(handler).onError(error);
    assertEquals(0, memory.getMemoryBlock().getUsedMemoryInBytes());
  }

  @Test
  public void testSynchronousSendFailureReleasesWireMemory() throws Exception {
    TException error = new TException();
    doThrow(error).when(client).syncLogEntries(any(), any());
    thread.sendBatchAsync(batch, handler);
    verify(handler).onError(error);
    assertEquals(0, memory.getMemoryBlock().getUsedMemoryInBytes());
  }
}
