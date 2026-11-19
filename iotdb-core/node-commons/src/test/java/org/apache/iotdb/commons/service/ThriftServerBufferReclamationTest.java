/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * under the Apache License, Version 2.0.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an AS IS BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.iotdb.commons.service;

import org.apache.iotdb.rpc.AutoResizingBufferMemoryControl;
import org.apache.iotdb.rpc.AutoResizingBufferMemoryManager;
import org.apache.iotdb.rpc.TElasticFramedTransport;

import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.server.TThreadPoolServer;
import org.apache.thrift.transport.TServerSocket;
import org.apache.thrift.transport.TSocket;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.net.InetAddress;
import java.net.ServerSocket;
import java.nio.ByteBuffer;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static org.awaitility.Awaitility.await;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class ThriftServerBufferReclamationTest {

  private final TestMemoryControl memoryControl = new TestMemoryControl();

  @Before
  public void setUp() {
    AutoResizingBufferMemoryManager.setMemoryControl(memoryControl);
  }

  @After
  public void tearDown() {
    AutoResizingBufferMemoryManager.setMemoryControl(
        new AutoResizingBufferMemoryControl() {
          @Override
          public boolean allocate(long sizeInBytes) {
            return true;
          }

          @Override
          public void release(long sizeInBytes) {}
        });
  }

  @Test
  public void testNewConnectionReclaimsIdleBuffersAndKeepsOldConnectionUsable() throws Exception {
    int port;
    try (ServerSocket portProbe = new ServerSocket(0)) {
      port = portProbe.getLocalPort();
    }
    TServerSocket serverSocket = new TServerSocket(port);
    CountDownLatch secondProcessorCall = new CountDownLatch(1);
    AtomicInteger processorCalls = new AtomicInteger();
    TThreadPoolServer.Args args =
        new TThreadPoolServer.Args(serverSocket)
            .minWorkerThreads(0)
            .maxWorkerThreads(4)
            .protocolFactory(new TBinaryProtocol.Factory())
            .processor(
                (input, output) -> {
                  if (processorCalls.incrementAndGet() == 2) {
                    secondProcessorCall.countDown();
                  }
                  output.writeBinary(input.readBinary());
                  output.getTransport().flush();
                })
            .transportFactory(
                AbstractThriftServiceThread.registerServerTransports(
                    new TElasticFramedTransport.Factory(8, 2048, true)));
    ExecutorService workers = Executors.newFixedThreadPool(4);
    args.executorService(workers);
    TThreadPoolServer server = new TThreadPoolServer(args);
    Thread serverThread = new Thread(server::serve);
    serverThread.setDaemon(true);
    serverThread.start();

    try {
      await().atMost(5, TimeUnit.SECONDS).until(server::isServing);
      try (TElasticFramedTransport first = createClientTransport(port);
          TElasticFramedTransport second = createClientTransport(port)) {
        first.open();
        echo(first, new byte[128]);
        assertTrue(secondProcessorCall.await(5, TimeUnit.SECONDS));
        memoryControl.setLimit(memoryControl.getUsedMemoryInBytes());

        second.open();
        echo(second, new byte[8]);
        echo(first, new byte[8]);
      }
    } finally {
      server.stop();
      serverSocket.close();
      workers.shutdownNow();
      assertTrue(workers.awaitTermination(10, TimeUnit.SECONDS));
      serverThread.join(10_000);
      assertFalse(serverThread.isAlive());
    }
    assertEquals(0, memoryControl.getUsedMemoryInBytes());
  }

  private TElasticFramedTransport createClientTransport(int port) throws Exception {
    return new TElasticFramedTransport(
        new TSocket(InetAddress.getLoopbackAddress().getHostAddress(), port, 5000), 8, 2048, true);
  }

  private void echo(TElasticFramedTransport transport, byte[] payload) throws Exception {
    TBinaryProtocol protocol = new TBinaryProtocol(transport);
    protocol.writeBinary(ByteBuffer.wrap(payload));
    transport.flush();
    ByteBuffer response = protocol.readBinary();
    byte[] actual = new byte[response.remaining()];
    response.get(actual);
    assertArrayEquals(payload, actual);
  }

  private static class TestMemoryControl implements AutoResizingBufferMemoryControl {
    private final AtomicLong usedMemoryInBytes = new AtomicLong();
    private volatile long limit = Long.MAX_VALUE;

    @Override
    public boolean allocate(long sizeInBytes) {
      while (true) {
        long current = usedMemoryInBytes.get();
        if (current + sizeInBytes > limit) {
          return false;
        }
        if (usedMemoryInBytes.compareAndSet(current, current + sizeInBytes)) {
          return true;
        }
      }
    }

    @Override
    public void release(long sizeInBytes) {
      usedMemoryInBytes.addAndGet(-sizeInBytes);
    }

    private long getUsedMemoryInBytes() {
      return usedMemoryInBytes.get();
    }

    private void setLimit(long limit) {
      this.limit = limit;
    }
  }
}
