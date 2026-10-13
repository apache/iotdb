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

package org.apache.iotdb.rpc;

import org.apache.iotdb.rpc.i18n.RpcMessages;

import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.server.TServer;
import org.apache.thrift.server.TThreadPoolServer;
import org.apache.thrift.transport.TMemoryBuffer;
import org.apache.thrift.transport.TServerSocket;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransportException;
import org.apache.thrift.transport.layered.TFramedTransport;
import org.junit.After;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.lang.reflect.Field;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

public class AutoResizingBufferTest {

  private final TestMemoryControl memoryControl = new TestMemoryControl();

  @After
  public void tearDown() {
    AutoResizingBufferMemoryManager.resetMemoryControl();
  }

  @Test
  public void testAllocateAndReleaseMemoryWhenResizing() throws Exception {
    AutoResizingBufferMemoryManager.setMemoryControl(memoryControl);

    AutoResizingBuffer buffer = new AutoResizingBuffer(100);
    Assert.assertEquals(100, memoryControl.getUsedMemoryInBytes());

    buffer.resizeIfNecessary(200);
    Assert.assertEquals(200, buffer.array().length);
    Assert.assertEquals(200, memoryControl.getUsedMemoryInBytes());

    setLastShrinkTime(buffer, System.currentTimeMillis() - RpcUtils.MIN_SHRINK_INTERVAL - 1);
    for (int i = 0; i <= RpcUtils.MAX_BUFFER_OVERSIZE_TIME; i++) {
      buffer.resizeIfNecessary(101);
    }
    Assert.assertEquals(150, buffer.array().length);
    Assert.assertEquals(150, memoryControl.getUsedMemoryInBytes());

    buffer.close();
    Assert.assertEquals(0, memoryControl.getUsedMemoryInBytes());
  }

  @Test
  public void testThrowIOExceptionWhenMemoryIsInsufficient() throws Exception {
    memoryControl.setLimit(120);
    AutoResizingBufferMemoryManager.setMemoryControl(memoryControl);
    AutoResizingBufferMemoryManager.setMemoryAllocateRetryIntervalInMs(1);

    AutoResizingBuffer buffer = new AutoResizingBuffer(100);
    try {
      buffer.resizeIfNecessary(200);
      Assert.fail("Expected IOException");
    } catch (IOException e) {
      Assert.assertEquals(
          String.format(
              RpcMessages
                  .EXCEPTION_AUTORESIZINGBUFFER_FAILED_TO_ALLOCATE_ARG_BYTES_AFTER_ARG_RETRIES_POOL_TOTAL_ARG_BYTES_USED_ARG_BYTES_AVAILABLE_ARG_BYTES_8244057A,
              100,
              5,
              120,
              100,
              20),
          e.getMessage());
    } finally {
      Assert.assertEquals(100, memoryControl.getUsedMemoryInBytes());
      buffer.close();
    }
  }

  @Test
  public void testMemoryAllocationMetrics() throws Exception {
    memoryControl.setLimit(120);
    AutoResizingBufferMemoryManager.setMemoryControl(memoryControl);
    AutoResizingBufferMemoryManager.setMemoryAllocateRetryIntervalInMs(1);

    AutoResizingBuffer buffer = new AutoResizingBuffer(100);
    Assert.assertEquals(1, AutoResizingBufferMemoryManager.getMemoryAllocationCount());
    Assert.assertEquals(0, AutoResizingBufferMemoryManager.getMemoryAllocationFailureCount());

    try {
      buffer.resizeIfNecessary(200);
      Assert.fail("Expected IOException");
    } catch (IOException ignored) {
      Assert.assertEquals(1, AutoResizingBufferMemoryManager.getMemoryAllocationCount());
      Assert.assertEquals(1, AutoResizingBufferMemoryManager.getMemoryAllocationFailureCount());
    } finally {
      buffer.close();
    }
  }

  @Test
  public void testSmallFramesShrinkBufferWithoutGoingBelowInitialCapacity() throws Exception {
    AutoResizingBufferMemoryManager.setMemoryControl(memoryControl);

    try (AutoResizingBuffer buffer = new AutoResizingBuffer(100)) {
      buffer.resizeIfNecessary(1600);
      for (int size : new int[] {100, 4, 0}) {
        int previousCapacity = buffer.array().length;
        setLastShrinkTime(buffer, System.currentTimeMillis() - RpcUtils.MIN_SHRINK_INTERVAL - 1);
        for (int i = 0; i <= RpcUtils.MAX_BUFFER_OVERSIZE_TIME; i++) {
          buffer.resizeIfNecessary(size);
        }
        Assert.assertTrue(buffer.array().length < previousCapacity);
        Assert.assertTrue(buffer.array().length >= 100);
        Assert.assertEquals(buffer.array().length, memoryControl.getUsedMemoryInBytes());
      }
    }
    Assert.assertEquals(0, memoryControl.getUsedMemoryInBytes());
  }

  @Test
  public void testNewConnectionReclaimsIdleBuffersUnderMemoryPressure() throws Exception {
    memoryControl.setLimit(80);
    AutoResizingBufferMemoryManager.setMemoryControl(memoryControl);
    AutoResizingBufferMemoryManager.setMemoryAllocateRetryIntervalInMs(1);

    byte[] payload = new byte[40];
    Arrays.fill(payload, (byte) 7);
    try (TElasticFramedTransport transport =
        new TElasticFramedTransport(new TMemoryBuffer(0), 8, 1024, true)) {
      transport.write(payload);
      transport.flush();
      byte[] received = new byte[payload.length];
      transport.readAll(received, 0, received.length);
      Assert.assertArrayEquals(payload, received);
      Assert.assertEquals(80, memoryControl.getUsedMemoryInBytes());

      try (TElasticFramedTransport newConnection =
          new TElasticFramedTransport(new TMemoryBuffer(0), 8, 1024, true)) {
        Assert.assertTrue(memoryControl.getUsedMemoryInBytes() <= 80);
        Assert.assertTrue(
            transport.readBuffer.getBuffer().length == 8
                || transport.writeBuffer.getBuffer().length == 8);
        Assert.assertEquals(0, AutoResizingBufferMemoryManager.getMemoryAllocationFailureCount());
        transport.write(payload);
        transport.flush();
        transport.readAll(received, 0, received.length);
        Assert.assertArrayEquals(payload, received);
      }
    }
    Assert.assertEquals(0, memoryControl.getUsedMemoryInBytes());
  }

  @Test(timeout = 20000)
  public void testThriftServerAcceptsNewConnectionAfterLargeFrame() throws Exception {
    ServerSocket socket = new ServerSocket();
    socket.bind(new InetSocketAddress("127.0.0.1", 0));
    ExecutorService workers = Executors.newFixedThreadPool(2);
    TServer server =
        new TThreadPoolServer(
            new TThreadPoolServer.Args(new TServerSocket(socket))
                .executorService(workers)
                .transportFactory(new TElasticFramedTransport.Factory(8, 1024, true))
                .protocolFactory(new TBinaryProtocol.Factory())
                .processor(
                    (input, output) -> {
                      output.writeBinary(input.readBinary());
                      output.getTransport().flush();
                    }));
    Thread serverThread = new Thread(server::serve, "rpc-buffer-memory-test-server");
    serverThread.setDaemon(true);

    // Client buffers are outside the server's memory control, as in a real deployment.
    try (TElasticFramedTransport first =
            new TElasticFramedTransport(
                new TSocket("127.0.0.1", socket.getLocalPort(), 5000), 8, 1024, true);
        TElasticFramedTransport second =
            new TElasticFramedTransport(
                new TSocket("127.0.0.1", socket.getLocalPort(), 5000), 8, 1024, true)) {
      AutoResizingBufferMemoryManager.setMemoryControl(memoryControl);
      AutoResizingBufferMemoryManager.setMemoryAllocateRetryIntervalInMs(1);
      serverThread.start();
      first.open();
      TProtocol firstProtocol = new TBinaryProtocol(first);
      byte[] payload = new byte[40];
      Arrays.fill(payload, (byte) 13);
      firstProtocol.writeBinary(ByteBuffer.wrap(payload));
      first.flush();
      Assert.assertEquals(ByteBuffer.wrap(payload), firstProtocol.readBinary());

      memoryControl.setLimit(memoryControl.getUsedMemoryInBytes());
      second.open();
      TProtocol secondProtocol = new TBinaryProtocol(second);
      secondProtocol.writeBinary(ByteBuffer.wrap(new byte[] {1, 2, 3, 4}));
      second.flush();
      Assert.assertEquals(ByteBuffer.wrap(new byte[] {1, 2, 3, 4}), secondProtocol.readBinary());
      Assert.assertEquals(0, AutoResizingBufferMemoryManager.getMemoryAllocationFailureCount());
    } finally {
      server.stop();
      workers.shutdownNow();
      Assert.assertTrue(workers.awaitTermination(5, TimeUnit.SECONDS));
      serverThread.join(5000);
      Assert.assertFalse(serverThread.isAlive());
    }
    Assert.assertEquals(0, memoryControl.getUsedMemoryInBytes());
  }

  @Test
  public void testMemoryPressureDoesNotReclaimUnflushedWriteBuffer() throws Exception {
    memoryControl.setLimit(48);
    AutoResizingBufferMemoryManager.setMemoryControl(memoryControl);
    AutoResizingBufferMemoryManager.setMemoryAllocateRetryIntervalInMs(1);

    byte[] payload = new byte[40];
    Arrays.fill(payload, (byte) 9);
    TMemoryBuffer wire = new TMemoryBuffer(0);
    try (TElasticFramedTransport transport = new TElasticFramedTransport(wire, 8, 1024, true)) {
      transport.write(payload);
      Assert.assertThrows(IOException.class, () -> new AutoResizingBuffer(8));
      Assert.assertEquals(48, memoryControl.getUsedMemoryInBytes());

      transport.flush();
      byte[] frame = new byte[4 + payload.length];
      wire.readAll(frame, 0, frame.length);
      Assert.assertEquals(payload.length, TFramedTransport.decodeFrameSize(frame));
      Assert.assertArrayEquals(payload, Arrays.copyOfRange(frame, 4, frame.length));
    }
    Assert.assertEquals(0, memoryControl.getUsedMemoryInBytes());
  }

  @Test
  public void testMemoryPressurePreservesUnreadFrame() throws Exception {
    memoryControl.setLimit(80);
    AutoResizingBufferMemoryManager.setMemoryControl(memoryControl);
    AutoResizingBufferMemoryManager.setMemoryAllocateRetryIntervalInMs(1);

    byte[] payload = new byte[40];
    Arrays.fill(payload, (byte) 11);
    try (TElasticFramedTransport transport =
        new TElasticFramedTransport(new TMemoryBuffer(0), 8, 1024, false)) {
      transport.write(payload);
      transport.flush();
      byte[] received = new byte[payload.length];
      transport.readAll(received, 0, 1);
      Assert.assertEquals(80, memoryControl.getUsedMemoryInBytes());

      try (TElasticFramedTransport newConnection =
          new TElasticFramedTransport(new TMemoryBuffer(0), 8, 1024, true)) {
        Assert.assertEquals(40, transport.readBuffer.getBuffer().length);
        Assert.assertEquals(8, transport.writeBuffer.getBuffer().length);
        transport.readAll(received, 1, received.length - 1);
        Assert.assertArrayEquals(payload, received);
      }
    }
    Assert.assertEquals(0, memoryControl.getUsedMemoryInBytes());
  }

  @Test
  public void testSnappyConnectionReclaimsIdleCompressionBuffers() throws Exception {
    AutoResizingBufferMemoryManager.setMemoryControl(memoryControl);
    AutoResizingBufferMemoryManager.setMemoryAllocateRetryIntervalInMs(1);

    byte[] payload = new byte[4096];
    new Random(0).nextBytes(payload);
    try (TSnappyElasticFramedTransport transport =
        new TSnappyElasticFramedTransport(new TMemoryBuffer(0), 32, 65536, true)) {
      transport.write(payload);
      transport.flush();
      byte[] received = new byte[payload.length];
      transport.readAll(received, 0, received.length);
      Assert.assertArrayEquals(payload, received);
      long retainedMemory = memoryControl.getUsedMemoryInBytes();
      memoryControl.setLimit(retainedMemory);

      try (TSnappyElasticFramedTransport newConnection =
          new TSnappyElasticFramedTransport(new TMemoryBuffer(0), 32, 65536, true)) {
        Assert.assertTrue(memoryControl.getUsedMemoryInBytes() < retainedMemory);
        memoryControl.setLimit(Long.MAX_VALUE);
        transport.write(payload);
        transport.flush();
        transport.readAll(received, 0, received.length);
        Assert.assertArrayEquals(payload, received);
      }
    }
    Assert.assertEquals(0, memoryControl.getUsedMemoryInBytes());
  }

  @Test
  public void testConcurrentAllocationsDoNotWaitForEachOthersBufferLocks() throws Exception {
    memoryControl.setLimit(80);
    CountDownLatch rejectedAllocations = new CountDownLatch(2);
    AutoResizingBufferMemoryManager.setMemoryControl(
        new AutoResizingBufferMemoryControl() {
          @Override
          public boolean allocate(long sizeInBytes) {
            if (memoryControl.allocate(sizeInBytes)) {
              return true;
            }
            // Both threads now hold their buffer locks while attempting to reclaim memory.
            rejectedAllocations.countDown();
            try {
              Assert.assertTrue(rejectedAllocations.await(5, TimeUnit.SECONDS));
            } catch (InterruptedException e) {
              Thread.currentThread().interrupt();
              throw new AssertionError(e);
            }
            return false;
          }

          @Override
          public void release(long sizeInBytes) {
            memoryControl.release(sizeInBytes);
          }
        });
    AutoResizingBufferMemoryManager.setMemoryAllocateRetryIntervalInMs(1);
    ExecutorService executor = Executors.newFixedThreadPool(2);
    try (AutoResizingBuffer first = new AutoResizingBuffer(8);
        AutoResizingBuffer second = new AutoResizingBuffer(8)) {
      first.resizeIfNecessary(40);
      second.resizeIfNecessary(40);
      Future<IOException> firstFailure =
          executor.submit(
              () -> Assert.assertThrows(IOException.class, () -> first.resizeIfNecessary(41)));
      Future<IOException> secondFailure =
          executor.submit(
              () -> Assert.assertThrows(IOException.class, () -> second.resizeIfNecessary(41)));
      Assert.assertNotNull(firstFailure.get(10, TimeUnit.SECONDS));
      Assert.assertNotNull(secondFailure.get(10, TimeUnit.SECONDS));
      Assert.assertEquals(80, memoryControl.getUsedMemoryInBytes());
    } finally {
      executor.shutdownNow();
    }
    Assert.assertEquals(0, memoryControl.getUsedMemoryInBytes());
  }

  @Test
  public void testMemoryPressureDoesNotReclaimBuffersFromAnotherControl() throws Exception {
    AutoResizingBufferMemoryManager.setMemoryControl(memoryControl);
    try (AutoScalingBufferWriteTransport original = new AutoScalingBufferWriteTransport(8)) {
      original.write(new byte[40]);
      original.reset();

      TestMemoryControl replacement = new TestMemoryControl();
      replacement.setLimit(0);
      AutoResizingBufferMemoryManager.setMemoryControl(replacement);
      AutoResizingBufferMemoryManager.setMemoryAllocateRetryIntervalInMs(1);
      Assert.assertThrows(IOException.class, () -> new AutoResizingBuffer(8));
      Assert.assertEquals(40, original.getBuffer().length);
      Assert.assertEquals(40, memoryControl.getUsedMemoryInBytes());
      Assert.assertEquals(0, replacement.getUsedMemoryInBytes());
    }
    Assert.assertEquals(0, memoryControl.getUsedMemoryInBytes());
  }

  @Test
  public void testFailedOpenDoesNotLeakBufferMemoryWhenReconnecting() throws Exception {
    memoryControl.setLimit(200);
    AutoResizingBufferMemoryManager.setMemoryControl(memoryControl);
    AutoResizingBufferMemoryManager.setMemoryAllocateRetryIntervalInMs(1);

    for (int i = 0; i < 10; i++) {
      TSocket socket =
          new TSocket("unavailable", 1) {
            @Override
            public void open() throws TTransportException {
              throw new TTransportException("Simulated connection failure");
            }
          };
      TElasticFramedTransport transport = new TElasticFramedTransport(socket, 50, 1024, true);
      Assert.assertThrows(TTransportException.class, transport::open);
      Assert.assertEquals(0, memoryControl.getUsedMemoryInBytes());
    }
    try (TElasticFramedTransport recovered =
        new TElasticFramedTransport(new TMemoryBuffer(0), 50, 1024, true)) {
      recovered.open();
      Assert.assertEquals(100, memoryControl.getUsedMemoryInBytes());
    }
    Assert.assertEquals(0, memoryControl.getUsedMemoryInBytes());
  }

  @Test
  public void testElasticFramedTransportReleasesMemoryWhenClosed() throws Exception {
    AutoResizingBufferMemoryManager.setMemoryControl(memoryControl);

    TElasticFramedTransport transport =
        new TElasticFramedTransport(new TMemoryBuffer(0), 100, 1024, true);
    Assert.assertEquals(200, memoryControl.getUsedMemoryInBytes());

    transport.close();
    Assert.assertEquals(0, memoryControl.getUsedMemoryInBytes());
  }

  @Test
  public void testElasticFramedTransportReleasesMemoryWhenConstructorFails() {
    memoryControl.setLimit(150);
    AutoResizingBufferMemoryManager.setMemoryControl(memoryControl);
    AutoResizingBufferMemoryManager.setMemoryAllocateRetryIntervalInMs(1);

    try {
      new TElasticFramedTransport(new TMemoryBuffer(0), 100, 1024, true);
      Assert.fail("Expected TTransportException");
    } catch (TTransportException e) {
      Assert.assertEquals(0, memoryControl.getUsedMemoryInBytes());
    }
  }

  @Test
  public void testSnappyElasticFramedTransportReleasesMemory() throws Exception {
    AutoResizingBufferMemoryManager.setMemoryControl(memoryControl);

    TSnappyElasticFramedTransport transport =
        new TSnappyElasticFramedTransport(new TMemoryBuffer(0), 100, 1024, true);
    Assert.assertEquals(400, memoryControl.getUsedMemoryInBytes());

    transport.close();
    Assert.assertEquals(0, memoryControl.getUsedMemoryInBytes());
  }

  @Test
  public void testSnappyElasticFramedTransportReleasesMemoryWhenConstructorFails() {
    memoryControl.setLimit(350);
    AutoResizingBufferMemoryManager.setMemoryControl(memoryControl);
    AutoResizingBufferMemoryManager.setMemoryAllocateRetryIntervalInMs(1);

    try {
      new TSnappyElasticFramedTransport(new TMemoryBuffer(0), 100, 1024, true);
      Assert.fail("Expected TTransportException");
    } catch (TTransportException e) {
      Assert.assertEquals(0, memoryControl.getUsedMemoryInBytes());
    }
  }

  @Test
  public void testBufferReleasesMemoryToTheControlUsedForAllocation() throws Exception {
    TestMemoryControl firstMemoryControl = new TestMemoryControl();
    AutoResizingBufferMemoryManager.setMemoryControl(firstMemoryControl);
    AutoResizingBuffer firstBuffer = new AutoResizingBuffer(100);

    TestMemoryControl secondMemoryControl = new TestMemoryControl();
    AutoResizingBufferMemoryManager.setMemoryControl(secondMemoryControl);
    AutoResizingBuffer secondBuffer = new AutoResizingBuffer(200);

    firstBuffer.close();
    Assert.assertEquals(0, firstMemoryControl.getUsedMemoryInBytes());
    Assert.assertEquals(200, secondMemoryControl.getUsedMemoryInBytes());

    secondBuffer.close();
    Assert.assertEquals(0, secondMemoryControl.getUsedMemoryInBytes());
  }

  @Test
  public void testBufferCreatedWithoutControlDoesNotReleaseToNewControl() throws Exception {
    AutoResizingBuffer bufferWithoutControl = new AutoResizingBuffer(100);

    AutoResizingBufferMemoryManager.setMemoryControl(memoryControl);
    AutoResizingBuffer bufferWithControl = new AutoResizingBuffer(100);

    bufferWithoutControl.close();
    Assert.assertEquals(100, memoryControl.getUsedMemoryInBytes());

    bufferWithoutControl.resizeIfNecessary(100);
    Assert.assertEquals(200, memoryControl.getUsedMemoryInBytes());
    bufferWithoutControl.close();
    Assert.assertEquals(100, memoryControl.getUsedMemoryInBytes());

    bufferWithControl.close();
    Assert.assertEquals(0, memoryControl.getUsedMemoryInBytes());
  }

  private void setLastShrinkTime(AutoResizingBuffer buffer, long lastShrinkTime) throws Exception {
    Field field = AutoResizingBuffer.class.getDeclaredField("lastShrinkTime");
    field.setAccessible(true);
    field.set(buffer, lastShrinkTime);
  }

  private static class TestMemoryControl implements AutoResizingBufferMemoryControl {

    private final AtomicLong usedMemoryInBytes = new AtomicLong();
    private long limit = Long.MAX_VALUE;

    @Override
    public boolean allocate(long sizeInBytes) {
      while (true) {
        long current = usedMemoryInBytes.get();
        long next = current + sizeInBytes;
        if (next > limit) {
          return false;
        }
        if (usedMemoryInBytes.compareAndSet(current, next)) {
          return true;
        }
      }
    }

    @Override
    public void release(long sizeInBytes) {
      usedMemoryInBytes.addAndGet(-sizeInBytes);
    }

    @Override
    public long getUsedMemoryInBytes() {
      return usedMemoryInBytes.get();
    }

    @Override
    public long getTotalMemorySizeInBytes() {
      return limit;
    }

    private void setLimit(long limit) {
      this.limit = limit;
    }
  }
}
