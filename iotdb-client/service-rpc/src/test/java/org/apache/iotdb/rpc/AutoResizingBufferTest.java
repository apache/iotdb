/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iotdb.rpc;

import org.apache.thrift.transport.TMemoryBuffer;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;
import org.junit.After;
import org.junit.Assert;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.lang.reflect.Field;
import java.util.Arrays;
import java.util.Random;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

public class AutoResizingBufferTest {

  private final TestMemoryControl memoryControl = new TestMemoryControl();

  @After
  public void tearDown() {
    AutoResizingBufferMemoryManager.resetMemoryControl();
  }

  @Test
  public void testGrowthNearIntegerLimitKeepsHeadroom() {
    Assert.assertEquals(
        AutoResizingBuffer.MAX_ARRAY_SIZE,
        AutoResizingBuffer.calculateNewCapacity(1_500_000_000, 1_500_000_008));
    Assert.assertEquals(
        AutoResizingBuffer.MAX_ARRAY_SIZE,
        AutoResizingBuffer.calculateNewCapacity(
            AutoResizingBuffer.MAX_ARRAY_SIZE - 8, AutoResizingBuffer.MAX_ARRAY_SIZE));
  }

  @Test
  public void testInvalidBufferSizeDoesNotResize() throws Exception {
    AutoResizingBuffer buffer = new AutoResizingBuffer(8);
    for (int size : new int[] {-1, Integer.MAX_VALUE}) {
      try {
        buffer.ensureCapacity(size);
        Assert.fail("The requested capacity must fit in a Java array");
      } catch (IllegalArgumentException expected) {
        Assert.assertEquals(8, buffer.array().length);
      }
    }
  }

  @Test
  public void testWritePositionOverflowIsRejectedBeforeCopying() throws Exception {
    AutoScalingBufferWriteTransport transport = new AutoScalingBufferWriteTransport(8);
    Field position = AutoScalingBufferWriteTransport.class.getDeclaredField("pos");
    position.setAccessible(true);
    position.setInt(transport, Integer.MAX_VALUE - 4);
    byte[] buffer = transport.getBuffer();
    try {
      transport.write(new byte[8]);
      Assert.fail("The write position must not overflow");
    } catch (TTransportException e) {
      Assert.assertEquals(TTransportException.CORRUPTED_DATA, e.getType());
      Assert.assertSame(buffer, transport.getBuffer());
      Assert.assertEquals(Integer.MAX_VALUE - 4, transport.getPos());
    }
  }

  @Test
  public void testGrowthPreservesDataAndHeadroom() throws Exception {
    AutoResizingBuffer buffer = new AutoResizingBuffer(100);
    buffer.array()[99] = 42;
    buffer.resizeIfNecessary(110);
    Assert.assertEquals(150, buffer.array().length);
    Assert.assertEquals(42, buffer.array()[99]);

    buffer.resizeIfNecessary(1000);
    Assert.assertEquals(1000, buffer.array().length);
    Assert.assertEquals(42, buffer.array()[99]);
  }

  @Test
  public void testShrinkForRequestsAtOrBelowInitialCapacity() throws Exception {
    for (int requestSize : new int[] {0, 1, 100}) {
      AutoResizingBuffer buffer = new AutoResizingBuffer(100);
      buffer.resizeIfNecessary(1000);
      buffer.array()[99] = 42;
      for (int i = 0; i <= RpcUtils.MAX_BUFFER_OVERSIZE_TIME; i++) {
        buffer.resizeIfNecessary(requestSize);
      }
      Assert.assertEquals(550, buffer.array().length);
      Assert.assertEquals(42, buffer.array()[99]);
      for (int i = 0; i < 20; i++) {
        setLastShrinkTime(buffer, 0);
        for (int j = 0; j <= RpcUtils.MAX_BUFFER_OVERSIZE_TIME; j++) {
          buffer.resizeIfNecessary(requestSize);
        }
        Assert.assertTrue(buffer.array().length >= 100);
      }
    }
  }

  @Test
  public void testMemoryPressureReclaimsIdleServerTransportBuffers() throws Exception {
    memoryControl.setLimit(96);
    AutoResizingBufferMemoryManager.setMemoryControl(memoryControl);
    AutoResizingBufferMemoryManager.setMemoryAllocateRetryIntervalInMs(1);

    try (AutoResizingBufferMemoryManager.ServerBufferPool pool =
        AutoResizingBufferMemoryManager.createServerBufferPool()) {
      try (TElasticFramedTransport transport = newServerTransport(pool, new TMemoryBuffer(0))) {
        transport.write(new byte[64]);
        transport.flush();
        Assert.assertEquals(64, transport.writeBuffer.getBuffer().length);
        Assert.assertEquals(72, memoryControl.getUsedMemoryInBytes());

        try (TElasticFramedTransport requester = newServerTransport(pool, new TMemoryBuffer(0))) {
          requester.write(new byte[32]);
          requester.flush();
          Assert.assertEquals(8, transport.writeBuffer.getBuffer().length);
          Assert.assertEquals(32, requester.writeBuffer.getBuffer().length);
          Assert.assertEquals(56, memoryControl.getUsedMemoryInBytes());
        }
      }
    }
    Assert.assertEquals(0, memoryControl.getUsedMemoryInBytes());
  }

  @Test
  public void testMemoryPressureReclaimsCompressedServerBuffers() throws Exception {
    AutoResizingBufferMemoryManager.setMemoryControl(memoryControl);
    AutoResizingBufferMemoryManager.setMemoryAllocateRetryIntervalInMs(1);

    try (AutoResizingBufferMemoryManager.ServerBufferPool pool =
            AutoResizingBufferMemoryManager.createServerBufferPool();
        TSnappyElasticFramedTransport transport =
            (TSnappyElasticFramedTransport)
                pool.wrapTransportFactory(new TSnappyElasticFramedTransport.Factory(8, 2048, false))
                    .getTransport(new TMemoryBuffer(0))) {
      byte[] payload = new byte[1024];
      new Random(0).nextBytes(payload);
      transport.write(payload);
      transport.flush();
      byte[] actual = new byte[payload.length];
      transport.readAll(actual, 0, actual.length);
      Assert.assertArrayEquals(payload, actual);

      AutoResizingBuffer[] buffers = reusableBuffers(transport);
      for (AutoResizingBuffer buffer : buffers) {
        Assert.assertTrue(buffer.array().length > 8);
      }
      transport.endServerRequest();

      memoryControl.setLimit(40);
      Assert.assertTrue(pool.reclaimIdleBuffers(8) >= 8);
      try (AutoResizingBuffer allocation = new AutoResizingBuffer(8)) {
        for (AutoResizingBuffer buffer : buffers) {
          Assert.assertEquals(8, buffer.array().length);
        }
        Assert.assertEquals(40, memoryControl.getUsedMemoryInBytes());
      }
    }
    Assert.assertEquals(0, memoryControl.getUsedMemoryInBytes());
  }

  @Test
  public void testMemoryPressureReusesPreferredPoolCandidateSnapshot() throws Exception {
    AutoResizingBufferMemoryManager.setMemoryControl(memoryControl);
    AutoResizingBufferMemoryManager.setMemoryAllocateRetryIntervalInMs(1);

    try (AutoResizingBufferMemoryManager.ServerBufferPool pool =
        AutoResizingBufferMemoryManager.createServerBufferPool()) {
      try (TElasticFramedTransport oldest = newServerTransport(pool, new TMemoryBuffer(0));
          TElasticFramedTransport newer = newServerTransport(pool, new TMemoryBuffer(0));
          TElasticFramedTransport firstRequester = newServerTransport(pool, new TMemoryBuffer(0));
          TElasticFramedTransport secondRequester =
              newServerTransport(pool, new TMemoryBuffer(0))) {
        oldest.write(new byte[64]);
        oldest.flush();
        Thread.sleep(10);
        newer.write(new byte[64]);
        newer.flush();
        memoryControl.setLimit(memoryControl.getUsedMemoryInBytes());

        firstRequester.write(new byte[16]);
        firstRequester.flush();
        Object candidateSnapshot =
            getFieldValue(
                pool, AutoResizingBufferMemoryManager.ServerBufferPool.class, "candidateSnapshot");
        Assert.assertNotNull(candidateSnapshot);

        memoryControl.setLimit(memoryControl.getUsedMemoryInBytes());
        secondRequester.write(new byte[16]);
        secondRequester.flush();

        Assert.assertSame(
            candidateSnapshot,
            getFieldValue(
                pool, AutoResizingBufferMemoryManager.ServerBufferPool.class, "candidateSnapshot"));
        Assert.assertEquals(8, oldest.writeBuffer.getBuffer().length);
        Assert.assertEquals(8, newer.writeBuffer.getBuffer().length);
      }
    }
    Assert.assertEquals(0, memoryControl.getUsedMemoryInBytes());
  }

  @Test
  public void testMemoryPressureReusesOtherPoolCandidateSnapshot() throws Exception {
    AutoResizingBufferMemoryManager.setMemoryControl(memoryControl);
    AutoResizingBufferMemoryManager.setMemoryAllocateRetryIntervalInMs(1);

    try (AutoResizingBufferMemoryManager.ServerBufferPool pool =
        AutoResizingBufferMemoryManager.createServerBufferPool()) {
      try (TElasticFramedTransport oldest = newServerTransport(pool, new TMemoryBuffer(0));
          TElasticFramedTransport newer = newServerTransport(pool, new TMemoryBuffer(0));
          TElasticFramedTransport firstRequester =
              new TElasticFramedTransport(new TMemoryBuffer(0), 8, 128, true);
          TElasticFramedTransport secondRequester =
              new TElasticFramedTransport(new TMemoryBuffer(0), 8, 128, true)) {
        oldest.write(new byte[64]);
        oldest.flush();
        Thread.sleep(10);
        newer.write(new byte[64]);
        newer.flush();
        memoryControl.setLimit(memoryControl.getUsedMemoryInBytes());

        firstRequester.write(new byte[16]);
        firstRequester.flush();
        Object candidateSnapshot =
            getFieldValue(
                null, AutoResizingBufferMemoryManager.class, "otherPoolCandidateSnapshot");
        Assert.assertNotNull(candidateSnapshot);

        memoryControl.setLimit(memoryControl.getUsedMemoryInBytes());
        secondRequester.write(new byte[16]);
        secondRequester.flush();

        Assert.assertSame(
            candidateSnapshot,
            getFieldValue(
                null, AutoResizingBufferMemoryManager.class, "otherPoolCandidateSnapshot"));
        Assert.assertEquals(8, oldest.writeBuffer.getBuffer().length);
        Assert.assertEquals(8, newer.writeBuffer.getBuffer().length);
      }
    }
    Assert.assertEquals(0, memoryControl.getUsedMemoryInBytes());
  }

  @Test
  public void testMemoryPressureDoesNotReclaimAnActiveServerTransport() throws Exception {
    memoryControl.setLimit(96);
    AutoResizingBufferMemoryManager.setMemoryControl(memoryControl);
    AutoResizingBufferMemoryManager.setMemoryAllocateRetryIntervalInMs(1);

    try (AutoResizingBufferMemoryManager.ServerBufferPool pool =
        AutoResizingBufferMemoryManager.createServerBufferPool()) {
      try (TElasticFramedTransport transport = newServerTransport(pool, new TMemoryBuffer(0))) {
        transport.write(new byte[64]);
        transport.flush();
        transport.beginServerRequest();
        try {
          try (TElasticFramedTransport requester = newServerTransport(pool, new TMemoryBuffer(0))) {
            requester.write(new byte[32]);
            Assert.fail("An active request must keep its buffers reserved");
          } catch (TTransportException expected) {
            Assert.assertEquals(
                1, AutoResizingBufferMemoryManager.getMemoryAllocationFailureCount());
          }
          Assert.assertEquals(64, transport.writeBuffer.getBuffer().length);
          Assert.assertEquals(72, memoryControl.getUsedMemoryInBytes());
        } finally {
          transport.endServerRequest();
        }

        try (TElasticFramedTransport requester = newServerTransport(pool, new TMemoryBuffer(0))) {
          requester.write(new byte[32]);
          requester.flush();
          Assert.assertEquals(8, transport.writeBuffer.getBuffer().length);
          Assert.assertEquals(56, memoryControl.getUsedMemoryInBytes());
        }
      }
    }
    Assert.assertEquals(0, memoryControl.getUsedMemoryInBytes());
  }

  @Test
  public void testMemoryPressureReclaimsOldestConnectionAndStopsWhenEnough() throws Exception {
    AutoResizingBufferMemoryManager.setMemoryControl(memoryControl);
    AutoResizingBufferMemoryManager.setMemoryAllocateRetryIntervalInMs(1);

    try (AutoResizingBufferMemoryManager.ServerBufferPool pool =
        AutoResizingBufferMemoryManager.createServerBufferPool()) {
      try (TElasticFramedTransport oldest = newServerTransport(pool, new TMemoryBuffer(0))) {
        oldest.write(new byte[32]);
        oldest.flush();
        Thread.sleep(10);

        try (TElasticFramedTransport newer = newServerTransport(pool, new TMemoryBuffer(0))) {
          newer.write(new byte[64]);
          newer.flush();

          try (TElasticFramedTransport requester = newServerTransport(pool, new TMemoryBuffer(0))) {
            memoryControl.setLimit(memoryControl.getUsedMemoryInBytes());
            requester.write(new byte[16]);
            requester.flush();

            Assert.assertEquals(8, oldest.writeBuffer.getBuffer().length);
            Assert.assertEquals(64, newer.writeBuffer.getBuffer().length);
            Assert.assertEquals(16, requester.writeBuffer.getBuffer().length);
          }
        }
      }
    }
    Assert.assertEquals(0, memoryControl.getUsedMemoryInBytes());
  }

  @Test
  public void testMemoryPressureDoesNotReclaimClosedServerPool() throws Exception {
    AutoResizingBufferMemoryManager.setMemoryControl(memoryControl);
    AutoResizingBufferMemoryManager.setMemoryAllocateRetryIntervalInMs(1);

    try (AutoResizingBufferMemoryManager.ServerBufferPool requesterPool =
            AutoResizingBufferMemoryManager.createServerBufferPool();
        AutoResizingBufferMemoryManager.ServerBufferPool otherPool =
            AutoResizingBufferMemoryManager.createServerBufferPool()) {
      try (TElasticFramedTransport otherPoolTransport =
              newServerTransport(otherPool, new TMemoryBuffer(0));
          TElasticFramedTransport requester =
              newServerTransport(requesterPool, new TMemoryBuffer(0))) {
        otherPoolTransport.write(new byte[64]);
        otherPoolTransport.flush();
        otherPool.close();
        memoryControl.setLimit(memoryControl.getUsedMemoryInBytes());

        try {
          requester.write(new byte[32]);
          Assert.fail("An unrelated pool must not be reclaimed");
        } catch (TTransportException expected) {
          Assert.assertEquals(64, otherPoolTransport.writeBuffer.getBuffer().length);
          Assert.assertEquals(88, memoryControl.getUsedMemoryInBytes());
        }
      }
    }
    Assert.assertEquals(0, memoryControl.getUsedMemoryInBytes());
  }

  @Test
  public void testMemoryPressureFallsBackToAnotherServerPool() throws Exception {
    AutoResizingBufferMemoryManager.setMemoryControl(memoryControl);
    AutoResizingBufferMemoryManager.setMemoryAllocateRetryIntervalInMs(1);

    try (AutoResizingBufferMemoryManager.ServerBufferPool requesterPool =
            AutoResizingBufferMemoryManager.createServerBufferPool();
        AutoResizingBufferMemoryManager.ServerBufferPool otherPool =
            AutoResizingBufferMemoryManager.createServerBufferPool()) {
      try (TElasticFramedTransport otherPoolTransport =
              newServerTransport(otherPool, new TMemoryBuffer(0));
          TElasticFramedTransport requester =
              newServerTransport(requesterPool, new TMemoryBuffer(0))) {
        otherPoolTransport.write(new byte[64]);
        otherPoolTransport.flush();
        memoryControl.setLimit(memoryControl.getUsedMemoryInBytes());

        requester.write(new byte[32]);
        requester.flush();

        Assert.assertEquals(8, otherPoolTransport.writeBuffer.getBuffer().length);
        Assert.assertEquals(32, requester.writeBuffer.getBuffer().length);
        Assert.assertEquals(56, memoryControl.getUsedMemoryInBytes());
      }
    }
    Assert.assertEquals(0, memoryControl.getUsedMemoryInBytes());
  }

  @Test
  public void testMemoryPressureReclaimsConnectionsRegisteredAfterPoolRetirement()
      throws Exception {
    AutoResizingBufferMemoryManager.setMemoryControl(memoryControl);
    AutoResizingBufferMemoryManager.setMemoryAllocateRetryIntervalInMs(1);

    try (AutoResizingBufferMemoryManager.ServerBufferPool pool =
        AutoResizingBufferMemoryManager.createServerBufferPool()) {
      pool.retire();
      try (TElasticFramedTransport serverTransport =
          newServerTransport(pool, new TMemoryBuffer(0))) {
        serverTransport.write(new byte[64]);
        serverTransport.flush();
        long memoryBeforeClientAllocation = memoryControl.getUsedMemoryInBytes();
        memoryControl.setLimit(memoryBeforeClientAllocation);

        try (TElasticFramedTransport clientTransport =
            new TElasticFramedTransport(new TMemoryBuffer(0), 8, 128, true)) {
          Assert.assertTrue(memoryControl.getUsedMemoryInBytes() < memoryBeforeClientAllocation);
        }
      }
    }
    Assert.assertEquals(0, memoryControl.getUsedMemoryInBytes());
  }

  @Test
  public void testMemoryPressureReclaimsExistingConnectionsFromRetiredPool() throws Exception {
    AutoResizingBufferMemoryManager.setMemoryControl(memoryControl);
    AutoResizingBufferMemoryManager.setMemoryAllocateRetryIntervalInMs(1);

    try (AutoResizingBufferMemoryManager.ServerBufferPool pool =
        AutoResizingBufferMemoryManager.createServerBufferPool()) {
      try (TElasticFramedTransport serverTransport =
          newServerTransport(pool, new TMemoryBuffer(0))) {
        serverTransport.write(new byte[64]);
        serverTransport.flush();
        pool.retire();
        long memoryBeforeClientAllocation = memoryControl.getUsedMemoryInBytes();
        memoryControl.setLimit(memoryBeforeClientAllocation);

        try (TElasticFramedTransport clientTransport =
            new TElasticFramedTransport(new TMemoryBuffer(0), 8, 128, true)) {
          Assert.assertTrue(memoryControl.getUsedMemoryInBytes() < memoryBeforeClientAllocation);
        }
      }
    }
    Assert.assertEquals(0, memoryControl.getUsedMemoryInBytes());
  }

  @Test
  public void testMemoryPressurePrefersRequestingPoolBeforeOtherPools() throws Exception {
    AutoResizingBufferMemoryManager.setMemoryControl(memoryControl);
    AutoResizingBufferMemoryManager.setMemoryAllocateRetryIntervalInMs(1);

    try (AutoResizingBufferMemoryManager.ServerBufferPool otherPool =
            AutoResizingBufferMemoryManager.createServerBufferPool();
        AutoResizingBufferMemoryManager.ServerBufferPool preferredPool =
            AutoResizingBufferMemoryManager.createServerBufferPool()) {
      try (TElasticFramedTransport otherPoolTransport =
          newServerTransport(otherPool, new TMemoryBuffer(0))) {
        otherPoolTransport.write(new byte[64]);
        otherPoolTransport.flush();
        Thread.sleep(10);

        try (TElasticFramedTransport preferredPoolTransport =
                newServerTransport(preferredPool, new TMemoryBuffer(0));
            TElasticFramedTransport requester =
                newServerTransport(preferredPool, new TMemoryBuffer(0))) {
          preferredPoolTransport.write(new byte[32]);
          preferredPoolTransport.flush();
          memoryControl.setLimit(memoryControl.getUsedMemoryInBytes());

          requester.write(new byte[16]);
          requester.flush();

          Assert.assertEquals(64, otherPoolTransport.writeBuffer.getBuffer().length);
          Assert.assertEquals(8, preferredPoolTransport.writeBuffer.getBuffer().length);
          Assert.assertEquals(16, requester.writeBuffer.getBuffer().length);
        }
      }
    }
    Assert.assertEquals(0, memoryControl.getUsedMemoryInBytes());
  }

  @Test
  public void testClientBufferAllocationCanReclaimIdleServerBuffers() throws Exception {
    AutoResizingBufferMemoryManager.setMemoryControl(memoryControl);
    AutoResizingBufferMemoryManager.setMemoryAllocateRetryIntervalInMs(1);

    try (AutoResizingBufferMemoryManager.ServerBufferPool serverPool =
        AutoResizingBufferMemoryManager.createServerBufferPool()) {
      try (TElasticFramedTransport serverTransport =
              newServerTransport(serverPool, new TMemoryBuffer(0));
          TElasticFramedTransport clientTransport =
              new TElasticFramedTransport(new TMemoryBuffer(0), 8, 2048, true)) {
        serverTransport.write(new byte[64]);
        serverTransport.flush();
        memoryControl.setLimit(memoryControl.getUsedMemoryInBytes());

        clientTransport.write(new byte[32]);
        clientTransport.flush();

        Assert.assertEquals(8, serverTransport.writeBuffer.getBuffer().length);
        Assert.assertEquals(32, clientTransport.writeBuffer.getBuffer().length);
      }
    }
    Assert.assertEquals(0, memoryControl.getUsedMemoryInBytes());
  }

  @Test
  public void testReclaimOneIdleConnectionKeepsOtherConnectionsUnchanged() throws Exception {
    AutoResizingBufferMemoryManager.setMemoryControl(memoryControl);

    try (AutoResizingBufferMemoryManager.ServerBufferPool pool =
        AutoResizingBufferMemoryManager.createServerBufferPool()) {
      try (TElasticFramedTransport oldest = newServerTransport(pool, new TMemoryBuffer(0))) {
        oldest.write(new byte[32]);
        oldest.flush();
        Thread.sleep(10);

        try (TElasticFramedTransport newer = newServerTransport(pool, new TMemoryBuffer(0))) {
          newer.write(new byte[64]);
          newer.flush();

          Assert.assertTrue(pool.reclaimOneIdleConnection() > 0);
          Assert.assertEquals(8, oldest.writeBuffer.getBuffer().length);
          Assert.assertEquals(64, newer.writeBuffer.getBuffer().length);
        }
      }
    }
    Assert.assertEquals(0, memoryControl.getUsedMemoryInBytes());
  }

  @Test
  public void testReclaimOneIdleConnectionRefreshesCandidateSnapshot() throws Exception {
    AutoResizingBufferMemoryManager.setMemoryControl(memoryControl);

    try (AutoResizingBufferMemoryManager.ServerBufferPool pool =
        AutoResizingBufferMemoryManager.createServerBufferPool()) {
      try (TElasticFramedTransport oldest = newServerTransport(pool, new TMemoryBuffer(0))) {
        oldest.write(new byte[32]);
        oldest.flush();
        Thread.sleep(10);

        try (TElasticFramedTransport next = newServerTransport(pool, new TMemoryBuffer(0))) {
          next.write(new byte[64]);
          next.flush();

          Assert.assertTrue(pool.reclaimOneIdleConnection() > 0);
          Assert.assertEquals(8, oldest.writeBuffer.getBuffer().length);

          next.getServerConnectionState().setRequestInProgress(true);
          try (TElasticFramedTransport newest = newServerTransport(pool, new TMemoryBuffer(0))) {
            newest.write(new byte[48]);
            newest.flush();

            Assert.assertTrue(pool.reclaimOneIdleConnection() > 0);
            Assert.assertEquals(8, newest.writeBuffer.getBuffer().length);

            next.getServerConnectionState().setRequestInProgress(false);
            Assert.assertTrue(pool.reclaimOneIdleConnection() > 0);
            Assert.assertEquals(8, next.writeBuffer.getBuffer().length);
          }
        }
      }
    }
    Assert.assertEquals(0, memoryControl.getUsedMemoryInBytes());
  }

  @Test
  public void testMemoryPressureReclaimsBothServerTransportsWhileWaitingForNextFrame()
      throws Exception {
    AutoResizingBufferMemoryManager.setMemoryControl(memoryControl);
    AutoResizingBufferMemoryManager.setMemoryAllocateRetryIntervalInMs(1);

    BlockingServerTransport socket = new BlockingServerTransport();
    try (AutoResizingBufferMemoryManager.ServerBufferPool pool =
            AutoResizingBufferMemoryManager.createServerBufferPool();
        TElasticFramedTransport input = newServerTransport(pool, socket);
        TElasticFramedTransport output = newServerTransport(pool, socket)) {

      byte[] firstFrame = new byte[64];
      Arrays.fill(firstFrame, (byte) 7);
      socket.offerFrame(firstFrame);
      byte[] firstRead = new byte[firstFrame.length];
      input.readAll(firstRead, 0, firstRead.length);
      Assert.assertArrayEquals(firstFrame, firstRead);
      output.write(firstFrame, 0, firstFrame.length);
      output.flush();
      Assert.assertEquals(64, input.readBuffer.getBuffer().length);
      Assert.assertEquals(64, output.writeBuffer.getBuffer().length);

      memoryControl.setLimit(memoryControl.getUsedMemoryInBytes());
      ExecutorService reader = Executors.newSingleThreadExecutor();
      try {
        byte[] secondFrame = new byte[8];
        Future<byte[]> nextRead =
            reader.submit(
                () -> {
                  byte[] result = new byte[secondFrame.length];
                  input.readAll(result, 0, result.length);
                  return result;
                });
        Assert.assertTrue(socket.awaitWaitingForNextFrame(5, TimeUnit.SECONDS));

        try (TElasticFramedTransport requester = newServerTransport(pool, new TMemoryBuffer(0))) {
          Assert.assertEquals(8, input.readBuffer.getBuffer().length);
          Assert.assertEquals(8, output.writeBuffer.getBuffer().length);
          Assert.assertEquals(0, input.readBuffer.getBufferPosition());
          Assert.assertEquals(0, input.readBuffer.getBytesRemainingInBuffer());
          Assert.assertEquals(48, memoryControl.getUsedMemoryInBytes());
        }

        Arrays.fill(secondFrame, (byte) 9);
        socket.offerFrame(secondFrame);
        Assert.assertArrayEquals(secondFrame, nextRead.get(5, TimeUnit.SECONDS));
      } finally {
        reader.shutdownNow();
        Assert.assertTrue(reader.awaitTermination(5, TimeUnit.SECONDS));
      }
    }
    Assert.assertEquals(0, memoryControl.getUsedMemoryInBytes());
  }

  @Test
  public void testSmallFramesRespectShrinkCountAndCooldown() throws Exception {
    AutoResizingBuffer buffer = new AutoResizingBuffer(100);
    buffer.resizeIfNecessary(1000);
    byte[] largeBuffer = buffer.array();
    setLastShrinkTime(buffer, System.currentTimeMillis() + RpcUtils.MIN_SHRINK_INTERVAL);

    for (int i = 0; i <= RpcUtils.MAX_BUFFER_OVERSIZE_TIME; i++) {
      buffer.resizeIfNecessary(101);
    }
    Assert.assertSame(largeBuffer, buffer.array());

    setLastShrinkTime(buffer, 0);
    buffer.resizeIfNecessary(101);
    Assert.assertEquals(550, buffer.array().length);
    byte[] shrunkBuffer = buffer.array();
    for (int i = 0; i <= RpcUtils.MAX_BUFFER_OVERSIZE_TIME; i++) {
      buffer.resizeIfNecessary(101);
    }
    Assert.assertSame(shrunkBuffer, buffer.array());

    AutoResizingBuffer countLimitedBuffer = new AutoResizingBuffer(100);
    countLimitedBuffer.resizeIfNecessary(1000);
    byte[] originalBuffer = countLimitedBuffer.array();
    for (int i = 0; i < RpcUtils.MAX_BUFFER_OVERSIZE_TIME; i++) {
      countLimitedBuffer.resizeIfNecessary(101);
      Assert.assertSame(originalBuffer, countLimitedBuffer.array());
    }
    countLimitedBuffer.resizeIfNecessary(101);
    Assert.assertEquals(550, countLimitedBuffer.array().length);
  }

  @Test
  public void testFramedTransportsShrinkAfterLargeFrames() throws Exception {
    for (boolean compressed : new boolean[] {false, true}) {
      try (TElasticFramedTransport transport = newFramedTransport(compressed)) {
        byte[] payload = new byte[1000];
        payload[999] = 42;
        transport.write(payload);
        transport.flush();
        byte[] actual = new byte[payload.length];
        transport.readAll(actual, 0, actual.length);
        Assert.assertArrayEquals(payload, actual);
        byte[] largeWriteBuffer = transport.writeBuffer.getBuffer();
        byte[] largeReadBuffer = transport.readBuffer.getBuffer();
        long cooldown = System.currentTimeMillis() + RpcUtils.MIN_SHRINK_INTERVAL;
        setLastShrinkTime(transport.writeBuffer, cooldown);
        setLastShrinkTime(transport.readBuffer, cooldown);

        for (int i = 0; i <= RpcUtils.MAX_BUFFER_OVERSIZE_TIME; i++) {
          transport.write(new byte[] {42});
          transport.flush();
          byte[] smallFrame = new byte[1];
          transport.readAll(smallFrame, 0, smallFrame.length);
          Assert.assertEquals(42, smallFrame[0]);
        }
        Assert.assertSame(largeWriteBuffer, transport.writeBuffer.getBuffer());
        Assert.assertSame(largeReadBuffer, transport.readBuffer.getBuffer());

        setLastShrinkTime(transport.writeBuffer, 0);
        setLastShrinkTime(transport.readBuffer, 0);
        transport.write(new byte[] {42});
        transport.flush();
        byte[] smallFrame = new byte[1];
        transport.readAll(smallFrame, 0, 1);
        Assert.assertEquals(42, smallFrame[0]);
        Assert.assertTrue(transport.writeBuffer.getBuffer().length < payload.length);
        Assert.assertTrue(transport.readBuffer.getBuffer().length < payload.length);
        Assert.assertTrue(transport.writeBuffer.getBuffer().length >= 100);
        Assert.assertTrue(transport.readBuffer.getBuffer().length >= 100);

        byte[] shrunkBuffer = transport.writeBuffer.getBuffer();
        for (int i = 0; i <= RpcUtils.MAX_BUFFER_OVERSIZE_TIME; i++) {
          transport.write(new byte[] {42});
          transport.flush();
          transport.readAll(smallFrame, 0, 1);
          Assert.assertEquals(42, smallFrame[0]);
        }
        Assert.assertSame(shrunkBuffer, transport.writeBuffer.getBuffer());
      }
    }
  }

  @Test
  public void testRepeatedLargeFramesKeepWriteCapacity() throws Exception {
    for (boolean compressed : new boolean[] {false, true}) {
      try (TElasticFramedTransport transport = newFramedTransport(compressed)) {
        byte[] payload = new byte[1000];
        new Random(0).nextBytes(payload);
        transport.write(payload);
        transport.flush();
        byte[] actual = new byte[payload.length];
        transport.readAll(actual, 0, actual.length);
        Assert.assertArrayEquals(payload, actual);
        byte[] writeBuffer = transport.writeBuffer.getBuffer();
        for (int i = 0; i < 3 * (RpcUtils.MAX_BUFFER_OVERSIZE_TIME + 1); i++) {
          setLastShrinkTime(transport.writeBuffer, 0);
          transport.write(payload, 0, 200);
          transport.write(payload, 200, payload.length - 200);
          transport.flush();
          Assert.assertSame(writeBuffer, transport.writeBuffer.getBuffer());
          transport.readAll(actual, 0, actual.length);
          Assert.assertArrayEquals(payload, actual);
        }
      }
    }
  }

  @Test
  public void testSmallWritesDoNotShrinkAnUnfinishedFrame() throws Exception {
    for (boolean compressed : new boolean[] {false, true}) {
      try (TElasticFramedTransport transport = newFramedTransport(compressed)) {
        byte[] payload = new byte[1000];
        new Random(0).nextBytes(payload);
        transport.write(payload);
        transport.flush();
        byte[] actual = new byte[payload.length];
        transport.readAll(actual, 0, actual.length);
        Assert.assertArrayEquals(payload, actual);
        byte[] writeBuffer = transport.writeBuffer.getBuffer();
        setLastShrinkTime(transport.writeBuffer, 0);

        int fragmentLength = 32;
        int prefixLength = fragmentLength * (RpcUtils.MAX_BUFFER_OVERSIZE_TIME + 5);
        for (int offset = 0; offset < prefixLength; offset += fragmentLength) {
          transport.write(payload, offset, fragmentLength);
          Assert.assertSame(writeBuffer, transport.writeBuffer.getBuffer());
          Assert.assertEquals(offset + fragmentLength, transport.writeBuffer.getPos());
        }
        transport.write(payload, prefixLength, payload.length - prefixLength);
        Assert.assertSame(writeBuffer, transport.writeBuffer.getBuffer());
        transport.flush();
        transport.readAll(actual, 0, actual.length);
        Assert.assertArrayEquals(payload, actual);
      }
    }
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
    } catch (AutoResizingBufferMemoryAllocationException e) {
      Assert.assertTrue(e.getMessage().contains("100"));
      Assert.assertTrue(e.getMessage().contains("5"));
    } finally {
      Assert.assertEquals(100, memoryControl.getUsedMemoryInBytes());
      buffer.close();
    }
  }

  @Test
  public void testInterruptedMemoryAllocationRemainsRegularIOException() {
    memoryControl.setLimit(0);
    AutoResizingBufferMemoryManager.setMemoryControl(memoryControl);
    AutoResizingBufferMemoryManager.setMemoryAllocateRetryIntervalInMs(1);
    final boolean wasInterrupted = Thread.interrupted();

    try {
      Thread.currentThread().interrupt();
      new AutoResizingBuffer(100);
      Assert.fail("Expected allocation interruption");
    } catch (IOException e) {
      Assert.assertFalse(e instanceof AutoResizingBufferMemoryAllocationException);
      Assert.assertTrue(e.getCause() instanceof InterruptedException);
      Assert.assertTrue(Thread.currentThread().isInterrupted());
    } finally {
      if (wasInterrupted) {
        Thread.currentThread().interrupt();
      } else {
        Thread.interrupted();
      }
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

    bufferWithoutControl.ensureCapacity(100);
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

  private Object getFieldValue(Object target, Class<?> declaringClass, String fieldName)
      throws Exception {
    Field field = declaringClass.getDeclaredField(fieldName);
    field.setAccessible(true);
    Object value = field.get(target);
    return value instanceof AtomicReference<?> reference ? reference.get() : value;
  }

  private void setLastShrinkTime(Object transport, long lastShrinkTime) throws Exception {
    Field field = transport.getClass().getDeclaredField("buf");
    field.setAccessible(true);
    setLastShrinkTime((AutoResizingBuffer) field.get(transport), lastShrinkTime);
  }

  private TElasticFramedTransport newFramedTransport(boolean compressed) throws Exception {
    return compressed
        ? new TSnappyElasticFramedTransport(new TMemoryBuffer(0), 100, 2048, true)
        : new TElasticFramedTransport(new TMemoryBuffer(0), 100, 2048, true);
  }

  private TElasticFramedTransport newServerTransport(
      AutoResizingBufferMemoryManager.ServerBufferPool pool, TTransport socket)
      throws TTransportException {
    return (TElasticFramedTransport)
        pool.wrapTransportFactory(new TElasticFramedTransport.Factory(8, 128, false))
            .getTransport(socket);
  }

  private AutoResizingBuffer[] reusableBuffers(TSnappyElasticFramedTransport transport)
      throws Exception {
    return new AutoResizingBuffer[] {
      reusableBuffer(transport.writeBuffer),
      reusableBuffer(transport.readBuffer),
      compressionBuffer(transport, "writeCompressBuffer"),
      compressionBuffer(transport, "readCompressBuffer")
    };
  }

  private AutoResizingBuffer compressionBuffer(
      TCompressedElasticFramedTransport transport, String name) throws Exception {
    Field field = TCompressedElasticFramedTransport.class.getDeclaredField(name);
    field.setAccessible(true);
    return reusableBuffer(field.get(transport));
  }

  private AutoResizingBuffer reusableBuffer(Object transport) throws Exception {
    Field field = transport.getClass().getDeclaredField("buf");
    field.setAccessible(true);
    return (AutoResizingBuffer) field.get(transport);
  }

  private static class BlockingServerTransport extends org.apache.thrift.transport.TTransport {

    private final BlockingQueue<byte[]> frames = new LinkedBlockingQueue<>();
    private final ByteArrayOutputStream writes = new ByteArrayOutputStream();
    private final CountDownLatch waitingForNextFrame = new CountDownLatch(1);
    private byte[] currentFrame;
    private int currentFramePosition;
    private boolean open = true;

    private void offerFrame(byte[] payload) {
      byte[] frame = new byte[payload.length + Integer.BYTES];
      frame[0] = (byte) (payload.length >>> 24);
      frame[1] = (byte) (payload.length >>> 16);
      frame[2] = (byte) (payload.length >>> 8);
      frame[3] = (byte) payload.length;
      System.arraycopy(payload, 0, frame, Integer.BYTES, payload.length);
      frames.add(frame);
    }

    private boolean awaitWaitingForNextFrame(long timeout, TimeUnit unit)
        throws InterruptedException {
      return waitingForNextFrame.await(timeout, unit);
    }

    @Override
    public boolean isOpen() {
      return open;
    }

    @Override
    public void open() {
      open = true;
    }

    @Override
    public void close() {
      open = false;
    }

    @Override
    public int read(byte[] buffer, int offset, int length)
        throws org.apache.thrift.transport.TTransportException {
      try {
        if (currentFrame == null || currentFramePosition == currentFrame.length) {
          if (frames.isEmpty()) {
            waitingForNextFrame.countDown();
          }
          currentFrame = frames.take();
          currentFramePosition = 0;
        }
        int bytesToRead = Math.min(length, currentFrame.length - currentFramePosition);
        System.arraycopy(currentFrame, currentFramePosition, buffer, offset, bytesToRead);
        currentFramePosition += bytesToRead;
        return bytesToRead;
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new org.apache.thrift.transport.TTransportException(e);
      }
    }

    @Override
    public void write(byte[] buffer, int offset, int length) {
      writes.write(buffer, offset, length);
    }

    @Override
    public void flush() {}

    @Override
    public org.apache.thrift.TConfiguration getConfiguration() {
      return null;
    }

    @Override
    public void updateKnownMessageSize(long size) {}

    @Override
    public void checkReadBytesAvailable(long numBytes) {}
  }

  private static class TestMemoryControl implements AutoResizingBufferMemoryControl {

    private final AtomicLong usedMemoryInBytes = new AtomicLong();
    private volatile long limit = Long.MAX_VALUE;

    @Override
    public boolean allocate(long sizeInBytes) {
      while (true) {
        long currentUsedMemory = usedMemoryInBytes.get();
        long nextUsedMemory = currentUsedMemory + sizeInBytes;
        if (nextUsedMemory > limit) {
          return false;
        }
        if (usedMemoryInBytes.compareAndSet(currentUsedMemory, nextUsedMemory)) {
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

    private void setLimit(long limit) {
      this.limit = limit;
    }
  }
}
