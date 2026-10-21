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

import org.apache.thrift.transport.TMemoryBuffer;
import org.apache.thrift.transport.TTransportException;
import org.junit.After;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.lang.reflect.Field;
import java.util.Arrays;
import java.util.Random;
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
  public void testGrowToRequiredCapacityWhenPreferredCapacityExceedsBudget() throws Exception {
    memoryControl.setLimit(120);
    AutoResizingBufferMemoryManager.setMemoryControl(memoryControl);
    AutoResizingBufferMemoryManager.setMemoryAllocateRetryIntervalInMs(1);

    try (AutoResizingBuffer buffer = new AutoResizingBuffer(100)) {
      buffer.array()[99] = 42;
      buffer.resizeIfNecessary(110);

      Assert.assertEquals(110, buffer.array().length);
      Assert.assertEquals(110, memoryControl.getUsedMemoryInBytes());
      Assert.assertEquals(42, buffer.array()[99]);
      Assert.assertEquals(2, AutoResizingBufferMemoryManager.getMemoryAllocationCount());
      Assert.assertEquals(0, AutoResizingBufferMemoryManager.getMemoryAllocationFailureCount());
    }
    Assert.assertEquals(0, memoryControl.getUsedMemoryInBytes());
  }

  @Test
  public void testKeepPreferredGrowthWhenBudgetAllows() throws Exception {
    AutoResizingBufferMemoryManager.setMemoryControl(memoryControl);

    try (AutoResizingBuffer buffer = new AutoResizingBuffer(100)) {
      buffer.resizeIfNecessary(110);
      Assert.assertEquals(150, buffer.array().length);
      Assert.assertEquals(150, memoryControl.getUsedMemoryInBytes());
      Assert.assertEquals(2, AutoResizingBufferMemoryManager.getMemoryAllocationCount());
    }
    Assert.assertEquals(0, memoryControl.getUsedMemoryInBytes());
  }

  @Test
  public void testShrinkForRequestsAtOrBelowInitialCapacity() throws Exception {
    AutoResizingBufferMemoryManager.setMemoryControl(memoryControl);

    for (int requestSize : new int[] {0, 1, 100}) {
      try (AutoResizingBuffer buffer = new AutoResizingBuffer(100)) {
        buffer.resizeIfNecessary(1000);
        buffer.array()[99] = 42;
        for (int i = 0; i <= RpcUtils.MAX_BUFFER_OVERSIZE_TIME; i++) {
          buffer.resizeIfNecessary(requestSize);
        }

        Assert.assertEquals(550, buffer.array().length);
        Assert.assertEquals(550, memoryControl.getUsedMemoryInBytes());
        Assert.assertEquals(42, buffer.array()[99]);
      }
      Assert.assertEquals(0, memoryControl.getUsedMemoryInBytes());
    }
  }

  @Test
  public void testSmallFramesReleaseCapacityForAnotherConnection() throws Exception {
    memoryControl.setLimit(800);
    AutoResizingBufferMemoryManager.setMemoryControl(memoryControl);
    AutoResizingBufferMemoryManager.setMemoryAllocateRetryIntervalInMs(1);

    AutoScalingBufferReadTransport first = new AutoScalingBufferReadTransport(100);
    AutoScalingBufferReadTransport second = new AutoScalingBufferReadTransport(100);
    try {
      TMemoryBuffer input = new TMemoryBuffer(600);
      byte[] payload = new byte[600];
      payload[0] = 42;
      input.write(payload);
      first.fill(input, payload.length);
      Assert.assertEquals(700, memoryControl.getUsedMemoryInBytes());
      first.consumeBuffer(payload.length);

      for (int i = 0; i <= RpcUtils.MAX_BUFFER_OVERSIZE_TIME; i++) {
        input.write(new byte[] {42});
        first.fill(input, 1);
        first.consumeBuffer(1);
      }
      Assert.assertEquals(450, memoryControl.getUsedMemoryInBytes());

      input.write(payload, 0, 400);
      second.fill(input, 400);
      Assert.assertEquals(750, memoryControl.getUsedMemoryInBytes());
      Assert.assertEquals(42, first.getBuffer()[0]);
      Assert.assertEquals(42, second.getBuffer()[0]);
      Assert.assertEquals(400, second.getBytesRemainingInBuffer());
    } finally {
      first.close();
      second.close();
    }
    Assert.assertEquals(0, memoryControl.getUsedMemoryInBytes());
  }

  @Test
  public void testFramedTransportsShrinkAfterLargeFrames() throws Exception {
    AutoResizingBufferMemoryManager.setMemoryControl(memoryControl);

    for (boolean compressed : new boolean[] {false, true}) {
      TMemoryBuffer underlying = new TMemoryBuffer(0);
      try (TElasticFramedTransport transport =
          compressed
              ? new TSnappyElasticFramedTransport(underlying, 100, 2048, true)
              : new TElasticFramedTransport(underlying, 100, 2048, true)) {
        byte[] payload = new byte[1000];
        payload[999] = 42;
        transport.write(payload);
        transport.flush();
        byte[] actual = new byte[payload.length];
        transport.readAll(actual, 0, actual.length);
        Assert.assertArrayEquals(payload, actual);
        long largeFrameMemory = memoryControl.getUsedMemoryInBytes();
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
        Assert.assertTrue(memoryControl.getUsedMemoryInBytes() < largeFrameMemory);

        byte[] shrunkBuffer = transport.writeBuffer.getBuffer();
        for (int i = 0; i <= RpcUtils.MAX_BUFFER_OVERSIZE_TIME; i++) {
          transport.write(new byte[] {42});
          transport.flush();
          transport.readAll(smallFrame, 0, 1);
          Assert.assertEquals(42, smallFrame[0]);
        }
        Assert.assertSame(shrunkBuffer, transport.writeBuffer.getBuffer());
      }
      Assert.assertEquals(0, memoryControl.getUsedMemoryInBytes());
    }
  }

  @Test
  public void testRepeatedLargeFramesKeepWriteCapacity() throws Exception {
    AutoResizingBufferMemoryManager.setMemoryControl(memoryControl);

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
          long allocationCount = AutoResizingBufferMemoryManager.getMemoryAllocationCount();
          transport.write(payload);
          transport.flush();
          Assert.assertSame(writeBuffer, transport.writeBuffer.getBuffer());
          Assert.assertEquals(
              allocationCount, AutoResizingBufferMemoryManager.getMemoryAllocationCount());
          transport.readAll(actual, 0, actual.length);
          Assert.assertArrayEquals(payload, actual);
        }
      }
      Assert.assertEquals(0, memoryControl.getUsedMemoryInBytes());
    }
  }

  @Test
  public void testSmallWritesDoNotShrinkAnUnfinishedFrame() throws Exception {
    AutoResizingBufferMemoryManager.setMemoryControl(memoryControl);

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

        int prefixLength = RpcUtils.MAX_BUFFER_OVERSIZE_TIME + 1;
        for (int i = 0; i < prefixLength; i++) {
          transport.write(payload, i, 1);
          Assert.assertSame(writeBuffer, transport.writeBuffer.getBuffer());
          Assert.assertEquals(i + 1, transport.writeBuffer.getPos());
        }
        transport.write(payload, prefixLength, payload.length - prefixLength);
        Assert.assertSame(writeBuffer, transport.writeBuffer.getBuffer());
        transport.flush();
        transport.readAll(actual, 0, actual.length);
        Assert.assertArrayEquals(payload, actual);
      }
      Assert.assertEquals(0, memoryControl.getUsedMemoryInBytes());
    }
  }

  @Test
  public void testLargeFrameReuseDoesNotStarveAnotherConnection() throws Exception {
    AutoResizingBufferMemoryManager.setMemoryControl(memoryControl);
    AutoResizingBufferMemoryManager.setMemoryAllocateRetryIntervalInMs(1);

    for (boolean compressed : new boolean[] {false, true}) {
      memoryControl.setLimit(Long.MAX_VALUE);
      try (TElasticFramedTransport transport = newFramedTransport(compressed);
          AutoScalingBufferReadTransport anotherConnection =
              new AutoScalingBufferReadTransport(100)) {
        byte[] payload = new byte[1000];
        new Random(0).nextBytes(payload);
        transport.write(payload);
        transport.flush();
        byte[] writeBuffer = transport.writeBuffer.getBuffer();
        long retainedMemory = memoryControl.getUsedMemoryInBytes();
        // Only leave enough quota for the other connection to grow from 100 to 200 bytes.
        memoryControl.setLimit(retainedMemory + 100);
        for (int i = 0; i < 3 * (RpcUtils.MAX_BUFFER_OVERSIZE_TIME + 1); i++) {
          setLastShrinkTime(transport.writeBuffer, 0);
          transport.write(payload, 0, 600);
          transport.write(payload, 600, 400);
          transport.flush();
          Assert.assertSame(writeBuffer, transport.writeBuffer.getBuffer());
        }
        transport.write(payload, 0, 600);
        transport.write(payload, 600, 300);
        Assert.assertSame(writeBuffer, transport.writeBuffer.getBuffer());
        TMemoryBuffer input = new TMemoryBuffer(0);
        input.write(payload, 0, 200);
        anotherConnection.fill(input, 200);

        Assert.assertEquals(0, AutoResizingBufferMemoryManager.getMemoryAllocationFailureCount());
        Assert.assertEquals(retainedMemory + 100, memoryControl.getUsedMemoryInBytes());
        Assert.assertArrayEquals(
            Arrays.copyOf(payload, 200), Arrays.copyOf(anotherConnection.getBuffer(), 200));
        Assert.assertArrayEquals(
            Arrays.copyOf(payload, 900), Arrays.copyOf(transport.writeBuffer.getBuffer(), 900));
      }
      Assert.assertEquals(0, memoryControl.getUsedMemoryInBytes());
    }
  }

  @Test
  public void testFailedIncrementalGrowthPreservesFrameAndAccounting() throws Exception {
    memoryControl.setLimit(120);
    AutoResizingBufferMemoryManager.setMemoryControl(memoryControl);
    AutoResizingBufferMemoryManager.setMemoryAllocateRetryIntervalInMs(1);

    try (AutoScalingBufferWriteTransport transport = new AutoScalingBufferWriteTransport(100)) {
      byte[] payload = new byte[130];
      new Random(0).nextBytes(payload);
      transport.write(payload, 0, 90);
      byte[] originalBuffer = transport.getBuffer();
      try {
        transport.write(payload, 90, 40);
        Assert.fail("Expected TTransportException");
      } catch (TTransportException expected) {
        Assert.assertSame(originalBuffer, transport.getBuffer());
        Assert.assertEquals(90, transport.getPos());
        Assert.assertEquals(100, memoryControl.getUsedMemoryInBytes());
        Assert.assertArrayEquals(
            Arrays.copyOf(payload, 90), Arrays.copyOf(transport.getBuffer(), 90));
      }

      memoryControl.setLimit(130);
      transport.write(payload, 90, 40);
      Assert.assertEquals(130, transport.getBuffer().length);
      Assert.assertEquals(130, transport.getPos());
      Assert.assertEquals(130, memoryControl.getUsedMemoryInBytes());
      Assert.assertArrayEquals(payload, transport.getBuffer());
      Assert.assertEquals(1, AutoResizingBufferMemoryManager.getMemoryAllocationFailureCount());
      transport.close();
      Assert.assertEquals(0, memoryControl.getUsedMemoryInBytes());
    }
    Assert.assertEquals(0, memoryControl.getUsedMemoryInBytes());
  }

  @Test
  public void testConcurrentReadBuffersEnforceSharedBudget() throws Exception {
    memoryControl.setLimit(1000);
    AutoResizingBufferMemoryManager.setMemoryControl(memoryControl);
    AutoResizingBufferMemoryManager.setMemoryAllocateRetryIntervalInMs(1);

    AutoScalingBufferReadTransport first = new AutoScalingBufferReadTransport(100);
    AutoScalingBufferReadTransport second = new AutoScalingBufferReadTransport(100);
    AutoScalingBufferReadTransport third = new AutoScalingBufferReadTransport(100);
    try {
      byte[] payload = new byte[400];
      payload[399] = 42;
      TMemoryBuffer input = new TMemoryBuffer(0);
      input.write(payload);
      first.fill(input, payload.length);
      input.write(payload);
      second.fill(input, payload.length);
      Assert.assertEquals(900, memoryControl.getUsedMemoryInBytes());

      input.write(payload);
      try {
        third.fill(input, payload.length);
        Assert.fail("Expected TTransportException");
      } catch (TTransportException expected) {
        Assert.assertEquals(900, memoryControl.getUsedMemoryInBytes());
        Assert.assertEquals(1, AutoResizingBufferMemoryManager.getMemoryAllocationFailureCount());
      }

      first.close();
      third.fill(input, payload.length);
      Assert.assertEquals(800, memoryControl.getUsedMemoryInBytes());
      Assert.assertEquals(42, second.getBuffer()[399]);
      Assert.assertEquals(42, third.getBuffer()[399]);
    } finally {
      first.close();
      second.close();
      third.close();
    }
    Assert.assertEquals(0, memoryControl.getUsedMemoryInBytes());
  }

  @Test
  public void testThrowIOExceptionWhenMemoryIsInsufficient() throws Exception {
    memoryControl.setLimit(120);
    AutoResizingBufferMemoryManager.setMemoryControl(memoryControl);
    AutoResizingBufferMemoryManager.setMemoryAllocateRetryIntervalInMs(1);

    AutoResizingBuffer buffer = new AutoResizingBuffer(100);
    buffer.array()[99] = 42;
    try {
      buffer.resizeIfNecessary(200);
      Assert.fail("Expected IOException");
    } catch (IOException e) {
      Assert.assertTrue(e.getMessage().contains("100"));
      Assert.assertTrue(e.getMessage().contains("5"));
    } finally {
      Assert.assertEquals(100, buffer.array().length);
      Assert.assertEquals(42, buffer.array()[99]);
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

    private long getUsedMemoryInBytes() {
      return usedMemoryInBytes.get();
    }

    private void setLimit(long limit) {
      this.limit = limit;
    }
  }
}
