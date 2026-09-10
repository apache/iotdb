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
      Assert.assertTrue(e.getMessage().contains("100"));
      Assert.assertTrue(e.getMessage().contains("5"));
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

    private long getUsedMemoryInBytes() {
      return usedMemoryInBytes.get();
    }

    private void setLimit(long limit) {
      this.limit = limit;
    }
  }
}
