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
import org.junit.Assert;
import org.junit.Test;

import java.lang.reflect.Field;
import java.util.Random;

public class AutoResizingBufferTest {

  @Test
  public void testGrowthPreservesDataAndHeadroom() {
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
}
