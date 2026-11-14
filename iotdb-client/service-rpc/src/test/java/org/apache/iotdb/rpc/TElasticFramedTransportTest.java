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

import org.apache.thrift.transport.TByteBuffer;
import org.apache.thrift.transport.TMemoryBuffer;
import org.apache.thrift.transport.TTransportException;
import org.junit.Test;

import java.lang.reflect.Field;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Random;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class TElasticFramedTransportTest {

  @Test
  public void testRejectOversizedFrameBeforeGrowingBuffer() throws Exception {
    for (int prefixLength : new int[] {0, 64}) {
      boolean[] closed = {false};
      TMemoryBuffer underlying =
          new TMemoryBuffer(0) {
            @Override
            public void close() {
              closed[0] = true;
              super.close();
            }
          };
      TElasticFramedTransport transport = new TElasticFramedTransport(underlying, 8, 64, false);
      transport.write(new byte[prefixLength]);
      byte[] buffer = transport.writeBuffer.getBuffer();
      try {
        transport.write(new byte[65 - prefixLength]);
        fail("An oversized frame must fail during serialization, before flush");
      } catch (TTransportException e) {
        assertEquals(TTransportException.CORRUPTED_DATA, e.getType());
        assertTrue(e.getMessage(), e.getMessage().contains("at least 65 bytes"));
        assertTrue(
            e.getMessage(),
            e.getMessage().contains("configured Thrift max frame size of 64 bytes"));
        assertFalse(e.getMessage(), e.getMessage().contains("dn_thrift_max_frame_size"));
        assertSame(buffer, transport.writeBuffer.getBuffer());
        assertEquals(prefixLength, transport.writeBuffer.getPos());
        assertEquals(0, underlying.length());
        assertTrue(closed[0]);
      }
    }
  }

  @Test
  public void testIncrementalGrowthDoesNotExceedFrameLimit() throws Exception {
    int limit = 1024 * 1024;
    try (TElasticFramedTransport transport =
        new TElasticFramedTransport(new TMemoryBuffer(0), 1024, limit, false)) {
      byte[] chunk = new byte[8];
      Arrays.fill(chunk, (byte) 42);
      for (int written = 0; written < limit; written += chunk.length) {
        transport.write(chunk);
        assertTrue(
            "Allocated " + transport.writeBuffer.getBuffer().length + " bytes for limit " + limit,
            transport.writeBuffer.getBuffer().length <= limit);
      }
      assertEquals(limit, transport.writeBuffer.getBuffer().length);
      assertEquals(42, transport.writeBuffer.getBuffer()[0]);
      assertEquals(42, transport.writeBuffer.getBuffer()[limit - 1]);
      TTransportException failure =
          assertThrows(TTransportException.class, () -> transport.write(chunk));
      assertTrue(failure.getMessage(), failure.getMessage().contains("at least 1048584 bytes"));
      assertEquals(limit, transport.writeBuffer.getBuffer().length);
    }
  }

  @Test
  public void testInitialWriteBufferRespectsFrameLimit() throws Exception {
    try (TElasticFramedTransport transport =
        new TElasticFramedTransport(new TMemoryBuffer(0), 1024, 64, false)) {
      assertEquals(64, transport.writeBuffer.getBuffer().length);
      byte[] payload = new byte[64];
      payload[63] = 42;
      for (int frame = 0; frame < 2; frame++) {
        transport.write(payload);
        transport.flush();
        byte[] actual = new byte[payload.length];
        transport.readAll(actual, 0, actual.length);
        assertArrayEquals(payload, actual);
        assertEquals(64, transport.writeBuffer.getBuffer().length);
      }
    }
  }

  @Test
  public void testFrameAtSizeLimit() throws Exception {
    TElasticFramedTransport transport =
        new TElasticFramedTransport(new TMemoryBuffer(0), 8, 64, false);
    byte[] payload = new byte[64];
    payload[63] = 42;
    for (int i = 0; i < payload.length; i += 8) {
      transport.write(payload, i, 8);
    }
    transport.flush();
    byte[] actual = new byte[payload.length];
    transport.readAll(actual, 0, actual.length);
    assertArrayEquals(payload, actual);
  }

  @Test
  public void testCompressedFrameLimitAppliesAfterCompression() throws Exception {
    TElasticFramedTransport transport =
        new TSnappyElasticFramedTransport(new TMemoryBuffer(0), 128, 64, false);
    byte[] payload = new byte[256];
    transport.write(payload);
    transport.flush();
    byte[] actual = new byte[payload.length];
    transport.readAll(actual, 0, actual.length);
    assertArrayEquals(payload, actual);
  }

  @Test
  public void testCompressedFrameLimitIsCheckedAfterBufferedWrites() throws Exception {
    boolean[] closed = {false};
    TMemoryBuffer underlying =
        new TMemoryBuffer(0) {
          @Override
          public void close() {
            closed[0] = true;
            super.close();
          }
        };
    TSnappyElasticFramedTransport transport =
        new TSnappyElasticFramedTransport(underlying, 8, 64, false);
    byte[] payload = new byte[256];
    transport.write(payload);
    transport.flush();
    byte[] actual = new byte[payload.length];
    transport.readAll(actual, 0, actual.length);
    assertArrayEquals(payload, actual);
    byte[] buffer = transport.writeBuffer.getBuffer();

    new Random(0).nextBytes(payload);
    transport.write(payload);
    assertSame(buffer, transport.writeBuffer.getBuffer());
    TTransportException failure = assertThrows(TTransportException.class, transport::flush);
    assertEquals(TTransportException.CORRUPTED_DATA, failure.getType());
    assertTrue(closed[0]);
  }

  @Test
  public void testBufferedWriteCannotBypassArrayLimitOnOverflow() throws Exception {
    for (boolean compressed : new boolean[] {false, true}) {
      boolean[] closed = {false};
      TMemoryBuffer underlying =
          new TMemoryBuffer(0) {
            @Override
            public void close() {
              closed[0] = true;
              super.close();
            }
          };
      TElasticFramedTransport transport =
          compressed
              ? new TSnappyElasticFramedTransport(underlying, 8, 64, false)
              : new TElasticFramedTransport(underlying, 8, Integer.MAX_VALUE, false);
      Field position = AutoScalingBufferWriteTransport.class.getDeclaredField("pos");
      position.setAccessible(true);
      int oldPosition = AutoResizingBuffer.MAX_ARRAY_SIZE - 4;
      position.setInt(transport.writeBuffer, oldPosition);
      final byte[] buffer = transport.writeBuffer.getBuffer();
      TTransportException failure =
          assertThrows(TTransportException.class, () -> transport.write(new byte[16]));
      assertEquals(TTransportException.CORRUPTED_DATA, failure.getType());
      assertTrue(failure.getMessage(), failure.getMessage().contains("maximum Java array size"));
      assertFalse(failure.getMessage(), failure.getMessage().contains("dn_thrift_max_frame_size"));
      assertSame(buffer, transport.writeBuffer.getBuffer());
      assertEquals(oldPosition, transport.writeBuffer.getPos());
      assertTrue(closed[0]);
    }
  }

  @Test
  public void testInvalidCompressionBufferSizesCloseTransport() throws Exception {
    for (int size : new int[] {-1, Integer.MAX_VALUE}) {
      for (boolean read : new boolean[] {false, true}) {
        boolean[] closed = {false};
        TMemoryBuffer underlying =
            new TMemoryBuffer(0) {
              @Override
              public void close() {
                closed[0] = true;
                super.close();
              }
            };
        TSnappyElasticFramedTransport transport =
            new TSnappyElasticFramedTransport(underlying, 8, 64, false) {
              @Override
              protected int maxCompressedLength(int length) {
                // Snappy's bound overflows int for large, otherwise valid input lengths.
                return size;
              }

              @Override
              protected int uncompressedLength(byte[] bytes, int offset, int length) {
                return size;
              }
            };
        TTransportException failure;
        if (read) {
          underlying.write(new byte[] {0, 0, 0, 1, 0});
          failure =
              assertThrows(TTransportException.class, () -> transport.read(new byte[1], 0, 1));
        } else {
          transport.write(new byte[1]);
          failure = assertThrows(TTransportException.class, transport::flush);
        }
        assertEquals(TTransportException.CORRUPTED_DATA, failure.getType());
        assertTrue(closed[0]);
      }
    }
  }

  @Test
  public void testSingularSize() {

    try {
      TElasticFramedTransport transport =
          new TElasticFramedTransport(
              new TByteBuffer(
                  ByteBuffer.wrap("GET 127.0.0.1 HTTP/1.1".getBytes(StandardCharsets.UTF_8))),
              128 * 1024 * 1024,
              512 * 1024 * 1024,
              false);
      transport.open();
      transport.read(ByteBuffer.allocate(4096));
      fail("Exception expected");
    } catch (TTransportException e) {
      assertEquals(
          "Singular frame size (1195725856) detected, you may be sending HTTP GET/POST requests to the Thrift-RPC port, please confirm that you are using the right port",
          e.getMessage());
    }

    try {
      TElasticFramedTransport transport =
          new TElasticFramedTransport(
              new TByteBuffer(
                  ByteBuffer.wrap("POST 127.0.0.1 HTTP/1.1".getBytes(StandardCharsets.UTF_8))),
              128 * 1024 * 1024,
              512 * 1024 * 1024,
              false);
      transport.open();
      transport.read(ByteBuffer.allocate(4096));
      fail("Exception expected");
    } catch (TTransportException e) {
      assertEquals(
          "Singular frame size (1347375956) detected, you may be sending HTTP GET/POST requests to the Thrift-RPC port, please confirm that you are using the right port",
          e.getMessage());
    }
  }
}
