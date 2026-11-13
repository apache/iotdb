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

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;
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
        assertTrue(e.getMessage().contains("Frame size (65)"));
        assertSame(buffer, transport.writeBuffer.getBuffer());
        assertEquals(prefixLength, transport.writeBuffer.getPos());
        assertEquals(0, underlying.length());
        assertTrue(closed[0]);
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
        new TSnappyElasticFramedTransport(new TMemoryBuffer(0), 8, 64, false);
    byte[] payload = new byte[256];
    transport.write(payload);
    transport.flush();
    byte[] actual = new byte[payload.length];
    transport.readAll(actual, 0, actual.length);
    assertArrayEquals(payload, actual);
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
