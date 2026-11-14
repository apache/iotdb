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

import org.apache.thrift.TConfiguration;
import org.apache.thrift.transport.TByteBuffer;
import org.apache.thrift.transport.TMemoryBuffer;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransportException;
import org.junit.Test;

import java.lang.reflect.Field;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
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
  public void testEarlyFrameLimitErrorKeepsRemoteEndpoint() throws Exception {
    assertEarlyWriteRejectionKeepsRemoteEndpoint(false);
  }

  @Test
  public void testEarlyArrayLimitErrorKeepsRemoteEndpoint() throws Exception {
    assertEarlyWriteRejectionKeepsRemoteEndpoint(true);
  }

  private void assertEarlyWriteRejectionKeepsRemoteEndpoint(boolean arrayLimit) throws Exception {
    try (ServerSocket server = new ServerSocket(0, 1, InetAddress.getLoopbackAddress());
        Socket client = new Socket(server.getInetAddress(), server.getLocalPort());
        Socket accepted = server.accept();
        TSocket underlying = new TSocket(client);
        TElasticFramedTransport transport =
            arrayLimit
                ? new TSnappyElasticFramedTransport(underlying, 8, 64, false)
                : new TElasticFramedTransport(underlying, 8, 64, false)) {
      String remoteEndpoint = client.getRemoteSocketAddress().toString();
      if (arrayLimit) {
        Field position = AutoScalingBufferWriteTransport.class.getDeclaredField("pos");
        position.setAccessible(true);
        position.setInt(transport.writeBuffer, AutoResizingBuffer.MAX_ARRAY_SIZE - 4);
      } else {
        transport.write(new byte[64]);
      }

      TTransportException failure =
          assertThrows(TTransportException.class, () -> transport.write(new byte[16]));
      assertEquals(TTransportException.CORRUPTED_DATA, failure.getType());
      assertTrue(failure.getMessage(), failure.getMessage().contains(" to " + remoteEndpoint));
      assertTrue(client.isClosed());
      accepted.setSoTimeout(1000);
      assertEquals(-1, accepted.getInputStream().read());
    }
  }

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
        assertEquals(0, transport.writeBuffer.getPos());
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
              : new TElasticFramedTransport(
                  underlying, 8, AutoResizingBuffer.MAX_ARRAY_SIZE, false);
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
      assertEquals(0, transport.writeBuffer.getPos());
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
  public void testReadFramesResetMessageSize() throws TTransportException {
    byte[] firstFrame = {1, 2};
    byte[] secondFrame = {3, 4, 5, 6, 7, 8};
    ByteBuffer framedData =
        ByteBuffer.allocate(8 + firstFrame.length + secondFrame.length)
            .putInt(firstFrame.length)
            .put(firstFrame)
            .putInt(secondFrame.length)
            .put(secondFrame)
            .flip();
    TConfiguration configuration =
        TConfiguration.custom().setMaxMessageSize(14).setMaxFrameSize(10).build();
    TMemoryBuffer underlying = new BudgetConsumingMemoryBuffer(configuration, 14);
    underlying.write(framedData.array());
    TElasticFramedTransport transport =
        new TElasticFramedTransport(underlying, 4, configuration.getMaxFrameSize(), false);

    byte[] actualFirstFrame = new byte[firstFrame.length];
    transport.readAll(actualFirstFrame, 0, actualFirstFrame.length);
    assertArrayEquals(firstFrame, actualFirstFrame);

    byte[] actualSecondFrame = new byte[secondFrame.length];
    transport.readAll(actualSecondFrame, 0, actualSecondFrame.length);
    assertArrayEquals(secondFrame, actualSecondFrame);
  }

  @Test
  public void testReadCompressedFramesResetMessageSize() throws TTransportException {
    byte[] firstFrame = {1, 2};
    byte[] secondFrame = {3, 4, 5, 6, 7, 8};
    TMemoryBuffer wire = new TMemoryBuffer(128);
    TSnappyElasticFramedTransport output = new TSnappyElasticFramedTransport(wire, 4, 128, false);
    output.write(firstFrame);
    output.flush();
    output.write(secondFrame);
    output.flush();

    byte[] framedData = Arrays.copyOf(wire.getArray(), wire.length());
    ByteBuffer frameSizes = ByteBuffer.wrap(framedData);
    int firstCompressedSize = frameSizes.getInt();
    frameSizes.position(Integer.BYTES + firstCompressedSize);
    int secondCompressedSize = frameSizes.getInt();
    int maxFrameSize = Math.max(firstCompressedSize, secondCompressedSize);
    TConfiguration configuration =
        TConfiguration.custom()
            .setMaxMessageSize(maxFrameSize)
            .setMaxFrameSize(maxFrameSize)
            .build();
    TMemoryBuffer underlying = new BudgetConsumingMemoryBuffer(configuration, maxFrameSize);
    underlying.write(framedData);
    TSnappyElasticFramedTransport input =
        new TSnappyElasticFramedTransport(underlying, 4, configuration.getMaxFrameSize(), false);
    assertEquals(Integer.BYTES + maxFrameSize, configuration.getMaxMessageSize());

    byte[] actualFirstFrame = new byte[firstFrame.length];
    input.readAll(actualFirstFrame, 0, actualFirstFrame.length);
    assertArrayEquals(firstFrame, actualFirstFrame);

    byte[] actualSecondFrame = new byte[secondFrame.length];
    input.readAll(actualSecondFrame, 0, actualSecondFrame.length);
    assertArrayEquals(secondFrame, actualSecondFrame);
  }

  @Test
  public void testFrameSizeAlignsUnderlyingMessageSize() throws TTransportException {
    byte[] frame = {1, 2, 3, 4, 5, 6, 7, 8};
    ByteBuffer framedData =
        ByteBuffer.allocate(Integer.BYTES + frame.length).putInt(frame.length).put(frame).flip();
    TConfiguration configuration =
        TConfiguration.custom().setMaxMessageSize(6).setMaxFrameSize(frame.length).build();
    TMemoryBuffer underlying = new BudgetConsumingMemoryBuffer(configuration, 6);
    underlying.write(framedData.array());
    TElasticFramedTransport transport =
        new TElasticFramedTransport(underlying, 4, frame.length, false);

    assertEquals(Integer.BYTES + frame.length, configuration.getMaxMessageSize());
    byte[] actualFrame = new byte[frame.length];
    transport.readAll(actualFrame, 0, actualFrame.length);
    assertArrayEquals(frame, actualFrame);
  }

  private static class BudgetConsumingMemoryBuffer extends TMemoryBuffer {

    private BudgetConsumingMemoryBuffer(TConfiguration configuration, int size)
        throws TTransportException {
      super(configuration, size);
    }

    @Override
    public int read(byte[] buffer, int offset, int length) throws TTransportException {
      int bytesRead = super.read(buffer, offset, length);
      countConsumedMessageBytes(bytesRead);
      return bytesRead;
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

    try {
      TElasticFramedTransport transport =
          new TElasticFramedTransport(
              new TByteBuffer(ByteBuffer.wrap(getTypicalTLSClientHelloByteArray())),
              128 * 1024 * 1024,
              512 * 1024 * 1024,
              false);
      transport.open();
      transport.read(ByteBuffer.allocate(4096));
      fail("Exception expected");
    } catch (TTransportException e) {
      assertEquals(
          "Singular frame size (369296129) detected, you may be sending TLS ClientHello requests to the Non-SSL Thrift-RPC port, please confirm that you are using the right configuration",
          e.getMessage());
    }
  }

  private static byte[] getTypicalTLSClientHelloByteArray() {
    String clientHelloHex =
        "16030301B3010001AF0303CEC349A4962AFCE0390D4E33D24050D1BF6B1CA63B190A25"
            + "BCFB83D87A3E352C20187B978A0EB2F554EC0E41A4CA34B850B2CE472EAB7B3F58443DE7CDBE901412004A13"
            + "0213011303C02CC02BCCA9C030CCA8C02F009FCCAA00A3009E00A2C024C028C023C027006B006A00670040C0"
            + "0AC014C009C0130039003800330032009D009C003D003C0035002F00FF0100011C000500050100000000000A"
            + "00160014001D001700180019001E01000101010201030104000B000201000011000900070200040000000000"
            + "17000000230000000D002C002A040305030603080708080804080508060809080A080B040105010601040203"
            + "0303010302020302010202002B00050403040303002D000201010032002C002A040305030603080708080804"
            + "080508060809080A080B04010501060104020303030103020203020102020033006B0069001D002097B98B24"
            + "B9A97EB7C913BDB8B363E79C9D47935264B2CF83BF422571FBD41C360017004104FC839279D372DCB60680D2"
            + "81B3DC8D3B88F6231A880A3650FD45322A79C9EA14CE073C0B71FC0AF9683BFC6DA95EB23B4122EC9E09EB7F"
            + "88FF565415DDF44367";
    byte[] bytes = new byte[clientHelloHex.length() / 2];
    for (int i = 0; i < clientHelloHex.length(); i += 2) {
      int value = Integer.parseInt(clientHelloHex.substring(i, i + 2), 16);
      bytes[i / 2] = (byte) value;
    }
    return bytes;
  }
}
