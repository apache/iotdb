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

import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TCompactProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TMemoryBuffer;
import org.apache.thrift.transport.TTransportException;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.lang.reflect.Field;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Random;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

public class TCompressedElasticFramedTransportTest {

  private static final int INITIAL_CAPACITY = 32;
  private static final int BUFFER_THRESHOLD = 256;
  private static final int MAX_FRAME_SIZE = 4096;

  private final AtomicLong usedMemory = new AtomicLong();

  @Before
  public void setUp() {
    AutoResizingBufferMemoryManager.setMemoryControl(
        new AutoResizingBufferMemoryControl() {
          @Override
          public boolean allocate(long sizeInBytes) {
            usedMemory.addAndGet(sizeInBytes);
            return true;
          }

          @Override
          public void release(long sizeInBytes) {
            usedMemory.addAndGet(-sizeInBytes);
          }
        });
  }

  @After
  public void tearDown() {
    AutoResizingBufferMemoryManager.resetMemoryControl();
  }

  // Grow all four reusable buffers, then send an incompressible large frame in pieces. The large
  // frame must round-trip while all reusable buffers return to their initial, accounted capacities.
  @Test
  public void testLargeFrameResetsAllBuffersAndSmallFramesResumeReuse() throws Exception {
    TSnappyElasticFramedTransport.Factory factory =
        new TSnappyElasticFramedTransport.Factory(
            INITIAL_CAPACITY, MAX_FRAME_SIZE, false, BUFFER_THRESHOLD);
    try (TSnappyElasticFramedTransport transport =
        (TSnappyElasticFramedTransport) factory.getTransport(new TMemoryBuffer(0))) {
      roundTrip(transport, randomBytes(128));
      for (AutoResizingBuffer buffer : reusableBuffers(transport)) {
        assertTrue(buffer.array().length > INITIAL_CAPACITY);
      }

      byte[] payload = randomBytes(1024);
      transport.write(payload, 0, 200);
      transport.write(payload, 200, 100);
      transport.write(payload, 300, payload.length - 300);
      assertEquals(INITIAL_CAPACITY, reusableBuffer(transport.writeBuffer).array().length);
      transport.flush();
      assertEquals(INITIAL_CAPACITY, transport.writeBuffer.getBuffer().length);
      assertEquals(
          INITIAL_CAPACITY, compressionBuffer(transport, "writeCompressBuffer").array().length);

      byte[] actual = new byte[payload.length];
      transport.readAll(actual, 0, 100);
      assertEquals(payload.length - 100, transport.getBytesRemainingInBuffer());
      assertEquals(payload.length, transport.getBuffer().length);
      for (AutoResizingBuffer buffer : reusableBuffers(transport)) {
        assertEquals(INITIAL_CAPACITY, buffer.array().length);
      }
      assertEquals(4L * INITIAL_CAPACITY, usedMemory.get());
      transport.readAll(actual, 100, actual.length - 100);
      assertArrayEquals(payload, actual);
      assertEquals(INITIAL_CAPACITY, transport.getBuffer().length);

      roundTrip(transport, randomBytes(64));
      for (AutoResizingBuffer buffer : reusableBuffers(transport)) {
        assertTrue(buffer.array().length > INITIAL_CAPACITY);
      }
    }
    assertEquals(0, usedMemory.get());
  }

  // Exactly the threshold still uses the reusable read buffer. One extra byte selects temporary
  // storage, preserves partial reads, and releases that storage when the last byte is consumed.
  @Test
  public void testReadThresholdBoundaryAndTemporaryBufferLifetime() throws Exception {
    try (AutoScalingBufferReadTransport input =
        new AutoScalingBufferReadTransport(INITIAL_CAPACITY, BUFFER_THRESHOLD)) {
      TMemoryBuffer wire = new TMemoryBuffer(0);
      byte[] payload = randomBytes(BUFFER_THRESHOLD);
      wire.write(payload);
      input.fill(wire, payload.length);
      assertSame(reusableBuffer(input).array(), input.getBuffer());
      assertEquals(BUFFER_THRESHOLD, usedMemory.get());
      byte[] actual = new byte[payload.length];
      input.readAll(actual, 0, actual.length);
      assertArrayEquals(payload, actual);

      payload = randomBytes(BUFFER_THRESHOLD + 1);
      wire.write(payload);
      input.fill(wire, payload.length);
      assertEquals(payload.length, input.getBuffer().length);
      assertEquals(INITIAL_CAPACITY, reusableBuffer(input).array().length);
      assertEquals(INITIAL_CAPACITY, usedMemory.get());
      actual = new byte[payload.length];
      input.readAll(actual, 0, 100);
      input.readAll(actual, 100, actual.length - 100);
      assertArrayEquals(payload, actual);
      assertEquals(0, input.getBytesRemainingInBuffer());
      assertEquals(INITIAL_CAPACITY, input.getBuffer().length);
    }
    assertEquals(0, usedMemory.get());
  }

  // A frame crosses the threshold after an exact-boundary prefix, then grows through more small
  // writes. All bytes must survive both transitions, and reset must release the temporary buffer.
  @Test
  public void testIncrementalWritesPreservePrefixAcrossThreshold() throws Exception {
    try (AutoScalingBufferWriteTransport output =
        new AutoScalingBufferWriteTransport(INITIAL_CAPACITY, BUFFER_THRESHOLD)) {
      byte[] payload = randomBytes(1024);
      output.write(payload, 0, BUFFER_THRESHOLD);
      assertSame(reusableBuffer(output).array(), output.getBuffer());
      assertEquals(BUFFER_THRESHOLD, usedMemory.get());
      for (int i = BUFFER_THRESHOLD; i < payload.length; i++) {
        output.write(payload, i, 1);
      }
      assertEquals(payload.length, output.getPos());
      assertArrayEquals(payload, Arrays.copyOf(output.getBuffer(), output.getPos()));
      assertEquals(INITIAL_CAPACITY, reusableBuffer(output).array().length);
      assertEquals(INITIAL_CAPACITY, usedMemory.get());

      output.reset();
      assertEquals(0, output.getPos());
      assertEquals(INITIAL_CAPACITY, output.getBuffer().length);
      output.write(payload, 0, 64);
      assertSame(reusableBuffer(output).array(), output.getBuffer());
      assertArrayEquals(Arrays.copyOf(payload, 64), Arrays.copyOf(output.getBuffer(), 64));
    }
    assertEquals(0, usedMemory.get());
  }

  // A highly compressible payload has a small wire frame but a large uncompressed frame. Both
  // sizing paths must honor the threshold; zero and thresholds below the initial size also work.
  @Test
  public void testCompressibleFramesAndSmallThresholds() throws Exception {
    for (int threshold : new int[] {0, INITIAL_CAPACITY / 2, BUFFER_THRESHOLD}) {
      try (TSnappyElasticFramedTransport transport =
          new TSnappyElasticFramedTransport(
              new TMemoryBuffer(0), INITIAL_CAPACITY, MAX_FRAME_SIZE, true, threshold)) {
        roundTrip(transport, new byte[1024]);
        for (AutoResizingBuffer buffer : reusableBuffers(transport)) {
          assertEquals(INITIAL_CAPACITY, buffer.array().length);
        }
        assertEquals(4L * INITIAL_CAPACITY, usedMemory.get());
        roundTrip(transport, new byte[] {42});
      }
      assertEquals(0, usedMemory.get());
    }
  }

  // Binary and compact protocols, with and without binary copying, read successive large/small
  // frames. A previously returned binary value must remain valid after temporary storage is freed.
  @Test
  public void testProtocolsRetainBinaryValuesAcrossFrames() throws Exception {
    for (boolean compact : new boolean[] {false, true}) {
      for (boolean copyBinary : new boolean[] {false, true}) {
        try (TSnappyElasticFramedTransport transport =
            new TSnappyElasticFramedTransport(
                new TMemoryBuffer(0),
                INITIAL_CAPACITY,
                MAX_FRAME_SIZE,
                copyBinary,
                BUFFER_THRESHOLD)) {
          TProtocol protocol =
              compact ? new TCompactProtocol(transport) : new TBinaryProtocol(transport);
          byte[] payload = randomBytes(1024);
          protocol.writeBinary(ByteBuffer.wrap(payload));
          transport.flush();
          ByteBuffer firstValue = protocol.readBinary();
          protocol.writeBinary(ByteBuffer.wrap(new byte[] {42}));
          transport.flush();
          assertEquals(ByteBuffer.wrap(new byte[] {42}), protocol.readBinary());
          protocol.writeBinary(ByteBuffer.wrap(randomBytes(2048)));
          transport.flush();
          protocol.readBinary();
          assertEquals(ByteBuffer.wrap(payload), firstValue);
        }
        assertEquals(0, usedMemory.get());
      }
    }
  }

  // Temporary storage must not bypass the maximum wire frame size. A rejected flush closes the
  // transport and releases all four reusable allocations, including a pending temporary write.
  @Test
  public void testTemporaryFramesStillEnforceFrameLimit() throws Exception {
    try (TSnappyElasticFramedTransport transport =
        new TSnappyElasticFramedTransport(new TMemoryBuffer(0), INITIAL_CAPACITY, 128, true, 64)) {
      transport.write(randomBytes(1024));
      TTransportException exception = assertThrows(TTransportException.class, transport::flush);
      assertEquals(TTransportException.CORRUPTED_DATA, exception.getType());
      assertEquals(0, usedMemory.get());
      assertEquals(INITIAL_CAPACITY, transport.writeBuffer.getBuffer().length);
    }
    assertEquals(0, usedMemory.get());
  }

  // The original constructor keeps its reuse policy: even a large frame leaves grown reusable
  // buffers in place until the existing shrink policy runs, and close releases their accounting.
  @Test
  public void testOriginalConstructorPreservesReusePolicy() throws Exception {
    try (TSnappyElasticFramedTransport transport =
        new TSnappyElasticFramedTransport(
            new TMemoryBuffer(0), INITIAL_CAPACITY, MAX_FRAME_SIZE, true)) {
      roundTrip(transport, randomBytes(1024));
      for (AutoResizingBuffer buffer : reusableBuffers(transport)) {
        assertTrue(buffer.array().length > BUFFER_THRESHOLD);
      }
    }
    assertEquals(0, usedMemory.get());
  }

  private void roundTrip(TSnappyElasticFramedTransport transport, byte[] payload)
      throws TTransportException {
    transport.write(payload);
    transport.flush();
    byte[] actual = new byte[payload.length];
    transport.readAll(actual, 0, actual.length);
    assertArrayEquals(payload, actual);
  }

  private byte[] randomBytes(int size) {
    byte[] payload = new byte[size];
    new Random(0).nextBytes(payload);
    return payload;
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
}
