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

package org.apache.iotdb.consensus.iot.logdispatcher;

import org.apache.iotdb.common.rpc.thrift.TConsensusGroupId;
import org.apache.iotdb.common.rpc.thrift.TConsensusGroupType;
import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.consensus.DataRegionId;
import org.apache.iotdb.commons.memory.AtomicLongMemoryBlock;
import org.apache.iotdb.commons.memory.IMemoryBlock;
import org.apache.iotdb.consensus.IStateMachine;
import org.apache.iotdb.consensus.common.request.ByteBufferConsensusRequest;
import org.apache.iotdb.consensus.common.request.IndexedConsensusRequest;
import org.apache.iotdb.consensus.config.IoTConsensusConfig;
import org.apache.iotdb.consensus.iot.IoTConsensus;
import org.apache.iotdb.consensus.iot.IoTConsensusServerImpl;
import org.apache.iotdb.consensus.iot.IoTConsensusServerMetrics;
import org.apache.iotdb.consensus.iot.service.IoTConsensusRPCServiceProcessor;
import org.apache.iotdb.consensus.iot.thrift.TLogCompressionType;
import org.apache.iotdb.consensus.iot.thrift.TLogEntry;
import org.apache.iotdb.consensus.iot.thrift.TSyncLogEntriesReq;
import org.apache.iotdb.consensus.iot.thrift.TSyncLogEntriesRes;

import org.apache.thrift.TDeserializer;
import org.apache.thrift.TSerializer;
import org.apache.tsfile.file.metadata.enums.CompressionType;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Random;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith(Parameterized.class)
public class LogCompressionTest {
  @Parameterized.Parameters(name = "{0}")
  public static Object[][] codecs() {
    return new Object[][] {
      {CompressionType.LZ4, TLogCompressionType.LZ4},
      {CompressionType.SNAPPY, TLogCompressionType.SNAPPY},
      {CompressionType.ZSTD, TLogCompressionType.ZSTD}
    };
  }

  @Parameterized.Parameter public CompressionType codec;

  @Parameterized.Parameter(1)
  public TLogCompressionType expectedWireCodec;

  private IMemoryBlock previous;
  private final IoTConsensusMemoryManager memory = IoTConsensusMemoryManager.getInstance();
  private static final int LIMIT = 8 * 1024 * 1024;

  @Before
  public void setUp() {
    previous = memory.getMemoryBlock();
    memory.setMemoryBlock(new AtomicLongMemoryBlock("compression-test", null, 64L * 1024 * 1024));
    memory.reset();
  }

  @After
  public void tearDown() {
    assertEquals(0, memory.getMemoryBlock().getUsedMemoryInBytes());
    memory.reset();
    memory.setMemoryBlock(previous);
  }

  private TSyncLogEntriesReq request() {
    byte[] compressible = new byte[16384];
    Arrays.fill(compressible, (byte) 42);
    for (int i = 0; i < 17; i++) compressible[i] = (byte) i;
    ByteBuffer sliced = ByteBuffer.wrap(compressible);
    sliced.position(17);
    sliced.limit(16000);
    ByteBuffer direct = ByteBuffer.allocateDirect(8192);
    while (direct.hasRemaining()) direct.put((byte) 7);
    direct.flip();
    byte[] random = new byte[8192];
    new Random(123).nextBytes(random);
    TLogEntry wal =
        new TLogEntry(
                Arrays.asList(
                    sliced,
                    direct.asReadOnlyBuffer(),
                    ByteBuffer.wrap(new byte[] {1, 2, 3}),
                    ByteBuffer.wrap(random)),
                1,
                true,
                50000)
            .setRoutingEpoch(9)
            .setPhysicalTime(12345);
    TLogEntry queue =
        new TLogEntry(
            Collections.singletonList(ByteBuffer.wrap(compressible).asReadOnlyBuffer()),
            2,
            false,
            16384);
    return new TSyncLogEntriesReq(
        3, new TConsensusGroupId(TConsensusGroupType.DataRegion, 5), Arrays.asList(wal, queue));
  }

  @Test
  public void testRoundTripThriftAndRetryOwnership() throws Exception {
    TSyncLogEntriesReq raw = request();
    TSyncLogEntriesReq snapshot = raw.deepCopy();
    for (int attempt = 0; attempt < 2; attempt++) {
      LogCompression.EncodedRequest encoded = LogCompression.compress(raw, codec, LIMIT);
      try {
        assertNotSame(raw, encoded.getRequest());
        assertEquals(
            encoded.getRequest().getLogEntries().stream()
                .flatMap(e -> e.getData().stream())
                .mapToLong(ByteBuffer::remaining)
                .sum(),
            encoded.getEncodedBytes());
        assertTrue(encoded.getEncodedBytes() < encoded.getRawBytes());
        assertTrue(memory.getMemoryBlock().getUsedMemoryInBytes() > 0);
        assertEquals(
            expectedWireCodec,
            encoded.getRequest().getLogEntries().get(0).getCompressionInfo().get(0).getCodec());
        assertEquals(
            TLogCompressionType.NONE,
            encoded.getRequest().getLogEntries().get(0).getCompressionInfo().get(2).getCodec());
        TSyncLogEntriesReq received = new TSyncLogEntriesReq();
        new TDeserializer()
            .deserialize(received, new TSerializer().serialize(encoded.getRequest()));
        LogCompression.decompress(received, LIMIT);
        assertEquals(snapshot, received);
        assertEquals(snapshot, raw);
        assertEquals(17, raw.getLogEntries().get(0).getData().get(0).position());
        assertEquals(16000, raw.getLogEntries().get(0).getData().get(0).limit());
      } finally {
        encoded.close();
        encoded.close();
      }
    }
  }

  @Test
  public void testFixedMinimumSizeBoundary() throws Exception {
    for (int size : new int[] {4095, 4096, 4097}) {
      TSyncLogEntriesReq raw =
          new TSyncLogEntriesReq(
              1,
              new TConsensusGroupId(TConsensusGroupType.DataRegion, 1),
              Collections.singletonList(
                  new TLogEntry(
                      Collections.singletonList(ByteBuffer.allocate(size)), 1, false, size)));
      try (LogCompression.EncodedRequest encoded = LogCompression.compress(raw, codec, LIMIT)) {
        if (size < 4096) {
          assertSame(raw, encoded.getRequest());
          assertFalse(encoded.getRequest().getLogEntries().get(0).isSetCompressionInfo());
        } else {
          assertNotSame(raw, encoded.getRequest());
          assertEquals(
              expectedWireCodec,
              encoded.getRequest().getLogEntries().get(0).getCompressionInfo().get(0).getCodec());
        }
        TSyncLogEntriesReq received = encoded.getRequest().deepCopy();
        LogCompression.decompress(received, LIMIT);
        assertEquals(
            raw.getLogEntries().get(0).getData(), received.getLogEntries().get(0).getData());
        assertEquals(0, raw.getLogEntries().get(0).getData().get(0).position());
      }
    }
  }

  @Test
  public void testAdaptiveAndMemoryFallback() throws Exception {
    TSyncLogEntriesReq raw = request();
    try (LogCompression.EncodedRequest result =
        LogCompression.compress(raw, CompressionType.UNCOMPRESSED, LIMIT)) {
      assertSame(raw, result.getRequest());
    }
    byte[] random = new byte[16384];
    new Random(1).nextBytes(random);
    TSyncLogEntriesReq noise = raw.deepCopy();
    noise.setLogEntries(
        Collections.singletonList(
            new TLogEntry(
                Collections.singletonList(ByteBuffer.wrap(random)), 1, false, random.length)));
    try (LogCompression.EncodedRequest result = LogCompression.compress(noise, codec, LIMIT)) {
      assertSame(noise, result.getRequest());
    }
    long capacity = memory.getMemoryBlock().getTotalMemorySizeInBytes();
    assertTrue(memory.reserveLogCompression(capacity));
    try (LogCompression.EncodedRequest result = LogCompression.compress(raw, codec, LIMIT)) {
      assertSame(raw, result.getRequest());
      assertTrue(result.isMemoryLimited());
      assertEquals(result.getRawBytes(), result.getEncodedBytes());
      assertEquals(capacity, memory.getMemoryBlock().getUsedMemoryInBytes());
    } finally {
      memory.freeLogCompression(capacity);
    }
  }

  @Test
  public void testRejectsEntireRequestBeforePublishingAnyDecodedEntry() throws Exception {
    try (LogCompression.EncodedRequest encoded = LogCompression.compress(request(), codec, LIMIT)) {
      TSyncLogEntriesReq bad = encoded.getRequest().deepCopy();
      // The first entry decodes successfully before the later entry fails in the codec.
      bad.getLogEntries().get(1).getData().set(0, ByteBuffer.wrap(new byte[] {0, 1, 2}));
      TSyncLogEntriesReq snapshot = bad.deepCopy();
      assertThrows(IOException.class, () -> LogCompression.decompress(bad, LIMIT));
      assertEquals(snapshot, bad);
      // Raw blocks in mixed entries still have to match their declared lengths.
      TSyncLogEntriesReq secondBad = encoded.getRequest().deepCopy();
      secondBad.getLogEntries().get(0).getCompressionInfo().get(2).setUncompressedSize(4);
      final TSyncLogEntriesReq mixed = secondBad;
      assertThrows(IOException.class, () -> LogCompression.decompress(mixed, LIMIT));
    }
  }

  @Test
  public void testMixedRawBufferContentIsForwardedWithoutChecksum() throws Exception {
    try (LogCompression.EncodedRequest encoded = LogCompression.compress(request(), codec, LIMIT)) {
      TSyncLogEntriesReq received = encoded.getRequest().deepCopy();
      byte[] changed = {4, 5, 6};
      received.getLogEntries().get(0).getData().set(2, ByteBuffer.wrap(changed));
      TSyncLogEntriesReq expected = request();
      expected.getLogEntries().get(0).getData().set(2, ByteBuffer.wrap(changed));
      LogCompression.decompress(received, LIMIT);
      // Same-length raw content is preserved verbatim; there is no payload integrity checksum.
      assertEquals(expected, received);
    }
  }

  @Test
  public void testMalformedMetadataAndCorruptedCodecInput() throws Exception {
    try (LogCompression.EncodedRequest encoded = LogCompression.compress(request(), codec, LIMIT)) {
      TSyncLogEntriesReq count = encoded.getRequest().deepCopy();
      count.getLogEntries().get(0).getCompressionInfo().remove(0);
      assertThrows(IOException.class, () -> LogCompression.decompress(count, LIMIT));
      TSyncLogEntriesReq huge = encoded.getRequest().deepCopy();
      huge.getLogEntries()
          .get(0)
          .getCompressionInfo()
          .get(0)
          .setUncompressedSize(Integer.MAX_VALUE);
      assertThrows(IOException.class, () -> LogCompression.decompress(huge, LIMIT));
      TSyncLogEntriesReq longOutput = encoded.getRequest().deepCopy();
      longOutput.getLogEntries().get(0).getCompressionInfo().get(0).setUncompressedSize(16000);
      assertThrows(IOException.class, () -> LogCompression.decompress(longOutput, LIMIT));
      TSyncLogEntriesReq rawLength = encoded.getRequest().deepCopy();
      rawLength.getLogEntries().get(0).getCompressionInfo().get(2).setUncompressedSize(4);
      assertThrows(IOException.class, () -> LogCompression.decompress(rawLength, LIMIT));
      TSyncLogEntriesReq negative = encoded.getRequest().deepCopy();
      negative.getLogEntries().get(0).getCompressionInfo().get(0).setUncompressedSize(-1);
      assertThrows(IOException.class, () -> LogCompression.decompress(negative, LIMIT));
      TSyncLogEntriesReq unknown = encoded.getRequest().deepCopy();
      unknown.getLogEntries().get(0).getCompressionInfo().get(0).setCodec(null);
      assertThrows(IOException.class, () -> LogCompression.decompress(unknown, LIMIT));
      TSyncLogEntriesReq corrupt = encoded.getRequest().deepCopy();
      corrupt.getLogEntries().get(0).getData().set(0, ByteBuffer.wrap(new byte[] {0, 1, 2}));
      assertThrows(IOException.class, () -> LogCompression.decompress(corrupt, LIMIT));
    }
  }

  @Test
  public void testSingleLogAboveSoftBatchLimit() throws Exception {
    TSyncLogEntriesReq raw = request();
    raw.setLogEntries(
        Collections.singletonList(
            new TLogEntry(
                Collections.singletonList(ByteBuffer.allocate(2 * 1024 * 1024)),
                1,
                false,
                2 * 1024 * 1024)));
    try (LogCompression.EncodedRequest encoded = LogCompression.compress(raw, codec, LIMIT)) {
      TSyncLogEntriesReq received = encoded.getRequest().deepCopy();
      LogCompression.decompress(received, LIMIT);
      assertEquals(raw, received);
    }
  }

  @Test
  public void testProcessorRejectsBeforeDeserializeAndDecodesWithSendingDisabled()
      throws Exception {
    IoTConsensus consensus = mock(IoTConsensus.class);
    IoTConsensusServerImpl impl = mock(IoTConsensusServerImpl.class);
    IStateMachine stateMachine = mock(IStateMachine.class);
    when(consensus.getImpl(new DataRegionId(5))).thenReturn(impl);
    when(impl.isActive()).thenReturn(true);
    // Decoder support must not depend on the sending codec (default NONE).
    when(impl.getConfig()).thenReturn(IoTConsensusConfig.newBuilder().build());
    when(impl.getIoTConsensusServerMetrics()).thenReturn(mock(IoTConsensusServerMetrics.class));
    when(impl.getStateMachine()).thenReturn(stateMachine);
    IoTConsensusRPCServiceProcessor processor = new IoTConsensusRPCServiceProcessor(consensus);
    try (LogCompression.EncodedRequest encoded = LogCompression.compress(request(), codec, LIMIT)) {
      TSyncLogEntriesReq corrupt = encoded.getRequest().deepCopy();
      corrupt.getLogEntries().get(1).getData().set(0, ByteBuffer.wrap(new byte[] {0, 1, 2}));
      assertTrue(processor.syncLogEntries(corrupt).isSetLogCompressionError());
      // Snappy reports a short output buffer as IllegalArgumentException; the RPC boundary must
      // reject it in the same explicit, pre-application error path as codec I/O failures.
      TSyncLogEntriesReq shortOutput = encoded.getRequest().deepCopy();
      shortOutput.getLogEntries().get(0).getCompressionInfo().get(0).setUncompressedSize(1);
      assertTrue(processor.syncLogEntries(shortOutput).isSetLogCompressionError());
      verify(impl, never()).getStateMachine();
      verify(impl, never()).syncLog(anyInt(), any());
      verify(impl, never())
          .buildIndexedConsensusRequestForRemoteRequest(
              anyLong(), anyLong(), anyLong(), anyInt(), anyList());
      when(impl.buildIndexedConsensusRequestForRemoteRequest(
              anyLong(), anyLong(), anyLong(), anyInt(), anyList()))
          .thenAnswer(
              call -> new IndexedConsensusRequest(-1, call.getArgument(0), call.getArgument(4)));
      ByteBufferConsensusRequest applied = new ByteBufferConsensusRequest(ByteBuffer.allocate(1));
      when(stateMachine.deserializeRequest(any())).thenReturn(applied);
      when(impl.syncLog(eq(3), eq(applied)))
          .thenReturn(new TSStatus(200).setSubStatus(Collections.singletonList(new TSStatus(200))));
      TSyncLogEntriesReq valid = encoded.getRequest().deepCopy();
      TSyncLogEntriesRes result = processor.syncLogEntries(valid);
      assertFalse(result.isSetLogCompressionError());
      assertEquals(200, result.getStatuses().get(0).getCode());
      assertEquals(request(), valid);
      verify(impl).syncLog(3, applied);
    }
  }

  @Test
  public void testMultiBlockScratchBudgetMatchesSequentialPeak() throws Exception {
    memory.setMemoryBlock(new AtomicLongMemoryBlock("compression-tight-pool", null, 512 * 1024));
    TSyncLogEntriesReq raw = request();
    List<TLogEntry> entries = new ArrayList<>();
    for (int i = 0; i < 16; i++)
      entries.add(
          new TLogEntry(
              Collections.singletonList(ByteBuffer.allocate(16384)), i + 1, false, 16384));
    raw.setLogEntries(entries);
    try (LogCompression.EncodedRequest encoded = LogCompression.compress(raw, codec, LIMIT)) {
      assertNotSame(raw, encoded.getRequest());
      assertEquals(16 * 16384, encoded.getRawBytes());
      assertFalse(encoded.isMemoryLimited());
      TSyncLogEntriesReq received = encoded.getRequest().deepCopy();
      LogCompression.decompress(received, LIMIT);
      assertEquals(raw, received);
      assertTrue(memory.getMemoryBlock().getUsedMemoryInBytes() < 512 * 1024);
    }
  }

  @Test
  public void testMemoryPressureFallsBackPerBlockAndKeepsOtherBlocksCompressed() throws Exception {
    memory.setMemoryBlock(new AtomicLongMemoryBlock("compression-partial-pool", null, 48 * 1024));
    TSyncLogEntriesReq raw = request();
    raw.setLogEntries(
        Collections.singletonList(
            new TLogEntry(
                Arrays.asList(
                    ByteBuffer.allocate(4096),
                    ByteBuffer.allocate(32768),
                    ByteBuffer.allocate(4096)),
                1,
                true,
                40960)));
    try (LogCompression.EncodedRequest encoded = LogCompression.compress(raw, codec, LIMIT)) {
      assertNotSame(raw, encoded.getRequest());
      assertTrue(encoded.isMemoryLimited());
      assertEquals(
          expectedWireCodec,
          encoded.getRequest().getLogEntries().get(0).getCompressionInfo().get(0).getCodec());
      assertEquals(
          TLogCompressionType.NONE,
          encoded.getRequest().getLogEntries().get(0).getCompressionInfo().get(1).getCodec());
      assertEquals(
          expectedWireCodec,
          encoded.getRequest().getLogEntries().get(0).getCompressionInfo().get(2).getCodec());
      TSyncLogEntriesReq received = encoded.getRequest().deepCopy();
      LogCompression.decompress(received, LIMIT);
      assertEquals(raw, received);
      assertTrue(encoded.getEncodedBytes() < encoded.getRawBytes());
    }
  }
}
