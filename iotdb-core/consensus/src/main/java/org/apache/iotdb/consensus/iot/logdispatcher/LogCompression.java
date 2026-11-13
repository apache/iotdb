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

import org.apache.iotdb.consensus.i18n.IoTConsensusMessages;
import org.apache.iotdb.consensus.iot.thrift.TLogBufferCompression;
import org.apache.iotdb.consensus.iot.thrift.TLogCompressionType;
import org.apache.iotdb.consensus.iot.thrift.TLogEntry;
import org.apache.iotdb.consensus.iot.thrift.TSyncLogEntriesReq;

import org.apache.tsfile.compress.ICompressor;
import org.apache.tsfile.compress.IUnCompressor;
import org.apache.tsfile.file.metadata.enums.CompressionType;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

/** Wire-only encoding: the original batch and its buffer positions remain available for retries. */
public final class LogCompression {
  // Skip tiny blocks to avoid codec and descriptor overhead.
  private static final int MIN_COMPRESSION_SIZE = 4096;
  // Conservative allowance for the additional Thrift descriptor and list field on the wire.
  private static final int DESCRIPTOR_BYTES = 32;
  private static final ICompressor LZ4 = ICompressor.getCompressor(CompressionType.LZ4);
  private static final ICompressor SNAPPY = ICompressor.getCompressor(CompressionType.SNAPPY);
  private static final ICompressor ZSTD = ICompressor.getCompressor(CompressionType.ZSTD);
  private static final IUnCompressor LZ4_DECODER =
      IUnCompressor.getUnCompressor(CompressionType.LZ4);
  private static final IUnCompressor SNAPPY_DECODER =
      IUnCompressor.getUnCompressor(CompressionType.SNAPPY);
  private static final IUnCompressor ZSTD_DECODER =
      IUnCompressor.getUnCompressor(CompressionType.ZSTD);

  private LogCompression() {}

  /** Accounts for arrays throughout the async attempt, including synchronous send failure. */
  public static final class EncodedRequest implements AutoCloseable {
    private TSyncLogEntriesReq request;
    private final AtomicLong reserved;
    private final long rawBytes;
    private long encodedBytes;
    private boolean memoryLimited;

    private EncodedRequest(
        TSyncLogEntriesReq request, long reserved, long rawBytes, boolean memoryLimited) {
      this.request = request;
      this.reserved = new AtomicLong(reserved);
      this.rawBytes = rawBytes;
      this.encodedBytes = rawBytes;
      this.memoryLimited = memoryLimited;
    }

    public long getRawBytes() {
      return rawBytes;
    }

    public long getEncodedBytes() {
      return encodedBytes;
    }

    public boolean isMemoryLimited() {
      return memoryLimited;
    }

    public TSyncLogEntriesReq getRequest() {
      return request;
    }

    private void retain(long bytes) {
      long released = reserved.getAndSet(bytes) - bytes;
      if (released > 0) {
        IoTConsensusMemoryManager.getInstance().freeLogCompression(released);
      }
    }

    @Override
    public void close() {
      long bytes = reserved.getAndSet(0);
      if (bytes > 0) {
        IoTConsensusMemoryManager.getInstance().freeLogCompression(bytes);
      }
    }
  }

  public static EncodedRequest compress(
      TSyncLogEntriesReq original, CompressionType codec, int maxDecodedBytes) throws IOException {
    TLogCompressionType wireCodec = toWireCodec(codec);
    long buffers = 0;
    long rawSize = 0;
    boolean hasEligibleBuffer = false;
    for (TLogEntry entry : original.getLogEntries()) {
      for (ByteBuffer buffer : entry.getData()) {
        int uncompressedLength = buffer.remaining();
        rawSize += uncompressedLength;
        if (uncompressedLength >= MIN_COMPRESSION_SIZE) {
          hasEligibleBuffer = true;
        }
        buffers++;
      }
    }
    // Preserve the original path for oversized legal entries and when scratch memory is scarce.
    if (codec == CompressionType.UNCOMPRESSED || !hasEligibleBuffer || rawSize > maxDecodedBytes) {
      return new EncodedRequest(original, 0, rawSize, false);
    }
    // Charge descriptor/list/entry overhead first, then one block's working arrays at a time.
    // Retain only actual compact outputs between blocks, not a whole-batch worst-case estimate.
    long budget = buffers * 256 + original.getLogEntriesSize() * 128L;
    if (!IoTConsensusMemoryManager.getInstance().reserveLogCompression(budget)) {
      return new EncodedRequest(original, 0, rawSize, true);
    }
    EncodedRequest result = new EncodedRequest(original, budget, rawSize, false);
    try {
      List<TLogEntry> entries = new ArrayList<>(original.getLogEntriesSize());
      long retained = 0;
      boolean changed = false;
      for (TLogEntry entry : original.getLogEntries()) {
        List<ByteBuffer> data = new ArrayList<>(entry.getDataSize());
        List<TLogBufferCompression> info = new ArrayList<>(entry.getDataSize());
        long raw = 0;
        long wire = 0;
        long arrays = 0;
        boolean compressed = false;
        for (ByteBuffer buffer : entry.getData()) {
          int uncompressedLength = buffer.remaining();
          raw += uncompressedLength;
          ByteBuffer encoded = buffer;
          TLogCompressionType actualCodec = TLogCompressionType.NONE;
          if (uncompressedLength >= MIN_COMPRESSION_SIZE) {
            // Includes an optional input copy, codec-bound output and compact output, with
            // allowance for array headers and codec bounds on very small buffers.
            long workspace = 4L * uncompressedLength + 256;
            long before = result.reserved.get();
            if (IoTConsensusMemoryManager.getInstance().reserveLogCompression(workspace)) {
              result.reserved.addAndGet(workspace);
              byte[] input = buffer.hasArray() ? buffer.array() : bytes(buffer);
              int offset = buffer.hasArray() ? buffer.arrayOffset() + buffer.position() : 0;
              int maxCompressedLength =
                  compressor(codec).getMaxBytesForCompression(uncompressedLength);
              byte[] output = new byte[maxCompressedLength];
              int compressedLength =
                  compressor(codec).compress(input, offset, uncompressedLength, output);
              if (compressedLength < 0) {
                throw invalid(Failure.CODEC, uncompressedLength, compressedLength);
              }
              if ((long) compressedLength + DESCRIPTOR_BYTES < uncompressedLength) {
                encoded = ByteBuffer.wrap(Arrays.copyOf(output, compressedLength));
                actualCodec = wireCodec;
                arrays += compressedLength;
                compressed = true;
              }
              result.retain(
                  before + (actualCodec == TLogCompressionType.NONE ? 0 : encoded.remaining()));
            } else {
              // Keep this block raw; other smaller blocks in the entry may still fit.
              result.memoryLimited = true;
            }
          }
          data.add(encoded);
          info.add(new TLogBufferCompression(actualCodec, uncompressedLength));
          wire += encoded.remaining();
        }
        // Raw descriptors in a mixed WAL entry cost space too.
        if (compressed && wire + DESCRIPTOR_BYTES * (long) data.size() + 8 < raw) {
          entries.add(copy(entry, data).setCompressionInfo(info));
          retained += arrays + data.size() * 256L + 128;
          result.encodedBytes += wire - raw;
          changed = true;
        } else {
          entries.add(entry);
          result.retain(result.reserved.get() - arrays);
        }
      }
      if (changed) {
        result.request =
            new TSyncLogEntriesReq(original.getPeerId(), original.getConsensusGroupId(), entries);
      }
      result.retain(retained);
      return result;
    } catch (IOException | RuntimeException e) {
      result.close();
      throw e;
    }
  }

  /** Validate declared sizes before allocating; publish nothing until every buffer is decoded. */
  public static void decompress(TSyncLogEntriesReq request, int maxDecodedBytes)
      throws IOException {
    boolean encoded = false;
    long total = 0;
    for (TLogEntry entry : request.getLogEntries()) {
      if (entry.isSetCompressionInfo()) {
        encoded = true;
        if (entry.getCompressionInfoSize() != entry.getDataSize()) {
          throw invalid(Failure.DESCRIPTORS, entry.getDataSize(), entry.getCompressionInfoSize());
        }
        for (int i = 0; i < entry.getDataSize(); i++) {
          TLogBufferCompression info = entry.getCompressionInfo().get(i);
          int size = info.getUncompressedSize();
          if (size < 0) {
            throw invalid(Failure.LENGTH, 0, size);
          }
          if (info.getCodec() == null) {
            throw invalid(Failure.CODEC, 0, -1);
          }
          if (info.getCodec() == TLogCompressionType.NONE
              && size != entry.getData().get(i).remaining()) {
            throw invalid(Failure.LENGTH, size, entry.getData().get(i).remaining());
          }
          total += size;
          if (total > maxDecodedBytes) {
            throw invalid(Failure.LIMIT, maxDecodedBytes, total);
          }
        }
      } else {
        for (ByteBuffer buffer : entry.getData()) {
          total += buffer.remaining();
        }
      }
    }
    if (!encoded) {
      return;
    }
    if (total > maxDecodedBytes) {
      throw invalid(Failure.LIMIT, maxDecodedBytes, total);
    }
    List<TLogEntry> decoded = new ArrayList<>(request.getLogEntriesSize());
    for (TLogEntry entry : request.getLogEntries()) {
      if (!entry.isSetCompressionInfo()) {
        decoded.add(entry);
        continue;
      }
      List<ByteBuffer> data = new ArrayList<>(entry.getDataSize());
      for (int i = 0; i < entry.getDataSize(); i++) {
        TLogBufferCompression info = entry.getCompressionInfo().get(i);
        ByteBuffer buffer = entry.getData().get(i);
        if (info.getCodec() != TLogCompressionType.NONE) {
          byte[] input = buffer.hasArray() ? buffer.array() : bytes(buffer);
          int offset = buffer.hasArray() ? buffer.arrayOffset() + buffer.position() : 0;
          byte[] output = new byte[info.getUncompressedSize()];
          int actual;
          try {
            actual =
                decoder(info.getCodec()).uncompress(input, offset, buffer.remaining(), output, 0);
          } catch (RuntimeException e) {
            // TsFile codecs may report malformed input through unchecked exceptions.
            IOException failure = invalid(Failure.CODEC, output.length, buffer.remaining());
            failure.initCause(e);
            throw failure;
          }
          if (actual != output.length) {
            throw invalid(Failure.LENGTH, output.length, actual);
          }
          buffer = ByteBuffer.wrap(output);
        }
        data.add(buffer);
      }
      decoded.add(copy(entry, data));
    }
    // The RPC exclusively owns this request. Dropping the encoded entries also drops their arrays.
    // No partially decoded request is exposed on failure.
    request.setLogEntries(decoded);
  }

  private static TLogEntry copy(TLogEntry source, List<ByteBuffer> data) {
    TLogEntry entry =
        new TLogEntry(data, source.getSearchIndex(), source.isFromWAL(), source.getMemorySize());
    if (source.isSetRoutingEpoch()) {
      entry.setRoutingEpoch(source.getRoutingEpoch());
    }
    if (source.isSetPhysicalTime()) {
      entry.setPhysicalTime(source.getPhysicalTime());
    }
    return entry;
  }

  private static byte[] bytes(ByteBuffer buffer) {
    ByteBuffer copy = buffer.duplicate();
    byte[] bytes = new byte[copy.remaining()];
    copy.get(bytes);
    return bytes;
  }

  private static TLogCompressionType toWireCodec(CompressionType codec) throws IOException {
    switch (codec) {
      case UNCOMPRESSED:
        return TLogCompressionType.NONE;
      case LZ4:
        return TLogCompressionType.LZ4;
      case SNAPPY:
        return TLogCompressionType.SNAPPY;
      case ZSTD:
        return TLogCompressionType.ZSTD;
      default:
        throw invalid(Failure.CODEC, 0, codec.serialize());
    }
  }

  private static ICompressor compressor(CompressionType codec) throws IOException {
    switch (codec) {
      case LZ4:
        return LZ4;
      case SNAPPY:
        return SNAPPY;
      case ZSTD:
        return ZSTD;
      default:
        throw invalid(Failure.CODEC, 0, codec.serialize());
    }
  }

  private static IUnCompressor decoder(TLogCompressionType codec) throws IOException {
    switch (codec) {
      case LZ4:
        return LZ4_DECODER;
      case SNAPPY:
        return SNAPPY_DECODER;
      case ZSTD:
        return ZSTD_DECODER;
      default:
        throw invalid(Failure.CODEC, 0, codec.getValue());
    }
  }

  private enum Failure {
    CODEC,
    LENGTH,
    LIMIT,
    DESCRIPTORS
  }

  private static IOException invalid(Failure failure, long expected, long actual) {
    return new IOException(
        String.format(
            IoTConsensusMessages
                .EXCEPTION_INVALID_IOTCONSENSUS_COMPRESSED_LOG_ARG_EXPECTED_ARG_ACTUAL_ARG_4986175E,
            failure.name(),
            expected,
            actual));
  }
}
