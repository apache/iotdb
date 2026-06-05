/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the License, Version 2.0 (the
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

package org.apache.iotdb.db.pipe.sink.util;

import org.apache.iotdb.commons.pipe.config.PipeConfig;
import org.apache.iotdb.commons.pipe.sink.payload.thrift.request.PipeRequestType;
import org.apache.iotdb.db.pipe.event.common.util.PipeObjectPathUtil;
import org.apache.iotdb.db.pipe.sink.payload.evolvable.request.PipeTransferTsFileObjectBatchReq;
import org.apache.iotdb.db.pipe.sink.payload.evolvable.request.PipeTransferTsFileObjectBatchReq.ObjectFilePieceChunk;
import org.apache.iotdb.service.rpc.thrift.TPipeTransferReq;

import org.apache.tsfile.utils.Pair;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.stream.Stream;

/**
 * Streams object files via {@link PipeObjectPathUtil#getObjectFileStream(Path)} (lazy walk, O(1)
 * listing state per step), groups read pieces into {@link
 * PipeRequestType#TRANSFER_TS_FILE_OBJECT_BATCH} payloads. Caller must close {@code
 * objectFileStream} when finished.
 */
public final class PipeTsFileObjectBatchTransfer {

  public static int defaultMaxBatchSerializedSumBytes() {
    return PipeConfig.getInstance().getPipeSinkReadFileBufferSize();
  }

  private PipeTsFileObjectBatchTransfer() {}

  /** One logical RPC payload: multiple object pieces for the same TsFile. */
  public static final class ObjectBatch {
    private final List<ObjectFilePieceChunk> chunks;

    ObjectBatch(final List<ObjectFilePieceChunk> chunks) {
      this.chunks = chunks;
    }

    public List<ObjectFilePieceChunk> getChunks() {
      return chunks;
    }

    public TPipeTransferReq toThrift(final String tsFileNameWithoutSuffix) throws IOException {
      return PipeTransferTsFileObjectBatchReq.toTPipeTransferReq(tsFileNameWithoutSuffix, chunks);
    }

    public byte[] toAirGapBytes(final String tsFileNameWithoutSuffix) throws IOException {
      return PipeTransferTsFileObjectBatchReq.toTPipeTransferBytes(tsFileNameWithoutSuffix, chunks);
    }
  }

  /**
   * @param tsFileNameWithoutSuffix used only to estimate serialized batch size (thrift body
   *     prefix).
   */
  public static Iterator<ObjectBatch> batchIterator(
      final String tsFileNameWithoutSuffix,
      final Stream<Pair<Path, File>> objectFileStream,
      final int maxPiecePayloadBytes,
      final int maxBatchSerializedSumBytes)
      throws IOException {

    final Iterator<Pair<Path, File>> fileIter = objectFileStream.iterator();

    return new Iterator<ObjectBatch>() {
      private RandomAccessFile currentRaf;
      private Pair<Path, File> currentEntry;
      private Pair<Path, File> pendingEmptyEntry;
      private long filePosition;
      private boolean finished;

      private void closeRaf() throws IOException {
        if (currentRaf != null) {
          currentRaf.close();
          currentRaf = null;
        }
      }

      private boolean openNextFile() throws IOException {
        closeRaf();
        while (fileIter.hasNext()) {
          currentEntry = fileIter.next();
          final File f = currentEntry.getRight();
          if (f != null && f.isFile() && f.length() == 0) {
            pendingEmptyEntry = currentEntry;
            currentEntry = null;
            return true;
          }
          if (f != null && f.isFile()) {
            currentRaf = new RandomAccessFile(f, "r");
            filePosition = 0L;
            return true;
          }
        }
        currentEntry = null;
        return false;
      }

      private boolean ensureOpenReadable() throws IOException {
        if (finished) {
          return false;
        }
        if (pendingEmptyEntry != null) {
          return true;
        }
        if (currentRaf != null && filePosition < currentRaf.length()) {
          return true;
        }
        closeRaf();
        return openNextFile();
      }

      @Override
      public boolean hasNext() {
        try {
          return ensureOpenReadable();
        } catch (final IOException e) {
          throw new IllegalStateException(e);
        }
      }

      @Override
      public ObjectBatch next() {
        try {
          if (!ensureOpenReadable()) {
            finished = true;
            throw new NoSuchElementException();
          }

          final int headerBytes =
              PipeTransferTsFileObjectBatchReq.estimateSerializedBodyHeaderBytes(
                  tsFileNameWithoutSuffix);
          final List<ObjectFilePieceChunk> chunks = new ArrayList<>();
          int serializedChunksSum = 0;

          while (ensureOpenReadable()) {
            if (pendingEmptyEntry != null) {
              final String[] segments =
                  PipeObjectPathUtil.toPathSegments(pendingEmptyEntry.getLeft());
              final int chunkSerializedEstimate =
                  PipeTransferTsFileObjectBatchReq.estimateSerializedChunkBytes(segments, 0);
              if (!chunks.isEmpty()
                  && headerBytes + serializedChunksSum + chunkSerializedEstimate
                      > maxBatchSerializedSumBytes) {
                return new ObjectBatch(chunks);
              }
              chunks.add(new ObjectFilePieceChunk(segments, 0L, 0L, new byte[0], 0));
              serializedChunksSum += chunkSerializedEstimate;
              pendingEmptyEntry = null;
              if (headerBytes + serializedChunksSum >= maxBatchSerializedSumBytes) {
                return new ObjectBatch(chunks);
              }
              continue;
            }

            final long totalLen = currentRaf.length();
            final String[] segments = PipeObjectPathUtil.toPathSegments(currentEntry.getLeft());
            final long remain = totalLen - filePosition;
            if (remain <= 0) {
              closeRaf();
              continue;
            }

            final int natural = (int) Math.min(maxPiecePayloadBytes, remain);
            final int chunkSerializedEstimate =
                PipeTransferTsFileObjectBatchReq.estimateSerializedChunkBytes(segments, natural);

            if (!chunks.isEmpty()
                && headerBytes + serializedChunksSum + chunkSerializedEstimate
                    > maxBatchSerializedSumBytes) {
              return new ObjectBatch(chunks);
            }

            final byte[] buf = new byte[natural];
            final int n = currentRaf.read(buf, 0, natural);
            if (n <= 0) {
              closeRaf();
              continue;
            }

            chunks.add(new ObjectFilePieceChunk(segments, filePosition, totalLen, buf, n));
            filePosition += n;
            serializedChunksSum +=
                PipeTransferTsFileObjectBatchReq.estimateSerializedChunkBytes(segments, n);

            final boolean fileEnded = filePosition >= totalLen;
            if (fileEnded) {
              closeRaf();
            }
            if (headerBytes + serializedChunksSum >= maxBatchSerializedSumBytes || fileEnded) {
              return new ObjectBatch(chunks);
            }
          }

          if (chunks.isEmpty()) {
            finished = true;
            throw new NoSuchElementException();
          }
          return new ObjectBatch(chunks);
        } catch (final IOException e) {
          throw new IllegalStateException(e);
        }
      }
    };
  }
}
