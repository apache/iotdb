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

package org.apache.iotdb.db.pipe.sink.payload.evolvable.request;

import org.apache.iotdb.commons.pipe.sink.payload.thrift.request.IoTDBSinkRequestVersion;
import org.apache.iotdb.commons.pipe.sink.payload.thrift.request.PipeRequestType;
import org.apache.iotdb.service.rpc.thrift.TPipeTransferReq;

import org.apache.tsfile.utils.PublicBAOS;
import org.apache.tsfile.utils.ReadWriteIOUtils;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

/**
 * Batches multiple object-file pieces for one TsFile under creation. Each piece carries portable
 * relative path segments, write offset, total object file length, and raw bytes for that segment.
 */
public class PipeTransferTsFileObjectBatchReq extends TPipeTransferReq {

  public static final class ObjectFilePieceChunk {
    private final String[] relativePathSegments;
    private final long startWritingOffset;
    private final long totalLength;
    private final byte[] objectPiece;
    private final int objectPieceLength;

    public ObjectFilePieceChunk(
        final String[] relativePathSegments,
        final long startWritingOffset,
        final long totalLength,
        final byte[] objectPiece,
        final int objectPieceLength) {
      this.relativePathSegments =
          relativePathSegments == null ? new String[0] : relativePathSegments;
      this.startWritingOffset = startWritingOffset;
      this.totalLength = totalLength;
      this.objectPiece = Objects.requireNonNull(objectPiece, "objectPiece");
      this.objectPieceLength = objectPieceLength;
    }

    public String[] getRelativePathSegments() {
      return relativePathSegments;
    }

    public long getStartWritingOffset() {
      return startWritingOffset;
    }

    public long getTotalLength() {
      return totalLength;
    }

    public byte[] getObjectPiece() {
      return objectPiece;
    }

    public int getObjectPieceLength() {
      return objectPieceLength;
    }
  }

  private transient String tsFileNameWithoutSuffix;
  private transient List<ObjectFilePieceChunk> chunks;

  private PipeTransferTsFileObjectBatchReq() {}

  public String getTsFileNameWithoutSuffix() {
    return tsFileNameWithoutSuffix;
  }

  public List<ObjectFilePieceChunk> getChunks() {
    return chunks;
  }

  /////////////////////////////// Thrift ///////////////////////////////

  public static PipeTransferTsFileObjectBatchReq toTPipeTransferReq(
      final String tsFileNameWithoutSuffix, final List<ObjectFilePieceChunk> chunks)
      throws IOException {
    final PipeTransferTsFileObjectBatchReq req = new PipeTransferTsFileObjectBatchReq();
    req.tsFileNameWithoutSuffix = tsFileNameWithoutSuffix;
    req.chunks = chunks == null ? Collections.emptyList() : new ArrayList<>(chunks);

    req.version = IoTDBSinkRequestVersion.VERSION_1.getVersion();
    req.type = PipeRequestType.TRANSFER_TS_FILE_OBJECT_BATCH.getType();
    try (final PublicBAOS byteArrayOutputStream = new PublicBAOS();
        final DataOutputStream outputStream = new DataOutputStream(byteArrayOutputStream)) {
      ReadWriteIOUtils.write(tsFileNameWithoutSuffix, outputStream);
      ReadWriteIOUtils.write(req.chunks.size(), outputStream);
      for (final ObjectFilePieceChunk chunk : req.chunks) {
        writeChunk(chunk, outputStream);
      }
      req.body = ByteBuffer.wrap(byteArrayOutputStream.getBuf(), 0, byteArrayOutputStream.size());
    }
    return req;
  }

  private static void writeChunk(final ObjectFilePieceChunk chunk, final DataOutputStream out)
      throws IOException {
    ReadWriteIOUtils.write(chunk.relativePathSegments.length, out);
    for (final String segment : chunk.relativePathSegments) {
      ReadWriteIOUtils.write(segment, out);
    }
    ReadWriteIOUtils.write(chunk.startWritingOffset, out);
    ReadWriteIOUtils.write(chunk.totalLength, out);
    ReadWriteIOUtils.write(chunk.objectPieceLength, out);
    out.write(chunk.objectPiece, 0, chunk.objectPieceLength);
  }

  public static PipeTransferTsFileObjectBatchReq fromTPipeTransferReq(
      final TPipeTransferReq transferReq) {
    final PipeTransferTsFileObjectBatchReq req = new PipeTransferTsFileObjectBatchReq();
    req.tsFileNameWithoutSuffix = ReadWriteIOUtils.readString(transferReq.body);
    final int chunkCount = ReadWriteIOUtils.readInt(transferReq.body);
    final List<ObjectFilePieceChunk> chunks = new ArrayList<>(chunkCount);
    for (int i = 0; i < chunkCount; i++) {
      final int segmentSize = ReadWriteIOUtils.readInt(transferReq.body);
      final String[] segments = new String[segmentSize];
      for (int j = 0; j < segmentSize; j++) {
        segments[j] = ReadWriteIOUtils.readString(transferReq.body);
      }
      final long offset = ReadWriteIOUtils.readLong(transferReq.body);
      final long totalLength = ReadWriteIOUtils.readLong(transferReq.body);
      final int payloadLen = ReadWriteIOUtils.readInt(transferReq.body);
      final byte[] payload = new byte[payloadLen];
      transferReq.body.get(payload);
      chunks.add(new ObjectFilePieceChunk(segments, offset, totalLength, payload, payloadLen));
    }
    req.chunks = chunks;
    req.version = transferReq.version;
    req.type = transferReq.type;
    req.body = transferReq.body;
    return req;
  }

  /////////////////////////////// Air Gap ///////////////////////////////

  public static byte[] toTPipeTransferBytes(
      final String tsFileNameWithoutSuffix, final List<ObjectFilePieceChunk> chunks)
      throws IOException {
    try (final PublicBAOS byteArrayOutputStream = new PublicBAOS();
        final DataOutputStream outputStream = new DataOutputStream(byteArrayOutputStream)) {
      ReadWriteIOUtils.write(IoTDBSinkRequestVersion.VERSION_1.getVersion(), outputStream);
      ReadWriteIOUtils.write(PipeRequestType.TRANSFER_TS_FILE_OBJECT_BATCH.getType(), outputStream);
      ReadWriteIOUtils.write(tsFileNameWithoutSuffix, outputStream);
      final List<ObjectFilePieceChunk> nonNull = chunks == null ? Collections.emptyList() : chunks;
      ReadWriteIOUtils.write(nonNull.size(), outputStream);
      for (final ObjectFilePieceChunk chunk : nonNull) {
        writeChunk(chunk, outputStream);
      }
      return byteArrayOutputStream.toByteArray();
    }
  }

  /**
   * Rough upper bound of serialized size for one chunk (for batching heuristics without building a
   * full {@link org.apache.tsfile.utils.PublicBAOS}).
   */
  public static int estimateSerializedChunkBytes(
      final String[] relativePathSegments, final int payloadLength) {
    int n = 4;
    final String[] segs = relativePathSegments == null ? new String[0] : relativePathSegments;
    for (final String s : segs) {
      n += 4;
      if (s != null) {
        n += s.getBytes(StandardCharsets.UTF_8).length;
      }
    }
    n += 8 + 8 + 4 + payloadLength;
    return n;
  }

  /**
   * Serialized size of thrift body prefix before chunk list: tsFile base name string + chunk count
   * int (matches {@link #toTPipeTransferReq(String, List)} body layout).
   */
  public static int estimateSerializedBodyHeaderBytes(final String tsFileBaseName) {
    int n = 4;
    if (tsFileBaseName != null) {
      n += 4 + tsFileBaseName.getBytes(StandardCharsets.UTF_8).length;
    }
    return n;
  }
}
