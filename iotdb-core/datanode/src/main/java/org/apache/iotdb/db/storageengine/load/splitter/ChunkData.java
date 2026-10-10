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

package org.apache.iotdb.db.storageengine.load.splitter;

import org.apache.iotdb.common.rpc.thrift.TTimePartitionSlot;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.i18n.StorageEngineMessages;

import org.apache.tsfile.exception.write.PageException;
import org.apache.tsfile.file.header.ChunkHeader;
import org.apache.tsfile.file.header.PageHeader;
import org.apache.tsfile.file.metadata.IChunkMetadata;
import org.apache.tsfile.file.metadata.IDeviceID;
import org.apache.tsfile.read.common.Chunk;
import org.apache.tsfile.utils.ReadWriteIOUtils;
import org.apache.tsfile.write.writer.TsFileIOWriter;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.List;

/** Represents chunk-level TsFile data during load splitting. */
public interface ChunkData extends TsFileData {

  ByteBuffer EMPTY_BUFFER = ByteBuffer.allocate(0);
  byte[] EMPTY_BYTES = new byte[0];

  // -------------------------------------------------------------------------
  // Metadata & Status
  // -------------------------------------------------------------------------

  IDeviceID getDevice();

  TTimePartitionSlot getTimePartitionSlot();

  boolean isAligned();

  boolean isEntireChunk();

  void setNotDecode();

  // -------------------------------------------------------------------------
  // Write Operations
  // -------------------------------------------------------------------------

  void writeEntireChunk(ByteBuffer chunkData, IChunkMetadata chunkMetadata) throws IOException;

  void writeEntirePage(PageHeader pageHeader, ByteBuffer pageData) throws IOException;

  void writeDecodePage(long[] times, Object[] values, int satisfiedLength) throws IOException;

  void writeToFileWriter(TsFileIOWriter writer) throws IOException, PageException;

  default void endChunk() {}

  // -------------------------------------------------------------------------
  // Chunks & References
  // -------------------------------------------------------------------------

  default List<Chunk> getChunks() {
    return Collections.emptyList();
  }

  /** Gets staged file payload references corresponding to {@link #getChunks()}. */
  List<ChunkPayloadRef> getChunkPayloadRefs();

  void setChunkPayloadRefs(List<ChunkPayloadRef> chunkPayloadRefs);

  // -------------------------------------------------------------------------
  // Layout
  // -------------------------------------------------------------------------

  record ChunkLayout(
      long chunkGroupIndex,
      long chunkGroupHeaderOffset,
      long offset,
      long length,
      int chunkIndexInGroup,
      boolean firstChunkOfGroup) {}

  ChunkLayout getChunkLayout();

  void setChunkLayout(ChunkLayout layout);

  @Override
  default TsFileDataType getType() {
    return TsFileDataType.CHUNK;
  }

  // -------------------------------------------------------------------------
  // Factory Methods
  // -------------------------------------------------------------------------

  static ChunkData createChunkData(
      boolean isAligned,
      IDeviceID device,
      ChunkHeader chunkHeader,
      TTimePartitionSlot timePartitionSlot) {
    return isAligned
        ? new AlignedChunkData(device, chunkHeader, timePartitionSlot)
        : new NonAlignedChunkData(device, chunkHeader, timePartitionSlot);
  }

  static ChunkData deserialize(InputStream stream) throws PageException, IOException {
    return ReadWriteIOUtils.readBool(stream)
        ? AlignedChunkData.deserialize(stream)
        : NonAlignedChunkData.deserialize(stream);
  }

  // -------------------------------------------------------------------------
  // Payload Ser/De Helpers
  // -------------------------------------------------------------------------

  /** Serializes payload as an external {@link ChunkPayloadRef} reference or raw inline bytes. */
  static void serializeChunkPayload(
      DataOutputStream stream, ByteBuffer data, ChunkPayloadRef ref, boolean includeContent)
      throws IOException {
    // 1. Write as reference if inlining is not required
    if (!includeContent && ref != null) {
      ReadWriteIOUtils.write(true, stream); // isRef = true
      ref.serializeTo(stream);
      return;
    }

    // 2. Write inline payload bytes
    ReadWriteIOUtils.write(false, stream); // isRef = false
    byte[] payload = extractPayloadBytes(data, ref);
    ReadWriteIOUtils.write(payload.length, stream);
    stream.write(payload);
  }

  /** Deserializes payload, appending to {@code refs} if referenced externally. */
  static ByteBuffer deserializeChunkPayload(InputStream stream, List<ChunkPayloadRef> refs)
      throws IOException {
    boolean isRef = ReadWriteIOUtils.readBool(stream);
    if (isRef) {
      refs.add(ChunkPayloadRef.deserializeFrom(stream));
      return EMPTY_BUFFER;
    }

    int length = ReadWriteIOUtils.readInt(stream);
    int maxLength = IoTDBDescriptor.getInstance().getConfig().getThriftMaxFrameSize();
    if (length < 0 || length > maxLength) {
      throw new IOException(
          String.format(
              StorageEngineMessages
                  .EXCEPTION_INVALID_INLINE_CHUNK_PAYLOAD_LENGTH_ARG_THE_MAXIMUM_IS_ARG_20EE95D9,
              length,
              maxLength));
    }

    byte[] payload = new byte[length];
    (stream instanceof DataInputStream dis ? dis : new DataInputStream(stream)).readFully(payload);
    return ByteBuffer.wrap(payload);
  }

  private static byte[] extractPayloadBytes(ByteBuffer data, ChunkPayloadRef ref)
      throws IOException {
    if (data != null && data.hasRemaining()) {
      byte[] bytes = new byte[data.remaining()];
      data.duplicate().get(bytes);
      return bytes;
    }
    return ref != null ? ref.readPayload() : EMPTY_BYTES;
  }
}
