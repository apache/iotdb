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

public interface ChunkData extends TsFileData {
  IDeviceID getDevice();

  TTimePartitionSlot getTimePartitionSlot();

  void setNotDecode();

  boolean isAligned();

  boolean isEntireChunk();

  void writeEntireChunk(ByteBuffer chunkData, IChunkMetadata chunkMetadata) throws IOException;

  void writeEntirePage(PageHeader pageHeader, ByteBuffer pageData) throws IOException;

  void writeDecodePage(long[] times, Object[] values, int satisfiedLength) throws IOException;

  void writeToFileWriter(TsFileIOWriter writer) throws IOException, PageException;

  default void endChunk() {}

  default List<Chunk> getChunks() {
    return Collections.emptyList();
  }

  /**
   * Payload references of {@link #getChunks()}, in the same order. They are set once the chunks
   * have been written into the staged TsFile, and are present (instead of the payload itself) on a
   * piece that was read back from the WAL.
   */
  List<ChunkPayloadRef> getChunkPayloadRefs();

  void setChunkPayloadRefs(List<ChunkPayloadRef> chunkPayloadRefs);

  record ChunkLayout(
      long chunkGroupIndex,
      long chunkGroupHeaderOffset,
      long offset,
      long length,
      int chunkIndexInGroup,
      boolean firstChunkOfGroup) {}

  ChunkLayout getChunkLayout();

  void setChunkLayout(ChunkLayout layout);

  /**
   * Writes the payload slot of one chunk: its bytes when they are available in memory, otherwise
   * the reference that points at them in the staged file.
   *
   * @param includeContent whether the payload must be inlined. When the payload only exists as a
   *     reference and inlining was requested, it is read back from the staged file.
   */
  static void serializeChunkPayload(
      final DataOutputStream stream,
      final ByteBuffer data,
      final ChunkPayloadRef ref,
      final boolean includeContent)
      throws IOException {
    if (!includeContent && ref != null) {
      ReadWriteIOUtils.write(true, stream);
      ref.serializeTo(stream);
      return;
    }
    final byte[] payload;
    if (data != null && data.hasRemaining()) {
      payload = new byte[data.remaining()];
      data.duplicate().get(payload);
    } else if (ref != null) {
      payload = ref.readPayload();
    } else {
      payload = new byte[0];
    }
    ReadWriteIOUtils.write(false, stream);
    ReadWriteIOUtils.write(payload.length, stream);
    stream.write(payload);
  }

  /**
   * Reads the payload slot of one chunk, appending the reference to {@code refs} when the payload
   * is referenced instead of inlined.
   *
   * @return the payload, or an empty buffer when it has to be read back later
   */
  static ByteBuffer deserializeChunkPayload(
      final InputStream stream, final List<ChunkPayloadRef> refs) throws IOException {
    if (ReadWriteIOUtils.readBool(stream)) {
      refs.add(ChunkPayloadRef.deserializeFrom(stream));
      return ByteBuffer.wrap(new byte[0]);
    }
    final byte[] payload = new byte[ReadWriteIOUtils.readInt(stream)];
    new DataInputStream(stream).readFully(payload);
    return ByteBuffer.wrap(payload);
  }

  @Override
  default TsFileDataType getType() {
    return TsFileDataType.CHUNK;
  }

  static ChunkData deserialize(InputStream stream) throws PageException, IOException {
    boolean isAligned = ReadWriteIOUtils.readBool(stream);
    return isAligned
        ? AlignedChunkData.deserialize(stream)
        : NonAlignedChunkData.deserialize(stream);
  }

  static ChunkData createChunkData(
      boolean isAligned,
      IDeviceID device,
      ChunkHeader chunkHeader,
      TTimePartitionSlot timePartitionSlot) {
    return isAligned
        ? new AlignedChunkData(device, chunkHeader, timePartitionSlot)
        : new NonAlignedChunkData(device, chunkHeader, timePartitionSlot);
  }
}
