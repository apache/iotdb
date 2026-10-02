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
import org.apache.iotdb.commons.utils.TimePartitionUtils;
import org.apache.iotdb.db.i18n.StorageEngineMessages;
import org.apache.iotdb.db.utils.TypeServices;

import org.apache.tsfile.encrypt.EncryptParameter;
import org.apache.tsfile.encrypt.EncryptUtils;
import org.apache.tsfile.exception.write.PageException;
import org.apache.tsfile.file.header.ChunkHeader;
import org.apache.tsfile.file.header.PageHeader;
import org.apache.tsfile.file.metadata.IChunkMetadata;
import org.apache.tsfile.file.metadata.IDeviceID;
import org.apache.tsfile.file.metadata.PlainDeviceID;
import org.apache.tsfile.file.metadata.StringArrayDeviceID;
import org.apache.tsfile.file.metadata.statistics.Statistics;
import org.apache.tsfile.read.common.Chunk;
import org.apache.tsfile.read.common.type.Type;
import org.apache.tsfile.utils.ReadWriteIOUtils;
import org.apache.tsfile.write.chunk.ChunkWriterImpl;
import org.apache.tsfile.write.schema.MeasurementSchema;
import org.apache.tsfile.write.writer.TsFileIOWriter;

import javax.annotation.Nonnull;

import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import static org.apache.iotdb.db.storageengine.load.LoadTsFileManager.MEASUREMENT_ID_CACHE;
import static org.apache.tsfile.common.constant.TsFileConstant.TIME_COLUMN_MASK;
import static org.apache.tsfile.common.constant.TsFileConstant.VALUE_COLUMN_MASK;

public class NonAlignedChunkData implements ChunkData {

  /** Payload references of {@link #getChunks()}, see {@link ChunkPayloadRef}. */
  private List<ChunkPayloadRef> chunkPayloadRefs = new ArrayList<>();

  private final TTimePartitionSlot timePartitionSlot;
  private final IDeviceID device;
  private final ChunkHeader chunkHeader;
  private final EncryptParameter encryptParameter;

  private long dataSize;
  private boolean needDecodeChunk;

  // Buffer for unencoded pages
  private final List<PageBuffer> pageBuffers = new ArrayList<>();
  // The final standard Chunk object
  private Chunk chunk;
  private ChunkLayout chunkLayout;

  private static class PageBuffer {
    final boolean needDecode;
    final PageHeader pageHeader;
    final ByteBuffer pageData;
    final long[] timeBatch;
    final Object[] valueBatch;
    final int satisfiedLength;

    PageBuffer(PageHeader pageHeader, ByteBuffer pageData) {
      this.needDecode = false;
      this.pageHeader = pageHeader;
      this.pageData = pageData;
      this.timeBatch = null;
      this.valueBatch = null;
      this.satisfiedLength = 0;
    }

    PageBuffer(long[] timeBatch, Object[] valueBatch, int satisfiedLength) {
      this.needDecode = true;
      this.pageHeader = null;
      this.pageData = null;
      this.timeBatch = timeBatch;
      this.valueBatch = valueBatch;
      this.satisfiedLength = satisfiedLength;
    }
  }

  public NonAlignedChunkData(
      @Nonnull final IDeviceID device,
      final ChunkHeader chunkHeader,
      final TTimePartitionSlot timePartitionSlot) {
    this(device, chunkHeader, timePartitionSlot, EncryptUtils.getEncryptParameter());
  }

  public NonAlignedChunkData(
      @Nonnull final IDeviceID device,
      final ChunkHeader chunkHeader,
      final TTimePartitionSlot timePartitionSlot,
      final EncryptParameter encryptParameter) {
    this.dataSize = 0;
    this.device = device;
    this.chunkHeader = chunkHeader;
    this.timePartitionSlot = timePartitionSlot;
    this.encryptParameter = encryptParameter;
    this.needDecodeChunk = true;
    addAttrDataSize();
  }

  private void addAttrDataSize() {
    dataSize += 2 * Byte.BYTES; // isModification and isAligned
    dataSize += Long.BYTES; // timePartitionSlot
    dataSize += device.serializedSize(); // device
    dataSize += chunkHeader.getSerializedSize(); // timeChunkHeader
  }

  @Override
  public IDeviceID getDevice() {
    return device;
  }

  @Override
  public TTimePartitionSlot getTimePartitionSlot() {
    return timePartitionSlot;
  }

  @Override
  public long getDataSize() {
    return dataSize;
  }

  @Override
  public void setNotDecode() {
    needDecodeChunk = false;
  }

  @Override
  public boolean isAligned() {
    return false;
  }

  @Override
  public boolean isEntireChunk() {
    return !needDecodeChunk;
  }

  // ----------------------- Data Collection (In-Memory) -----------------------

  @Override
  public void writeEntireChunk(final ByteBuffer chunkData, final IChunkMetadata chunkMetadata) {
    this.chunk = new Chunk(chunkHeader, chunkData, null, chunkMetadata.getStatistics());
    this.needDecodeChunk = false;
    this.dataSize += chunkData.remaining() + chunkMetadata.getStatistics().getSerializedSize();
  }

  @Override
  public void writeEntirePage(final PageHeader pageHeader, final ByteBuffer pageData) {
    pageBuffers.add(new PageBuffer(pageHeader, pageData));
  }

  @Override
  public void writeDecodePage(
      final long[] times, final Object[] values, final int satisfiedLength) {
    pageBuffers.add(new PageBuffer(times, values, satisfiedLength));
  }

  @Override
  public List<Chunk> getChunks() {
    if (chunk == null) {
      endChunk();
    }
    return chunk == null ? List.of() : List.of(chunk);
  }

  @Override
  public ChunkLayout getChunkLayout() {
    return chunkLayout;
  }

  @Override
  public void setChunkLayout(final ChunkLayout chunkLayout) {
    this.chunkLayout = chunkLayout;
  }

  // ----------------------- Unified Encoder Trigger -----------------------

  @Override
  public void endChunk() {
    if (pageBuffers.isEmpty()) {
      return;
    }
    try {
      encodeAndBuildChunk();
      pageBuffers.clear();
      needDecodeChunk = false;
    } catch (final IOException | PageException e) {
      throw new IllegalStateException(
          StorageEngineMessages.EXCEPTION_FAILED_TO_ENCODE_CHUNK_USING_CHUNKWRITERIMPL_260BF917, e);
    }
  }

  private void encodeAndBuildChunk() throws IOException, PageException {
    ChunkWriterImpl chunkWriter =
        new ChunkWriterImpl(
            new MeasurementSchema(
                chunkHeader.getMeasurementID(),
                chunkHeader.getDataType(),
                chunkHeader.getEncodingType(),
                chunkHeader.getCompressionType()),
            encryptParameter);

    for (PageBuffer page : pageBuffers) {
      if (page.needDecode) {
        final long partitionStart = timePartitionSlot.getStartTime();
        final long partitionEnd = getTimePartitionEnd(partitionStart);
        boolean hasPoint = false;
        for (int j = 0; j < page.timeBatch.length; j++) {
          final long time = page.timeBatch[j];
          if (time < partitionStart || time > partitionEnd) {
            continue;
          }
          writePointToChunkWriter(chunkWriter, time, page.valueBatch[j]);
          hasPoint = true;
        }
        if (hasPoint) {
          chunkWriter.sealCurrentPage();
        }
      } else {
        // Skip empty pages: writing the bare page header would produce a chunk whose data size does
        // not describe a parsable page.
        if (page.pageData.remaining() > 0) {
          chunkWriter.writePageHeaderAndDataIntoBuff(page.pageData, page.pageHeader);
        }
      }
    }

    // The buffered pages may have been split and re-encoded, so the source ChunkHeader no longer
    // describes the resulting payload. Rebuild it from the actual encoded bytes and page count.
    final ByteBuffer chunkData = chunkWriter.getByteBuffer();
    final Statistics<?> statistics = chunkWriter.getStatistics();
    final ChunkHeader encodedHeader =
        new ChunkHeader(
            chunkHeader.getMeasurementID(),
            chunkData.remaining(),
            chunkHeader.getDataType(),
            chunkHeader.getCompressionType(),
            chunkHeader.getEncodingType(),
            chunkWriter.getNumOfPages(),
            chunkHeader.getChunkType() & (TIME_COLUMN_MASK | VALUE_COLUMN_MASK));
    this.chunk = new Chunk(encodedHeader, chunkData, null, statistics);

    // Update global data size
    this.dataSize += chunkData.remaining() + statistics.getSerializedSize();
  }

  private static long getTimePartitionEnd(final long partitionStart) {
    // Delegate to the overflow-safe helper: a naive "start + interval - 1" wraps around for the
    // last representable partition and collapses to the start itself when interval == 1.
    return TimePartitionUtils.getTimePartitionEndTime(partitionStart);
  }

  private void writePointToChunkWriter(
      ChunkWriterImpl chunkWriter, final long time, final Object value) {
    TypeServices.StorageEngine.OBJECT_VALUE_CHUNK_WRITER_SERVICE
        .call(Type.fromTsDataType(chunkHeader.getDataType()))
        .write(chunkWriter, time, value);
  }

  // ----------------------- Serialization / Deserialization -----------------------

  @Override
  public void serialize(final DataOutputStream stream) throws IOException {
    serialize(stream, true);
  }

  @Override
  public void serialize(final DataOutputStream stream, final boolean includeContent)
      throws IOException {
    ReadWriteIOUtils.write(getType().ordinal(), stream);
    ReadWriteIOUtils.write(isAligned(), stream);

    // Encode the buffered pages first: the header written by serializeAttr must describe the very
    // payload that is written below, and the source header no longer matches it once the pages
    // have been split or re-encoded.
    if (!pageBuffers.isEmpty()) {
      endChunk();
    }

    serializeAttr(stream);
    writeChunkLayout(stream);

    // Write the standard chunk out natively, or reference its payload in the staged file
    if (chunk != null) {
      chunk.getChunkStatistic().serialize(stream);
      ChunkData.serializeChunkPayload(stream, chunk.getData(), payloadRefAt(0), includeContent);
    }
  }

  private ChunkPayloadRef payloadRefAt(final int index) {
    return index < chunkPayloadRefs.size() ? chunkPayloadRefs.get(index) : null;
  }

  @Override
  public List<ChunkPayloadRef> getChunkPayloadRefs() {
    return chunkPayloadRefs;
  }

  @Override
  public void setChunkPayloadRefs(final List<ChunkPayloadRef> chunkPayloadRefs) {
    this.chunkPayloadRefs = chunkPayloadRefs;
  }

  private void serializeAttr(final DataOutputStream stream) throws IOException {
    ReadWriteIOUtils.write(timePartitionSlot.getStartTime(), stream);
    ReadWriteIOUtils.write(device instanceof StringArrayDeviceID, stream);
    device.serialize(stream);
    ReadWriteIOUtils.write(dataSize, stream);
    // Data has been standardized, so needDecodeChunk is always false on the wire
    ReadWriteIOUtils.write(false, stream);
    // The pages may have been split and re-encoded, in which case the chunk header rebuilt from the
    // actual encoded bytes is the one describing the payload that follows.
    (chunk == null ? chunkHeader : chunk.getHeader()).serializeTo(stream);
  }

  private void writeChunkLayout(final DataOutputStream stream) throws IOException {
    ReadWriteIOUtils.write(chunkLayout != null, stream);
    if (chunkLayout != null) {
      ReadWriteIOUtils.write(chunkLayout.chunkGroupIndex(), stream);
      ReadWriteIOUtils.write(chunkLayout.chunkGroupHeaderOffset(), stream);
      ReadWriteIOUtils.write(chunkLayout.offset(), stream);
      ReadWriteIOUtils.write(chunkLayout.length(), stream);
      ReadWriteIOUtils.write(chunkLayout.chunkIndexInGroup(), stream);
      ReadWriteIOUtils.write(chunkLayout.firstChunkOfGroup(), stream);
    }
  }

  public static NonAlignedChunkData deserialize(final InputStream stream)
      throws IOException, PageException {
    final TTimePartitionSlot timePartitionSlot =
        TimePartitionUtils.getTimePartitionSlot(ReadWriteIOUtils.readLong(stream));
    final boolean isStringArrayDeviceID = ReadWriteIOUtils.readBool(stream);
    final IDeviceID device =
        isStringArrayDeviceID
            ? StringArrayDeviceID.deserialize(stream)
            : PlainDeviceID.deserialize(stream).convertToStringArrayDeviceId();
    final long dataSize = ReadWriteIOUtils.readLong(stream);

    // Ignore needDecodeChunk flag from stream since it's now always standardized to false
    ReadWriteIOUtils.readBool(stream);

    final byte chunkType = ReadWriteIOUtils.readByte(stream);
    final ChunkHeader chunkHeader = ChunkHeader.deserializeFrom(stream, chunkType);
    String measurementID = chunkHeader.getMeasurementID();
    chunkHeader.setMeasurementID(MEASUREMENT_ID_CACHE.get(measurementID, m -> m));

    final NonAlignedChunkData chunkData =
        new NonAlignedChunkData(device, chunkHeader, timePartitionSlot);
    chunkData.dataSize = dataSize;
    chunkData.needDecodeChunk = false; // Always false after deserialization

    if (ReadWriteIOUtils.readBool(stream)) {
      chunkData.chunkLayout =
          new ChunkLayout(
              ReadWriteIOUtils.readLong(stream),
              ReadWriteIOUtils.readLong(stream),
              ReadWriteIOUtils.readLong(stream),
              ReadWriteIOUtils.readLong(stream),
              ReadWriteIOUtils.readInt(stream),
              ReadWriteIOUtils.readBool(stream));
    }

    chunkData.deserializeTsFileData(stream);

    return chunkData;
  }

  private void deserializeTsFileData(final InputStream stream) throws IOException {
    Statistics<? extends Serializable> statistics =
        Statistics.deserialize(stream, chunkHeader.getDataType());
    final ByteBuffer payload = ChunkData.deserializeChunkPayload(stream, chunkPayloadRefs);

    this.chunk = new Chunk(chunkHeader, payload, null, statistics);
  }

  // ----------------------- Write to TsFile -----------------------

  @Override
  public void writeToFileWriter(final TsFileIOWriter writer) throws IOException, PageException {
    // Fallback trigger for encoding
    if (!pageBuffers.isEmpty()) {
      endChunk();
    }
    if (chunk != null) {
      writer.writeChunk(chunk);
    }
  }

  @Override
  public String toString() {
    return "NonAlignedChunkData{"
        + "dataSize="
        + dataSize
        + ", timePartitionSlot="
        + timePartitionSlot
        + ", device='"
        + device
        + '\''
        + ", chunkHeader="
        + chunkHeader
        + ", needDecodeChunk="
        + needDecodeChunk
        + '}';
  }
}
