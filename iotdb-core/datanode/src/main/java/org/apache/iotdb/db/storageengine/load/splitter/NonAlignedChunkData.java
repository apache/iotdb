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
import org.apache.tsfile.utils.Binary;
import org.apache.tsfile.utils.ReadWriteIOUtils;
import org.apache.tsfile.write.UnSupportedDataTypeException;
import org.apache.tsfile.write.chunk.ChunkWriterImpl;
import org.apache.tsfile.write.schema.MeasurementSchema;
import org.apache.tsfile.write.writer.TsFileIOWriter;

import javax.annotation.Nonnull;

import java.io.DataInputStream;
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
      throw new IllegalStateException("Failed to encode chunk using ChunkWriterImpl", e);
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
        for (int j = 0; j < page.satisfiedLength; j++) {
          writePointToChunkWriter(chunkWriter, page.timeBatch[j], page.valueBatch[j]);
        }
        chunkWriter.sealCurrentPage();
      } else {
        chunkWriter.writePageHeaderAndDataIntoBuff(page.pageData, page.pageHeader);
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

  private void writePointToChunkWriter(
      ChunkWriterImpl chunkWriter, final long time, final Object value) {
    switch (chunkHeader.getDataType()) {
      case INT32:
      case DATE:
        chunkWriter.write(time, (int) value);
        break;
      case INT64:
      case TIMESTAMP:
        chunkWriter.write(time, (long) value);
        break;
      case FLOAT:
        chunkWriter.write(time, (float) value);
        break;
      case DOUBLE:
        chunkWriter.write(time, (double) value);
        break;
      case BOOLEAN:
        chunkWriter.write(time, (boolean) value);
        break;
      case TEXT:
      case BLOB:
      case STRING:
        chunkWriter.write(time, (Binary) value);
        break;
      default:
        throw new UnSupportedDataTypeException(
            String.format(
                StorageEngineMessages.STORAGE_EXCEPTION_DATA_TYPE_S_IS_NOT_SUPPORTED_5D5C02E4,
                chunkHeader.getDataType()));
    }
  }

  // ----------------------- Serialization / Deserialization -----------------------

  @Override
  public void serialize(final DataOutputStream stream) throws IOException {
    ReadWriteIOUtils.write(getType().ordinal(), stream);
    ReadWriteIOUtils.write(isAligned(), stream);
    serializeAttr(stream);
    writeChunkLayout(stream);

    // Ensure all internal pages are encoded to chunks if not already
    if (!pageBuffers.isEmpty()) {
      endChunk();
    }

    // Write the standard chunk out natively
    if (chunk != null) {
      chunk.getChunkStatistic().serialize(stream);
      ByteBuffer data = chunk.getData();
      ReadWriteIOUtils.write(data.remaining(), stream);
      stream.write(data.array(), data.arrayOffset() + data.position(), data.remaining());
    }
  }

  private void serializeAttr(final DataOutputStream stream) throws IOException {
    ReadWriteIOUtils.write(timePartitionSlot.getStartTime(), stream);
    ReadWriteIOUtils.write(device instanceof StringArrayDeviceID, stream);
    device.serialize(stream);
    ReadWriteIOUtils.write(dataSize, stream);
    // Data has been standardized, so needDecodeChunk is always false on the wire
    ReadWriteIOUtils.write(false, stream);
    chunkHeader.serializeTo(stream);
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
    int dataBytesSize = ReadWriteIOUtils.readInt(stream);
    byte[] dataBytes = new byte[dataBytesSize];
    new DataInputStream(stream).readFully(dataBytes);

    this.chunk = new Chunk(chunkHeader, ByteBuffer.wrap(dataBytes), null, statistics);
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
