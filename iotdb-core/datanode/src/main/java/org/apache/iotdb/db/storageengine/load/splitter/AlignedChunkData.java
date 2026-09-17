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
import org.apache.tsfile.enums.TSDataType;
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
import org.apache.tsfile.utils.TsPrimitiveType;
import org.apache.tsfile.write.UnSupportedDataTypeException;
import org.apache.tsfile.write.chunk.AlignedChunkWriterImpl;
import org.apache.tsfile.write.chunk.ValueChunkWriter;
import org.apache.tsfile.write.schema.IMeasurementSchema;
import org.apache.tsfile.write.schema.MeasurementSchema;
import org.apache.tsfile.write.writer.TsFileIOWriter;

import javax.annotation.Nonnull;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;
import java.util.Set;

import static org.apache.iotdb.db.storageengine.load.LoadTsFileManager.MEASUREMENT_ID_CACHE;
import static org.apache.tsfile.common.constant.TsFileConstant.TIME_COLUMN_MASK;
import static org.apache.tsfile.common.constant.TsFileConstant.VALUE_COLUMN_MASK;

public class AlignedChunkData implements ChunkData {
  protected static final int DEFAULT_INT32 = 0;
  protected static final long DEFAULT_INT64 = 0L;
  protected static final float DEFAULT_FLOAT = 0;
  protected static final double DEFAULT_DOUBLE = 0.0;
  protected static final boolean DEFAULT_BOOLEAN = false;
  protected static final Binary DEFAULT_BINARY = null;

  // ---------------- Permanent state: describes this ChunkData and its final result
  // ----------------
  protected final TTimePartitionSlot timePartitionSlot;
  protected final IDeviceID device;
  protected final EncryptParameter encryptParameter;
  protected List<ChunkHeader> chunkHeaderList;

  /** Final storage for complete Chunks; written directly when decoding is not required. */
  protected final List<Chunk> entireChunks = new ArrayList<>();

  protected long dataSize;
  protected boolean needDecodeChunk;

  // ---------------- Temporary state: used only while the splitter collects Pages ----------------
  /** Pages that are not encoded yet; built and cleared together in {@code endChunk()}. */
  protected final List<PageBuffer> pageBuffers = new ArrayList<>();

  /** Index of the Chunk currently being written: 0 = Time Chunk, 1..n = Value Chunks. */
  protected int currentChunkIndex = -1;

  protected ChunkLayout chunkLayout;

  /** Temporary time batches used for aligned row-level splitting. */
  protected List<long[]> timeBatch;

  protected List<Integer> pageNumbers;
  protected Queue<Integer> satisfiedLengthQueue;

  protected static class PageBuffer {
    final int chunkIndex;
    final boolean needDecode;
    final PageHeader pageHeader;
    final ByteBuffer pageData;
    final long[] timeBatch;
    final TsPrimitiveType[] valueBatch;
    final int satisfiedLength;

    PageBuffer(int chunkIndex, PageHeader pageHeader, ByteBuffer pageData) {
      this.chunkIndex = chunkIndex;
      this.needDecode = false;
      this.pageHeader = pageHeader;
      this.pageData = pageData;
      this.timeBatch = null;
      this.valueBatch = null;
      this.satisfiedLength = 0;
    }

    PageBuffer(int chunkIndex, long[] timeBatch, int satisfiedLength) {
      this.chunkIndex = chunkIndex;
      this.needDecode = true;
      this.pageHeader = null;
      this.pageData = null;
      this.timeBatch = timeBatch;
      this.valueBatch = null;
      this.satisfiedLength = satisfiedLength;
    }

    PageBuffer(
        int chunkIndex, long[] timeBatch, TsPrimitiveType[] valueBatch, int satisfiedLength) {
      this.chunkIndex = chunkIndex;
      this.needDecode = true;
      this.pageHeader = null;
      this.pageData = null;
      this.timeBatch = timeBatch;
      this.valueBatch = valueBatch;
      this.satisfiedLength = satisfiedLength;
    }
  }

  public AlignedChunkData(
      @Nonnull final IDeviceID device,
      final ChunkHeader chunkHeader,
      final TTimePartitionSlot timePartitionSlot) {
    this(device, chunkHeader, timePartitionSlot, EncryptUtils.getEncryptParameter());
  }

  public AlignedChunkData(
      @Nonnull final IDeviceID device,
      final ChunkHeader chunkHeader,
      final TTimePartitionSlot timePartitionSlot,
      final EncryptParameter encryptParameter) {
    this(device, timePartitionSlot, encryptParameter);
    chunkHeaderList.add(chunkHeader);
    pageNumbers.add(0);
    currentChunkIndex = 0;
    addAttrDataSize();
  }

  protected AlignedChunkData(final AlignedChunkData alignedChunkData) {
    this(
        alignedChunkData.device,
        alignedChunkData.timePartitionSlot,
        alignedChunkData.encryptParameter);
    this.satisfiedLengthQueue = new LinkedList<>(alignedChunkData.satisfiedLengthQueue);
    this.needDecodeChunk = alignedChunkData.needDecodeChunk;
    addAttrDataSize();
  }

  protected AlignedChunkData(
      @Nonnull final IDeviceID device, final TTimePartitionSlot timePartitionSlot) {
    this(device, timePartitionSlot, EncryptUtils.getEncryptParameter());
  }

  protected AlignedChunkData(
      @Nonnull final IDeviceID device,
      final TTimePartitionSlot timePartitionSlot,
      final EncryptParameter encryptParameter) {
    this.dataSize = 0;
    this.device = device;
    this.encryptParameter = encryptParameter;
    this.chunkHeaderList = new ArrayList<>();
    this.timePartitionSlot = timePartitionSlot;
    this.needDecodeChunk = true;
    this.pageNumbers = new ArrayList<>();
    this.satisfiedLengthQueue = new LinkedList<>();
  }

  private void addAttrDataSize() {
    dataSize += 2 * Byte.BYTES;
    dataSize += Long.BYTES;
    dataSize += device.serializedSize();
    dataSize += Integer.BYTES;
    if (!chunkHeaderList.isEmpty()) {
      dataSize += chunkHeaderList.get(0).getSerializedSize();
    }
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
    return true;
  }

  @Override
  public boolean isEntireChunk() {
    return !needDecodeChunk;
  }

  public void addValueChunk(final ChunkHeader chunkHeader) {
    this.chunkHeaderList.add(chunkHeader);
    this.pageNumbers.add(0);
    currentChunkIndex++;
    dataSize += chunkHeader.getSerializedSize();
    if (needDecodeChunk) {
      dataSize += Integer.BYTES;
    }
  }

  @Override
  public List<Chunk> getChunks() {
    if (!pageBuffers.isEmpty()) {
      endChunk();
    }
    return new ArrayList<>(entireChunks);
  }

  @Override
  public ChunkLayout getChunkLayout() {
    return chunkLayout;
  }

  @Override
  public void setChunkLayout(final ChunkLayout chunkLayout) {
    this.chunkLayout = chunkLayout;
  }

  // ----------------------- Data Collection (In-Memory) -----------------------

  @Override
  public void writeEntireChunk(final ByteBuffer chunkData, final IChunkMetadata chunkMetadata)
      throws IOException {
    if (chunkHeaderList.isEmpty()) {
      throw new IOException("Chunk header list is empty.");
    }
    final ChunkHeader currentChunkHeader = chunkHeaderList.get(chunkHeaderList.size() - 1);

    // Store complete chunk data directly into memory without decoding
    entireChunks.add(new Chunk(currentChunkHeader, chunkData, null, chunkMetadata.getStatistics()));
    needDecodeChunk = false;
    dataSize += chunkData.remaining() + chunkMetadata.getStatistics().getSerializedSize();
  }

  @Override
  public void writeEntirePage(final PageHeader pageHeader, final ByteBuffer pageData)
      throws IOException {
    pageNumbers.set(pageNumbers.size() - 1, pageNumbers.get(pageNumbers.size() - 1) + 1);
    // Wrap the un-decoded page and store it into the buffer
    pageBuffers.add(new PageBuffer(currentChunkIndex, pageHeader, pageData));
  }

  @Override
  public void writeDecodePage(final long[] times, final Object[] values, final int satisfiedLength)
      throws IOException {
    pageNumbers.set(pageNumbers.size() - 1, pageNumbers.get(pageNumbers.size() - 1) + 1);
    satisfiedLengthQueue.offer(satisfiedLength);

    if (currentChunkIndex == 0 || values == null) {
      pageBuffers.add(new PageBuffer(currentChunkIndex, times, satisfiedLength));
    } else {
      final TsPrimitiveType[] alignedValues = new TsPrimitiveType[values.length];
      for (int i = 0; i < values.length; i++) {
        alignedValues[i] = (TsPrimitiveType) values[i];
      }
      pageBuffers.add(new PageBuffer(currentChunkIndex, times, alignedValues, satisfiedLength));
    }
  }

  public void writeDecodeValuePage(
      final long[] times, final TsPrimitiveType[] values, final TSDataType dataType)
      throws IOException {
    pageNumbers.set(pageNumbers.size() - 1, pageNumbers.get(pageNumbers.size() - 1) + 1);
    final int satisfiedLength = satisfiedLengthQueue.poll();
    satisfiedLengthQueue.offer(satisfiedLength);
    pageBuffers.add(new PageBuffer(currentChunkIndex, times, values, satisfiedLength));
  }

  // ----------------------- Unified Encoder Trigger -----------------------

  @Override
  public void endChunk() {
    if (pageBuffers.isEmpty()) {
      return;
    }
    try {
      // Force encoding of buffered pages into standard Chunk objects
      encodeAndBuildChunks();

      // Clear the temporary buffers
      pageBuffers.clear();
      // Mark as fully processed standard chunks
      needDecodeChunk = false;
    } catch (final IOException | PageException e) {
      throw new IllegalStateException("Failed to encode chunk using AlignedChunkWriterImpl", e);
    }
  }

  protected void encodeAndBuildChunks() throws IOException, PageException {
    final AlignedChunkWriterImpl writer = createAlignedChunkWriter();

    writePageBuffersToWriter(writer);

    // One aligned ChunkData may accumulate pages from several physical Chunks before it is
    // consumed. Encode every Chunk represented by the current buffers, while leaving Chunks that
    // were sealed by an earlier endChunk() untouched.
    final Set<Integer> bufferedChunkIndexes = new LinkedHashSet<>();
    for (final PageBuffer pageBuffer : pageBuffers) {
      bufferedChunkIndexes.add(pageBuffer.chunkIndex);
    }
    for (final int chunkIndex : bufferedChunkIndexes) {
      final ChunkHeader sourceHeader = chunkHeaderList.get(chunkIndex);
      final ByteBuffer encodedData;
      final Statistics<?> statistics;
      final int pageCount;
      final int mask;
      if (chunkIndex == 0) {
        encodedData = writer.getTimeChunkWriter().getByteBuffer();
        statistics = writer.getTimeChunkWriter().getStatistics();
        pageCount = writer.getTimeChunkWriter().getNumOfPages();
        mask = TIME_COLUMN_MASK;
      } else {
        final ValueChunkWriter valueWriter = writer.getValueChunkWriterList().get(chunkIndex - 1);
        encodedData = valueWriter.getByteBuffer();
        statistics = valueWriter.getStatistics();
        pageCount = valueWriter.getNumOfPages();
        mask = VALUE_COLUMN_MASK;
      }

      final ChunkHeader encodedHeader =
          createEncodedChunkHeader(sourceHeader, encodedData.remaining(), pageCount, mask);
      entireChunks.add(new Chunk(encodedHeader, encodedData, null, statistics));
      dataSize += encodedData.remaining() + statistics.getSerializedSize();
    }
  }

  protected static ChunkHeader createEncodedChunkHeader(
      final ChunkHeader sourceHeader,
      final int encodedDataSize,
      final int pageCount,
      final int mask) {
    return new ChunkHeader(
        sourceHeader.getMeasurementID(),
        encodedDataSize,
        sourceHeader.getDataType(),
        sourceHeader.getCompressionType(),
        sourceHeader.getEncodingType(),
        pageCount,
        mask);
  }

  private AlignedChunkWriterImpl createAlignedChunkWriter() {
    IMeasurementSchema timeSchema = null;
    final List<IMeasurementSchema> valueSchemaList = new ArrayList<>();
    for (final ChunkHeader header : chunkHeaderList) {
      final MeasurementSchema schema =
          new MeasurementSchema(
              header.getMeasurementID(),
              header.getDataType(),
              header.getEncodingType(),
              header.getCompressionType());
      if (header.getDataType() == TSDataType.VECTOR) {
        timeSchema = schema;
      } else {
        valueSchemaList.add(schema);
      }
    }
    return new AlignedChunkWriterImpl(timeSchema, valueSchemaList, encryptParameter);
  }

  private void writePageBuffersToWriter(final AlignedChunkWriterImpl writer)
      throws IOException, PageException {
    for (final PageBuffer page : pageBuffers) {
      if (!page.needDecode) {
        if (page.chunkIndex == 0) {
          writer.writePageHeaderAndDataIntoTimeBuff(page.pageData, page.pageHeader);
        } else {
          writer.writePageHeaderAndDataIntoValueBuff(
              page.pageData, page.pageHeader, page.chunkIndex - 1);
        }
        continue;
      }
      if (page.valueBatch == null) {
        boolean hasPoint = false;
        for (final long time : page.timeBatch) {
          if (!isInCurrentTimePartition(time)) {
            continue;
          }
          writer.writeTime(time);
          hasPoint = true;
        }
        if (hasPoint) {
          writer.sealCurrentTimePage();
        }
      } else {
        final TSDataType dataType = chunkHeaderList.get(page.chunkIndex).getDataType();
        boolean hasPoint = false;
        for (int i = 0; i < page.timeBatch.length; i++) {
          if (!isInCurrentTimePartition(page.timeBatch[i])) {
            continue;
          }
          writeValueToAlignedChunkWriter(
              writer, page.timeBatch[i], page.valueBatch[i], dataType, page.chunkIndex - 1);
          hasPoint = true;
        }
        if (hasPoint) {
          writer.sealCurrentValuePage(page.chunkIndex - 1);
        }
      }
    }
  }

  protected boolean isInCurrentTimePartition(final long time) {
    final long partitionStart = timePartitionSlot.getStartTime();
    final long partitionEnd = partitionStart + TimePartitionUtils.getTimePartitionInterval() - 1;
    return time >= partitionStart && (partitionEnd <= partitionStart || time <= partitionEnd);
  }

  private void writeValueToAlignedChunkWriter(
      final AlignedChunkWriterImpl writer,
      final long time,
      final TsPrimitiveType value,
      final TSDataType dataType,
      final int valueChunkIndex) {
    final boolean isNull = value == null;
    switch (dataType) {
      case INT32:
      case DATE:
        writer.write(time, isNull ? DEFAULT_INT32 : value.getInt(), isNull, valueChunkIndex);
        break;
      case INT64:
      case TIMESTAMP:
        writer.write(time, isNull ? DEFAULT_INT64 : value.getLong(), isNull, valueChunkIndex);
        break;
      case FLOAT:
        writer.write(time, isNull ? DEFAULT_FLOAT : value.getFloat(), isNull, valueChunkIndex);
        break;
      case DOUBLE:
        writer.write(time, isNull ? DEFAULT_DOUBLE : value.getDouble(), isNull, valueChunkIndex);
        break;
      case BOOLEAN:
        writer.write(time, isNull ? DEFAULT_BOOLEAN : value.getBoolean(), isNull, valueChunkIndex);
        break;
      case TEXT:
      case BLOB:
      case STRING:
        writer.write(time, isNull ? DEFAULT_BINARY : value.getBinary(), isNull, valueChunkIndex);
        break;
      default:
        throw new UnSupportedDataTypeException(
            String.format(
                StorageEngineMessages.STORAGE_EXCEPTION_DATA_TYPE_S_IS_NOT_SUPPORTED_5D5C02E4,
                dataType));
    }
  }

  // ----------------------- Serialization / Deserialization -----------------------

  @Override
  public void serialize(final DataOutputStream stream) throws IOException {
    ReadWriteIOUtils.write(getType().ordinal(), stream);

    // Always false now since we ensure all outputs are encoded Chunk objects
    ReadWriteIOUtils.write(isAligned(), stream);
    serializeAttr(stream);
    writeChunkLayout(stream);

    // Fallback trigger: ensure un-processed buffers are encoded before serialization
    if (!pageBuffers.isEmpty()) {
      endChunk();
    }

    // Write all standard chunks out natively
    ReadWriteIOUtils.write(entireChunks.size(), stream);
    for (Chunk chunk : entireChunks) {
      chunk.getHeader().serializeTo(stream);
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

    // needDecodeChunk will be false by the time we reach here if endChunk is triggered
    ReadWriteIOUtils.write(needDecodeChunk, stream);
    ReadWriteIOUtils.write(chunkHeaderList.size(), stream);
    for (final ChunkHeader chunkHeader : chunkHeaderList) {
      chunkHeader.serializeTo(stream);
    }
    if (needDecodeChunk) {
      for (final Integer pageNumber : pageNumbers) {
        ReadWriteIOUtils.write(pageNumber, stream);
      }
    }
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

  public static AlignedChunkData deserialize(final InputStream stream)
      throws IOException, PageException {
    final TTimePartitionSlot timePartitionSlot =
        TimePartitionUtils.getTimePartitionSlot(ReadWriteIOUtils.readLong(stream));
    final boolean isStringArrayDeviceID = ReadWriteIOUtils.readBool(stream);
    final IDeviceID device =
        isStringArrayDeviceID
            ? StringArrayDeviceID.deserialize(stream)
            : PlainDeviceID.deserialize(stream).convertToStringArrayDeviceId();
    final long dataSize = ReadWriteIOUtils.readLong(stream);
    final boolean needDecodeChunk = ReadWriteIOUtils.readBool(stream);
    final int chunkHeaderListSize = ReadWriteIOUtils.readInt(stream);

    final List<ChunkHeader> chunkHeaderList = new ArrayList<>();
    for (int i = 0; i < chunkHeaderListSize; i++) {
      final byte chunkType = ReadWriteIOUtils.readByte(stream);
      ChunkHeader chunkHeader = ChunkHeader.deserializeFrom(stream, chunkType);
      String measurementID = chunkHeader.getMeasurementID();
      chunkHeader.setMeasurementID(MEASUREMENT_ID_CACHE.get(measurementID, m -> m));
      chunkHeaderList.add(chunkHeader);
    }

    final List<Integer> pageNumbers = new ArrayList<>();
    if (needDecodeChunk) {
      for (int i = 0; i < chunkHeaderListSize; i++) {
        pageNumbers.add(ReadWriteIOUtils.readInt(stream));
      }
    }

    final AlignedChunkData chunkData;
    if (chunkHeaderList.get(0).getMeasurementID().equals("")) {
      chunkData = new AlignedChunkData(device, chunkHeaderList.get(0), timePartitionSlot);
    } else {
      chunkData = new BatchedAlignedValueChunkData(device, timePartitionSlot);
    }
    chunkData.needDecodeChunk = needDecodeChunk;
    chunkData.chunkHeaderList = chunkHeaderList;
    chunkData.pageNumbers = pageNumbers;
    chunkData.dataSize = dataSize;

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

  protected void deserializeTsFileData(final InputStream stream) throws IOException {
    int chunkCount = ReadWriteIOUtils.readInt(stream);
    for (int i = 0; i < chunkCount; i++) {
      byte chunkType = ReadWriteIOUtils.readByte(stream);
      ChunkHeader header = ChunkHeader.deserializeFrom(stream, chunkType);
      Statistics statistics = Statistics.deserialize(stream, header.getDataType());

      int dataBytesSize = ReadWriteIOUtils.readInt(stream);
      byte[] dataBytes = new byte[dataBytesSize];
      new DataInputStream(stream).readFully(dataBytes);

      entireChunks.add(new Chunk(header, ByteBuffer.wrap(dataBytes), null, statistics));
    }
    // Deserialized chunks are now fully standard
    this.needDecodeChunk = false;
  }

  // ----------------------- Write to TsFile -----------------------

  @Override
  public void writeToFileWriter(final TsFileIOWriter writer) throws IOException, PageException {
    // Ensure all internal pages are encoded to chunks if not already
    if (!pageBuffers.isEmpty()) {
      endChunk();
    }

    // Everything is now standardized as complete chunks in memory, directly write to disk
    for (final Chunk chunk : entireChunks) {
      writer.writeChunk(chunk);
    }
  }

  @Override
  public String toString() {
    return "AlignedChunkData{"
        + "timePartitionSlot="
        + timePartitionSlot
        + ", device='"
        + device
        + '\''
        + ", chunkHeaderList="
        + chunkHeaderList
        + ", totalDataSize="
        + dataSize
        + ", needDecodeChunk="
        + needDecodeChunk
        + '}';
  }
}
