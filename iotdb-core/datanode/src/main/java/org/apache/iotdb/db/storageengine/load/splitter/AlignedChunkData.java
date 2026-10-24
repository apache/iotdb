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
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.exception.write.PageException;
import org.apache.tsfile.file.MetaMarker;
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
import org.apache.tsfile.utils.TsPrimitiveType;
import org.apache.tsfile.write.chunk.AlignedChunkWriterImpl;
import org.apache.tsfile.write.chunk.ValueChunkWriter;
import org.apache.tsfile.write.schema.IMeasurementSchema;
import org.apache.tsfile.write.schema.MeasurementSchema;
import org.apache.tsfile.write.writer.TsFileIOWriter;

import javax.annotation.Nonnull;

import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.BufferOverflowException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;

import static org.apache.iotdb.db.storageengine.load.LoadTsFileManager.MEASUREMENT_ID_CACHE;
import static org.apache.tsfile.common.constant.TsFileConstant.TIME_COLUMN_MASK;
import static org.apache.tsfile.common.constant.TsFileConstant.VALUE_COLUMN_MASK;

public class AlignedChunkData implements ChunkData {
  // ---------------- Permanent state: describes this ChunkData and its final result
  // ----------------
  protected final TTimePartitionSlot timePartitionSlot;
  protected final IDeviceID device;
  protected final EncryptParameter encryptParameter;
  protected List<ChunkHeader> chunkHeaderList;

  /** Final storage for complete Chunks; written directly when decoding is not required. */
  protected final List<Chunk> entireChunks = new ArrayList<>();

  /** Where the payloads of {@link #entireChunks} live in the staged TsFile. */
  protected List<ChunkPayloadRef> chunkPayloadRefs = new ArrayList<>();

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
    chunkHeaderList.add(ChunkData.transportHeader(chunkHeader));
    pageNumbers.add(0);
    currentChunkIndex = 0;
    addAttrDataSize();
  }

  /**
   * Detaches a consumed chunk data from the splitter: the copy is stored back into the map the
   * splitter keeps draining, so the pages that follow land in a fresh object instead of the object
   * that was just handed to the consumer. Without the copy the same object would be consumed again
   * with every later flush and would re-emit the chunks it already emitted, which leaves the split
   * TsFile with more time chunks than each value column has chunks.
   */
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
    this.chunkHeaderList.add(ChunkData.transportHeader(chunkHeader));
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
      throw new IOException(StorageEngineMessages.EXCEPTION_CHUNK_HEADER_LIST_IS_EMPTY_5DE68C78);
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
      throw new IllegalStateException(
          StorageEngineMessages
              .EXCEPTION_FAILED_TO_ENCODE_CHUNK_USING_ALIGNEDCHUNKWRITERIMPL_5AAF9AA8,
          e);
    }
  }

  protected void encodeAndBuildChunks() throws IOException, PageException {
    // Group the buffered pages by chunk once; scanning the whole page list once per chunk turns
    // wide aligned tables into O(columns * pages).
    final Map<Integer, List<PageBuffer>> pagesByChunkIndex = new LinkedHashMap<>();
    for (final PageBuffer pageBuffer : pageBuffers) {
      pagesByChunkIndex
          .computeIfAbsent(pageBuffer.chunkIndex, ignored -> new ArrayList<>())
          .add(pageBuffer);
    }

    // Fast-path: assemble chunks that need no splitting directly, avoiding re-encoding and its
    // buffer amplification.
    final Map<Integer, Chunk> directlyBuiltChunks = new LinkedHashMap<>();
    for (final Map.Entry<Integer, List<PageBuffer>> entry : pagesByChunkIndex.entrySet()) {
      final int chunkIndex = entry.getKey();
      final Chunk chunk =
          buildChunkFromUndecodedPages(
              chunkHeaderList.get(chunkIndex),
              chunkIndex == 0 ? TIME_COLUMN_MASK : VALUE_COLUMN_MASK,
              entry.getValue());
      if (chunk != null) {
        directlyBuiltChunks.put(chunkIndex, chunk);
      }
    }

    final AlignedChunkWriterImpl writer =
        directlyBuiltChunks.size() == pagesByChunkIndex.size() ? null : createAlignedChunkWriter();
    if (writer != null) {
      writePageBuffersToWriter(writer, directlyBuiltChunks.keySet());
    }

    for (final int chunkIndex : pagesByChunkIndex.keySet()) {
      final Chunk directChunk = directlyBuiltChunks.get(chunkIndex);
      if (directChunk != null) {
        entireChunks.add(directChunk);
        dataSize +=
            directChunk.getData().remaining() + directChunk.getChunkStatistic().getSerializedSize();
        continue;
      }

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

  /**
   * Reassembles a chunk from pages that were stored without decoding. Returns {@code null} when the
   * chunk has a decoded page, a non-empty page without statistics, or no page with data at all; the
   * caller then falls back to {@link AlignedChunkWriterImpl}.
   */
  protected Chunk buildChunkFromUndecodedPages(
      final ChunkHeader sourceHeader, final int mask, final List<PageBuffer> chunkPages)
      throws IOException {
    // A time chunk header carries VECTOR, and its pages use TimeStatistics rather than the
    // LongStatistics that Statistics.getStatsByType(INT64) would create.
    final TSDataType statisticsType = sourceHeader.getDataType();
    Statistics<?> statistics = null;
    int totalSize = 0;
    int pageCount = 0;
    int emptyPageCount = 0;
    for (final PageBuffer page : chunkPages) {
      if (page.needDecode) {
        // A decoded page is kept as points instead of bytes, so it cannot be copied as it is.
        return null;
      }
      if (page.pageHeader == null) {
        return null;
      }
      if (page.pageData.remaining() == 0) {
        // An empty page (an aligned series stores an all-null column as an empty page) has no data
        // and no statistics, but it still occupies one page slot. The aligned reader pairs the
        // pages of a value chunk with the pages of its time chunk by position, so dropping it would
        // shift every later page of the value chunk onto the wrong timestamps. It is kept as the
        // single 0 varint it serializes to, unless the whole chunk is empty: an all-null chunk
        // keeps the zero-page layout instead.
        emptyPageCount++;
        continue;
      }
      // A non-empty page without statistics belongs to a single-page chunk: the statistics of such
      // a chunk live in the chunk metadata, not in the page header, and cannot be rebuilt from the
      // page bytes. Such a chunk cannot be assembled here and has to go through the writer path.
      if (page.pageHeader.getStatistics() == null) {
        return null;
      }
      if (statistics == null) {
        statistics = Statistics.getStatsByType(statisticsType);
      }
      statistics.mergeStatistics(page.pageHeader.getStatistics());
      try {
        totalSize = Math.addExact(totalSize, page.pageHeader.getSerializedPageSize());
      } catch (final ArithmeticException e) {
        // A chunk that does not fit in one buffer cannot be assembled here: re-encode it instead.
        return null;
      }
      pageCount++;
    }
    if (statistics == null) {
      return null;
    }
    pageCount += emptyPageCount;
    try {
      // Each kept empty page is exactly the one byte of the 0 varint that represents it.
      totalSize = Math.addExact(totalSize, emptyPageCount);
    } catch (final ArithmeticException e) {
      return null;
    }

    final ByteBuffer encodedData = ByteBuffer.allocate(totalSize);
    final OutputStream headerStream = new ByteBufferOutputStream(encodedData);
    try {
      for (final PageBuffer page : chunkPages) {
        // The pages are copied as they were read, so each page header has to serialize back to
        // exactly the bytes getSerializedPageSize() accounted for. Verify that instead of trusting
        // it: a header that serializes longer would overflow the buffer, and a shorter one would
        // leave a chunk whose declared data size does not describe its pages.
        final int expectedPageSize = page.pageHeader.getSerializedPageSize();
        final int pageDataSize = page.pageData.remaining();
        if (expectedPageSize < pageDataSize || expectedPageSize > encodedData.remaining()) {
          return null;
        }
        final int pageStart = encodedData.position();
        page.pageHeader.serializeTo(headerStream);
        if (encodedData.position() - pageStart != expectedPageSize - pageDataSize
            || pageDataSize > encodedData.remaining()) {
          return null;
        }
        encodedData.put(page.pageData.duplicate());
      }
    } catch (final BufferOverflowException e) {
      // Defensive only: the checks above already keep every write within the allocated size.
      return null;
    }
    if (encodedData.position() != totalSize) {
      // Never hand out a chunk whose header advertises more bytes than it carries.
      return null;
    }
    encodedData.flip();
    // A chunk with one page is normally written with ONLY_ONE_PAGE_CHUNK_HEADER, whose page
    // header does not carry statistics. The pages copied here keep their statistics, so use the
    // multi-page marker to make them readable.
    final ChunkHeader encodedHeader =
        pageCount == 1
            ? new ChunkHeader(
                (byte) (MetaMarker.CHUNK_HEADER | mask),
                sourceHeader.getMeasurementID(),
                totalSize,
                sourceHeader.getDataType(),
                sourceHeader.getCompressionType(),
                sourceHeader.getEncodingType())
            : createEncodedChunkHeader(sourceHeader, totalSize, pageCount, mask);
    return new Chunk(encodedHeader, encodedData, null, statistics);
  }

  private static final class ByteBufferOutputStream extends OutputStream {
    private final ByteBuffer buffer;

    private ByteBufferOutputStream(final ByteBuffer buffer) {
      this.buffer = buffer;
    }

    @Override
    public void write(final int b) {
      buffer.put((byte) b);
    }

    @Override
    public void write(final byte[] bytes, final int offset, final int length) {
      buffer.put(bytes, offset, length);
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

  private void writePageBuffersToWriter(
      final AlignedChunkWriterImpl writer, final Set<Integer> directlyBuiltChunkIndexes)
      throws IOException, PageException {
    // Cache partition bounds: resolving them per point performs BigInteger work and allocations.
    final long partitionStart = timePartitionSlot.getStartTime();
    final long partitionEnd = TimePartitionUtils.getTimePartitionEndTime(partitionStart);
    for (final PageBuffer page : pageBuffers) {
      if (directlyBuiltChunkIndexes.contains(page.chunkIndex)) {
        continue;
      }
      if (!page.needDecode) {
        // An empty page (an aligned series stores an all-null column as an empty page) has to be
        // written whenever its chunk also has a page with data: the aligned reader pairs the pages
        // of a value chunk with the pages of its time chunk by position, so dropping the page would
        // shift every later page of the value chunk onto the wrong timestamps. A chunk that has no
        // page with data at all keeps the zero-page layout of an all-null column instead.
        if (page.pageData.remaining() == 0 && !hasPageWithData(page.chunkIndex)) {
          continue;
        }
        if (page.chunkIndex == 0) {
          writer.writePageHeaderAndDataIntoTimeBuff(page.pageData, page.pageHeader);
        } else {
          writer.writePageHeaderAndDataIntoValueBuff(
              page.pageData, page.pageHeader, page.chunkIndex - 1);
        }
        continue;
      }
      // Timestamps within a page are strictly increasing: a chunk group writer rejects an
      // out-of-order point (see Aligned/NonAlignedChunkGroupWriterImpl#checkIsHistoryData), so the
      // points of this partition are always a contiguous slice and the upper bound can start at the
      // lower bound.
      final int from = lowerBound(page.timeBatch, 0, page.timeBatch.length, partitionStart);
      final int to = upperBound(page.timeBatch, from, page.timeBatch.length, partitionEnd);
      if (from >= to) {
        continue;
      }
      if (page.valueBatch == null) {
        for (int i = from; i < to; i++) {
          writer.writeTime(page.timeBatch[i]);
        }
        writer.sealCurrentTimePage();
      } else {
        final TSDataType dataType = chunkHeaderList.get(page.chunkIndex).getDataType();
        for (int i = from; i < to; i++) {
          writeValueToAlignedChunkWriter(
              writer, page.timeBatch[i], page.valueBatch[i], dataType, page.chunkIndex - 1);
        }
        writer.sealCurrentValuePage(page.chunkIndex - 1);
      }
    }
  }

  /**
   * Whether the chunk written here carries at least one point. The empty pages of such a chunk are
   * written as well, so that the page sequence of every value chunk stays aligned with the page
   * sequence of the time chunk; a chunk without any point keeps the zero-page layout of an all-null
   * column. Only empty pages ask this question, so the scan stays off the common path.
   */
  private boolean hasPageWithData(final int chunkIndex) {
    for (final PageBuffer page : pageBuffers) {
      if (page.chunkIndex == chunkIndex
          && (page.needDecode ? page.satisfiedLength > 0 : page.pageData.remaining() > 0)) {
        return true;
      }
    }
    return false;
  }

  private static int lowerBound(final long[] times, final int low, final int high, final long key) {
    int left = low;
    int right = high;
    while (left < right) {
      final int mid = (left + right) >>> 1;
      if (times[mid] < key) {
        left = mid + 1;
      } else {
        right = mid;
      }
    }
    return left;
  }

  private static int upperBound(final long[] times, final int low, final int high, final long key) {
    int left = low;
    int right = high;
    while (left < right) {
      final int mid = (left + right) >>> 1;
      if (times[mid] <= key) {
        left = mid + 1;
      } else {
        right = mid;
      }
    }
    return left;
  }

  private void writeValueToAlignedChunkWriter(
      final AlignedChunkWriterImpl writer,
      final long time,
      final TsPrimitiveType value,
      final TSDataType dataType,
      final int valueChunkIndex) {
    TypeServices.StorageEngine.TS_PRIMITIVE_VALUE_CHUNK_WRITER_SERVICE
        .call(Type.fromTsDataType(dataType))
        .write(writer.getValueChunkWriterByIndex(valueChunkIndex), time, value, value == null);
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

    // The marker the reader uses to route this chunk back to the aligned layout.
    ReadWriteIOUtils.write(isAligned(), stream);
    serializeAttr(stream);
    writeChunkLayout(stream);

    // Fallback trigger: ensure un-processed buffers are encoded before serialization
    if (!pageBuffers.isEmpty()) {
      endChunk();
    }

    // Write all standard chunks out natively, or reference their payload in the staged file
    ReadWriteIOUtils.write(entireChunks.size(), stream);
    for (int i = 0; i < entireChunks.size(); i++) {
      final Chunk chunk = entireChunks.get(i);
      chunk.getHeader().serializeTo(stream);
      chunk.getChunkStatistic().serialize(stream);
      ChunkData.serializeChunkPayload(stream, chunk.getData(), payloadRefAt(i), includeContent);
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

  @Override
  public int serializedSize(final boolean includeContent) {
    int size = Integer.BYTES + Byte.BYTES; // type ordinal, isAligned
    size += Long.BYTES + Byte.BYTES + device.serializedSize() + Long.BYTES;
    size += Byte.BYTES + Integer.BYTES; // needDecodeChunk, chunkHeaderList
    for (final ChunkHeader header : chunkHeaderList) {
      size += header.getSerializedSize();
    }
    if (needDecodeChunk) {
      size += Integer.BYTES * pageNumbers.size();
    }
    size += Byte.BYTES + (chunkLayout == null ? 0 : ChunkLayout.serializedSize());
    size += Integer.BYTES; // entireChunks
    for (int i = 0; i < entireChunks.size(); i++) {
      final Chunk chunk = entireChunks.get(i);
      size += chunk.getHeader().getSerializedSize();
      size += chunk.getChunkStatistic().getSerializedSize();
      size +=
          ChunkData.serializedChunkPayloadSize(chunk.getData(), payloadRefAt(i), includeContent);
    }
    return size;
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
    final List<ChunkHeader> chunkHeaderList = new ArrayList<>(chunkHeaderListSize);
    for (int i = 0; i < chunkHeaderListSize; i++) {
      final byte chunkType = ReadWriteIOUtils.readByte(stream);
      ChunkHeader chunkHeader = ChunkHeader.deserializeFrom(stream, chunkType);
      String measurementID = chunkHeader.getMeasurementID();
      chunkHeader.setMeasurementID(MEASUREMENT_ID_CACHE.get(measurementID, m -> m));
      chunkHeaderList.add(chunkHeader);
    }
    final List<Integer> pageNumbers = new ArrayList<>(needDecodeChunk ? chunkHeaderListSize : 0);
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

      final ByteBuffer payload = ChunkData.deserializeChunkPayload(stream, chunkPayloadRefs);
      entireChunks.add(new Chunk(header, payload, null, statistics));
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
