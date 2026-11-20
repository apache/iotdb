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

import org.apache.iotdb.db.i18n.StorageEngineMessages;

import org.apache.tsfile.utils.ReadWriteIOUtils;

import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.Objects;

/**
 * One slice of a complete source TsFile that a LOAD task transfers byte for byte.
 *
 * <p>The deletion sidecar of a TsFile is not part of this record. A file that has modifications is
 * not loaded through the whole-file path at all: it falls back to the chunk-splitting path, which
 * already transfers its deletions. The whole-file path therefore only ever carries the {@code
 * .tsfile} bytes, and a receiver can rebuild the exact file without any extra artifact.
 *
 * <p>Slices are identified by {@code offset} and are applied in order, so a retried slice is
 * skipped instead of being appended twice. The source length, time partition and pipe flag are
 * repeated in every slice: the receiver creates the staging state on the first slice and needs them
 * to recognise the task and to rebuild its resource on PREPARE.
 */
public class WholeFileData implements TsFileData {

  private static final int TYPE_ORDINAL_BYTES = Integer.BYTES;
  private static final int LONG_BYTES = Long.BYTES;
  private static final int BOOLEAN_BYTES = Byte.BYTES;
  private static final int PAYLOAD_LENGTH_BYTES = Integer.BYTES;
  private static final int METADATA_BYTES =
      TYPE_ORDINAL_BYTES + 4 * LONG_BYTES + BOOLEAN_BYTES + PAYLOAD_LENGTH_BYTES;

  private final long offset;
  private final long sourceLength;
  private final long timePartitionStart;
  private final boolean generatedByPipe;
  private final long checksum;
  private final byte[] payload;

  public WholeFileData(
      final long offset,
      final long sourceLength,
      final long timePartitionStart,
      final boolean generatedByPipe,
      final long checksum,
      final byte[] payload) {
    if (payload == null || payload.length == 0) {
      throw new IllegalArgumentException(
          StorageEngineMessages.EXCEPTION_WHOLE_FILE_SLICE_PAYLOAD_CANNOT_BE_EMPTY_C14A7878);
    }
    if (offset < 0 || sourceLength < 0 || offset > sourceLength - payload.length) {
      throw new IllegalArgumentException(
          String.format(
              StorageEngineMessages
                  .EXCEPTION_WHOLE_FILE_SLICE_RANGE_IS_INVALID_OFFSET_D_LENGTH_D_SOURCE_LENGTH_D_402A1599,
              offset,
              payload.length,
              sourceLength));
    }
    if (generatedByPipe && timePartitionStart == Long.MIN_VALUE) {
      throw new IllegalArgumentException(
          StorageEngineMessages.EXCEPTION_WHOLE_FILE_SLICE_TIME_PARTITION_IS_MISSING_4EE30E24);
    }
    this.offset = offset;
    this.sourceLength = sourceLength;
    this.timePartitionStart = timePartitionStart;
    this.generatedByPipe = generatedByPipe;
    this.checksum = checksum;
    this.payload = payload;
  }

  public long getOffset() {
    return offset;
  }

  public long getSourceLength() {
    return sourceLength;
  }

  public long getTimePartitionStart() {
    return timePartitionStart;
  }

  public boolean isGeneratedByPipe() {
    return generatedByPipe;
  }

  public long getChecksum() {
    return checksum;
  }

  public byte[] getPayload() {
    return payload;
  }

  @Override
  public long getDataSize() {
    return payload.length;
  }

  @Override
  public TsFileDataType getType() {
    return TsFileDataType.WHOLE_FILE;
  }

  @Override
  public void serialize(final DataOutputStream stream) throws IOException {
    serialize(stream, true);
  }

  @Override
  public void serialize(final DataOutputStream stream, final boolean includeContent)
      throws IOException {
    // A whole-file slice has no reference form: its bytes are the payload, so they are always
    // written. A caller that asks for references therefore still gets a self-contained slice.
    ReadWriteIOUtils.write(getType().ordinal(), stream);
    ReadWriteIOUtils.write(offset, stream);
    ReadWriteIOUtils.write(sourceLength, stream);
    ReadWriteIOUtils.write(timePartitionStart, stream);
    ReadWriteIOUtils.write(generatedByPipe, stream);
    ReadWriteIOUtils.write(checksum, stream);
    ReadWriteIOUtils.write(payload.length, stream);
    stream.write(payload);
  }

  @Override
  public int serializedSize(final boolean includeContent) {
    return Math.toIntExact(METADATA_BYTES + (long) payload.length);
  }

  public static WholeFileData deserialize(final InputStream stream) throws IOException {
    final long offset = ReadWriteIOUtils.readLong(stream);
    final long sourceLength = ReadWriteIOUtils.readLong(stream);
    final long timePartitionStart = ReadWriteIOUtils.readLong(stream);
    final boolean generatedByPipe = ReadWriteIOUtils.readBool(stream);
    final long checksum = ReadWriteIOUtils.readLong(stream);
    final int payloadLength = ReadWriteIOUtils.readInt(stream);
    if (payloadLength <= 0) {
      throw new IOException(
          String.format(
              StorageEngineMessages
                  .EXCEPTION_WHOLE_FILE_SLICE_HAS_INVALID_PAYLOAD_LENGTH_D_0D04C9C1,
              payloadLength));
    }
    final byte[] payload = new byte[payloadLength];
    int read = 0;
    while (read < payloadLength) {
      final int current = stream.read(payload, read, payloadLength - read);
      if (current < 0) {
        throw new IOException(
            String.format(
                StorageEngineMessages
                    .EXCEPTION_WHOLE_FILE_SLICE_ENDED_AFTER_D_OF_D_PAYLOAD_BYTES_39E892F4,
                read,
                payloadLength));
      }
      read += current;
    }
    return new WholeFileData(
        offset, sourceLength, timePartitionStart, generatedByPipe, checksum, payload);
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof WholeFileData)) {
      return false;
    }
    final WholeFileData that = (WholeFileData) o;
    return offset == that.offset
        && sourceLength == that.sourceLength
        && timePartitionStart == that.timePartitionStart
        && generatedByPipe == that.generatedByPipe
        && checksum == that.checksum
        && Arrays.equals(payload, that.payload);
  }

  @Override
  public int hashCode() {
    return Objects.hash(offset, sourceLength, timePartitionStart, generatedByPipe, checksum);
  }

  @Override
  public String toString() {
    return "WholeFileData{"
        + "offset="
        + offset
        + ", length="
        + payload.length
        + ", sourceLength="
        + sourceLength
        + '}';
  }
}
