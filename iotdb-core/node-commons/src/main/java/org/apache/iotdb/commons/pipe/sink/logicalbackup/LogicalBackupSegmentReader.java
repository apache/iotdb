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

package org.apache.iotdb.commons.pipe.sink.logicalbackup;

import org.apache.iotdb.commons.i18n.LogicalBackupMessages;

import java.io.EOFException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.UUID;

public class LogicalBackupSegmentReader {

  private final int maxRecordBytes;

  public LogicalBackupSegmentReader(final int maxRecordBytes) {
    this.maxRecordBytes = maxRecordBytes;
  }

  public ScanResult scan(final Path segment, final boolean allowIncompleteTail) throws IOException {
    try (final RandomAccessFile reader = new RandomAccessFile(segment.toFile(), "r")) {
      final long fileLength = reader.length();
      if (fileLength < LogicalBackupFormat.SEGMENT_HEADER_SIZE) {
        throw new EOFException(
            String.format(
                LogicalBackupMessages
                    .EXCEPTION_INCOMPLETE_LOGICAL_BACKUP_SEGMENT_HEADER_ARG_1FE8371A,
                segment));
      }

      final byte[] segmentHeader = readBytes(reader, LogicalBackupFormat.SEGMENT_HEADER_SIZE);
      validateSegmentHeader(segmentHeader, segment);
      final ByteBuffer segmentHeaderBuffer = wrap(segmentHeader);
      segmentHeaderBuffer.position(Long.BYTES + Short.BYTES * 2);
      final long segmentId = segmentHeaderBuffer.getLong();
      final long createdAt = segmentHeaderBuffer.getLong();

      final MessageDigest digest = LogicalBackupFormat.newSha256();
      digest.update(segmentHeader);
      final List<LogicalBackupRecord> records = new ArrayList<>();
      long validLength = LogicalBackupFormat.SEGMENT_HEADER_SIZE;
      long lastCommittedLength = validLength;
      boolean openEventGroup = false;
      UUID openEventGroupId = null;
      int nextOperationIndex = 0;
      long expectedSequence = -1;
      boolean incompleteTail = false;
      Footer footer = null;

      while (reader.getFilePointer() < fileLength) {
        final long frameOffset = reader.getFilePointer();
        if (fileLength - frameOffset < Integer.BYTES) {
          incompleteTail = true;
          break;
        }

        final int magic = reader.readInt();
        reader.seek(frameOffset);
        if (magic == LogicalBackupFormat.FOOTER_MAGIC) {
          if (fileLength - frameOffset < LogicalBackupFormat.SEGMENT_FOOTER_SIZE) {
            incompleteTail = true;
            break;
          }
          if (fileLength - frameOffset > LogicalBackupFormat.SEGMENT_FOOTER_SIZE) {
            throw new IOException(
                String.format(
                    LogicalBackupMessages
                        .EXCEPTION_INVALID_LOGICAL_BACKUP_SEGMENT_FOOTER_SIZE_ARG_4711E278,
                    segment));
          }
          final byte[] footerBytes = readBytes(reader, LogicalBackupFormat.SEGMENT_FOOTER_SIZE);
          footer =
              parseAndValidateFooter(
                  footerBytes,
                  segmentId,
                  createdAt,
                  records.isEmpty() ? -1 : records.get(0).getSequence(),
                  records.isEmpty() ? -1 : records.get(records.size() - 1).getSequence(),
                  frameOffset,
                  records.size(),
                  digest.digest(),
                  segment);
          validLength = reader.getFilePointer();
          lastCommittedLength = validLength;
          break;
        }
        if (magic != LogicalBackupFormat.RECORD_MAGIC) {
          throw new IOException(
              String.format(
                  LogicalBackupMessages
                      .EXCEPTION_INVALID_LOGICAL_BACKUP_RECORD_MAGIC_AT_OFFSET_ARG_IN_ARG_C132B9D5,
                  frameOffset,
                  segment));
        }
        if (fileLength - frameOffset < LogicalBackupFormat.RECORD_HEADER_SIZE) {
          incompleteTail = true;
          break;
        }

        final byte[] header = readBytes(reader, LogicalBackupFormat.RECORD_HEADER_SIZE);
        final ByteBuffer headerBuffer = wrap(header);
        headerBuffer.getInt();
        final byte recordTypeCode = headerBuffer.get();
        final long sequence = headerBuffer.getLong();
        final UUID eventGroupId = new UUID(headerBuffer.getLong(), headerBuffer.getLong());
        final int operationIndex = headerBuffer.getInt();
        final long eventTime = headerBuffer.getLong();
        final byte requestVersion = headerBuffer.get();
        final short requestType = headerBuffer.getShort();
        final int metadataLength = headerBuffer.getInt();
        final int payloadLength = headerBuffer.getInt();
        final int headerCrc = headerBuffer.getInt();
        if (headerCrc
            != LogicalBackupFormat.crc32c(header, 0, LogicalBackupFormat.RECORD_HEADER_SIZE - 4)) {
          throw new IOException(
              String.format(
                  LogicalBackupMessages
                      .EXCEPTION_LOGICAL_BACKUP_RECORD_HEADER_CRC_MISMATCH_AT_OFFSET_ARG_IN_ARG_7076DD79,
                  frameOffset,
                  segment));
        }
        if (metadataLength < 0
            || payloadLength < 0
            || metadataLength > maxRecordBytes
            || payloadLength > maxRecordBytes
            || (long) metadataLength + payloadLength > maxRecordBytes) {
          throw new IOException(
              String.format(
                  LogicalBackupMessages
                      .EXCEPTION_INVALID_LOGICAL_BACKUP_RECORD_LENGTH_AT_OFFSET_ARG_IN_ARG_A7F4048E,
                  frameOffset,
                  segment));
        }
        final long remainingLength = (long) metadataLength + payloadLength + Integer.BYTES;
        if (fileLength - reader.getFilePointer() < remainingLength) {
          incompleteTail = true;
          break;
        }

        final byte[] metadataBytes = readBytes(reader, metadataLength);
        final byte[] payload = readBytes(reader, payloadLength);
        final int frameCrc = reader.readInt();
        final byte[] frameContent = new byte[metadataLength + payloadLength];
        System.arraycopy(metadataBytes, 0, frameContent, 0, metadataLength);
        System.arraycopy(payload, 0, frameContent, metadataLength, payloadLength);
        if (frameCrc != LogicalBackupFormat.crc32c(frameContent, 0, frameContent.length)) {
          throw new IOException(
              String.format(
                  LogicalBackupMessages
                      .EXCEPTION_LOGICAL_BACKUP_RECORD_PAYLOAD_CRC_MISMATCH_AT_OFFSET_ARG_IN_ARG_F9436552,
                  frameOffset,
                  segment));
        }

        final LogicalBackupRecordType recordType = LogicalBackupRecordType.valueOf(recordTypeCode);
        if (recordType == null) {
          throw new IOException(
              String.format(
                  LogicalBackupMessages
                      .EXCEPTION_UNKNOWN_LOGICAL_BACKUP_RECORD_TYPE_ARG_IN_ARG_22BB599D,
                  recordTypeCode,
                  segment));
        }
        if (expectedSequence >= 0 && sequence != expectedSequence) {
          throw new IOException(
              String.format(
                  LogicalBackupMessages
                      .EXCEPTION_LOGICAL_BACKUP_SEQUENCE_MISMATCH_AT_OFFSET_ARG_IN_ARG_CCEC352B,
                  frameOffset,
                  segment));
        }
        final LogicalBackupRecord record =
            new LogicalBackupRecord(
                recordType,
                sequence,
                eventGroupId,
                operationIndex,
                eventTime,
                requestVersion,
                requestType,
                new String(metadataBytes, StandardCharsets.UTF_8),
                payload);
        records.add(record);
        digest.update(header);
        digest.update(frameContent);
        digest.update(ByteBuffer.allocate(Integer.BYTES).putInt(frameCrc).array());
        validLength = reader.getFilePointer();
        if (recordType == LogicalBackupRecordType.EVENT_BEGIN) {
          if (openEventGroup || operationIndex != -1) {
            throw new IOException(
                String.format(
                    LogicalBackupMessages
                        .EXCEPTION_NESTED_LOGICAL_BACKUP_EVENT_GROUPS_IN_ARG_896AC47A,
                    segment));
          }
          openEventGroup = true;
          openEventGroupId = eventGroupId;
          nextOperationIndex = 0;
        } else if (recordType == LogicalBackupRecordType.EVENT_COMMIT) {
          if (!openEventGroup) {
            throw new IOException(
                String.format(
                    LogicalBackupMessages
                        .EXCEPTION_LOGICAL_BACKUP_COMMIT_WITHOUT_BEGIN_IN_ARG_B9031541,
                    segment));
          }
          if (!eventGroupId.equals(openEventGroupId) || operationIndex != nextOperationIndex) {
            throw new IOException(
                String.format(
                    LogicalBackupMessages
                        .EXCEPTION_LOGICAL_BACKUP_OPERATION_INDEX_MISMATCH_AT_OFFSET_ARG_IN_ARG_30C3005F,
                    frameOffset,
                    segment));
          }
          openEventGroup = false;
          openEventGroupId = null;
          lastCommittedLength = validLength;
        } else if (recordType == LogicalBackupRecordType.PIPE_REQUEST) {
          if (!openEventGroup) {
            throw new IOException(
                String.format(
                    LogicalBackupMessages
                        .EXCEPTION_LOGICAL_BACKUP_REQUEST_OUTSIDE_EVENT_GROUP_8351818F,
                    frameOffset,
                    segment));
          }
          if (!eventGroupId.equals(openEventGroupId) || operationIndex != nextOperationIndex) {
            throw new IOException(
                String.format(
                    LogicalBackupMessages
                        .EXCEPTION_LOGICAL_BACKUP_OPERATION_INDEX_MISMATCH_AT_OFFSET_ARG_IN_ARG_30C3005F,
                    frameOffset,
                    segment));
          }
          nextOperationIndex++;
        } else if (openEventGroup) {
          throw new IOException(
              String.format(
                  LogicalBackupMessages
                      .EXCEPTION_LOGICAL_BACKUP_CONTROL_RECORD_INSIDE_EVENT_GROUP_C45D158B,
                  frameOffset,
                  segment));
        } else if (!openEventGroup) {
          lastCommittedLength = validLength;
        }
        expectedSequence = sequence + 1;
      }

      if (incompleteTail && !allowIncompleteTail) {
        throw new EOFException(
            String.format(
                LogicalBackupMessages.EXCEPTION_INCOMPLETE_LOGICAL_BACKUP_RECORD_TAIL_ARG_F722DEE7,
                segment));
      }
      if (footer != null && openEventGroup) {
        throw new IOException(
            String.format(
                LogicalBackupMessages
                    .EXCEPTION_SEALED_LOGICAL_BACKUP_SEGMENT_CONTAINS_AN_OPEN_EVENT_GROUP_ARG_54C274D8,
                segment));
      }
      return new ScanResult(
          segmentId,
          createdAt,
          records,
          footer,
          validLength,
          lastCommittedLength,
          incompleteTail,
          openEventGroup);
    }
  }

  private static void validateSegmentHeader(final byte[] header, final Path segment)
      throws IOException {
    final ByteBuffer buffer = wrap(header);
    if (buffer.getLong() != LogicalBackupFormat.SEGMENT_MAGIC) {
      throw new IOException(
          String.format(
              LogicalBackupMessages.EXCEPTION_INVALID_LOGICAL_BACKUP_SEGMENT_MAGIC_ARG_8340BA73,
              segment));
    }
    final short majorVersion = buffer.getShort();
    buffer.getShort();
    if (majorVersion != LogicalBackupFormat.MAJOR_VERSION) {
      throw new IOException(
          String.format(
              LogicalBackupMessages
                  .EXCEPTION_UNSUPPORTED_LOGICAL_BACKUP_SEGMENT_MAJOR_VERSION_ARG_ARG_CE07F1CB,
              majorVersion,
              segment));
    }
    buffer.position(LogicalBackupFormat.SEGMENT_HEADER_SIZE - Integer.BYTES);
    final int headerCrc = buffer.getInt();
    if (headerCrc
        != LogicalBackupFormat.crc32c(header, 0, LogicalBackupFormat.SEGMENT_HEADER_SIZE - 4)) {
      throw new IOException(
          String.format(
              LogicalBackupMessages
                  .EXCEPTION_LOGICAL_BACKUP_SEGMENT_HEADER_CRC_MISMATCH_ARG_07466D55,
              segment));
    }
  }

  private static Footer parseAndValidateFooter(
      final byte[] bytes,
      final long expectedSegmentId,
      final long expectedCreatedAt,
      final long expectedFirstSequence,
      final long expectedLastSequence,
      final long expectedValidBytes,
      final long expectedRecordCount,
      final byte[] expectedDigest,
      final Path segment)
      throws IOException {
    final ByteBuffer buffer = wrap(bytes);
    if (buffer.getInt() != LogicalBackupFormat.FOOTER_MAGIC) {
      throw new IOException(
          String.format(
              LogicalBackupMessages
                  .EXCEPTION_INVALID_LOGICAL_BACKUP_SEGMENT_FOOTER_MAGIC_ARG_D973E2B4,
              segment));
    }
    final long segmentId = buffer.getLong();
    final long firstSequence = buffer.getLong();
    final long lastSequence = buffer.getLong();
    final long recordCount = buffer.getLong();
    final long validBytes = buffer.getLong();
    final long createdAt = buffer.getLong();
    final long sealedAt = buffer.getLong();
    final byte[] digest = new byte[LogicalBackupFormat.SHA256_SIZE];
    buffer.get(digest);
    final int footerCrc = buffer.getInt();
    if (segmentId != expectedSegmentId) {
      throw new IOException(
          String.format(
              LogicalBackupMessages
                  .EXCEPTION_LOGICAL_BACKUP_SEGMENT_FOOTER_ID_MISMATCH_ARG_D832F14D,
              segment));
    }
    if (createdAt != expectedCreatedAt
        || firstSequence != expectedFirstSequence
        || lastSequence != expectedLastSequence
        || validBytes != expectedValidBytes
        || recordCount != expectedRecordCount) {
      throw new IOException(
          String.format(
              LogicalBackupMessages
                  .EXCEPTION_LOGICAL_BACKUP_SEGMENT_FOOTER_METADATA_MISMATCH_ARG_77D1FADB,
              segment));
    }
    if (!Arrays.equals(digest, expectedDigest)) {
      throw new IOException(
          String.format(
              LogicalBackupMessages.EXCEPTION_LOGICAL_BACKUP_SEGMENT_DIGEST_MISMATCH_ARG_85346116,
              segment));
    }
    if (footerCrc
        != LogicalBackupFormat.crc32c(bytes, 0, LogicalBackupFormat.SEGMENT_FOOTER_SIZE - 4)) {
      throw new IOException(
          String.format(
              LogicalBackupMessages
                  .EXCEPTION_LOGICAL_BACKUP_SEGMENT_FOOTER_CRC_MISMATCH_ARG_25FB7D91,
              segment));
    }
    return new Footer(
        segmentId,
        firstSequence,
        lastSequence,
        recordCount,
        validBytes,
        createdAt,
        sealedAt,
        digest);
  }

  private static byte[] readBytes(final RandomAccessFile reader, final int length)
      throws IOException {
    final byte[] bytes = new byte[length];
    reader.readFully(bytes);
    return bytes;
  }

  private static ByteBuffer wrap(final byte[] bytes) {
    return ByteBuffer.wrap(bytes).order(ByteOrder.BIG_ENDIAN);
  }

  public static class ScanResult {
    private final long segmentId;
    private final long createdAt;
    private final List<LogicalBackupRecord> records;
    private final Footer footer;
    private final long validLength;
    private final long lastCommittedLength;
    private final boolean incompleteTail;
    private final boolean openEventGroup;

    private ScanResult(
        final long segmentId,
        final long createdAt,
        final List<LogicalBackupRecord> records,
        final Footer footer,
        final long validLength,
        final long lastCommittedLength,
        final boolean incompleteTail,
        final boolean openEventGroup) {
      this.segmentId = segmentId;
      this.createdAt = createdAt;
      this.records = Collections.unmodifiableList(records);
      this.footer = footer;
      this.validLength = validLength;
      this.lastCommittedLength = lastCommittedLength;
      this.incompleteTail = incompleteTail;
      this.openEventGroup = openEventGroup;
    }

    public long getSegmentId() {
      return segmentId;
    }

    public long getCreatedAt() {
      return createdAt;
    }

    public List<LogicalBackupRecord> getRecords() {
      return records;
    }

    public Footer getFooter() {
      return footer;
    }

    public long getValidLength() {
      return validLength;
    }

    public long getLastCommittedLength() {
      return lastCommittedLength;
    }

    public boolean hasIncompleteTail() {
      return incompleteTail;
    }

    public boolean hasOpenEventGroup() {
      return openEventGroup;
    }

    public boolean isSealed() {
      return footer != null;
    }
  }

  public static class Footer {
    private final long segmentId;
    private final long firstSequence;
    private final long lastSequence;
    private final long recordCount;
    private final long validBytes;
    private final long createdAt;
    private final long sealedAt;
    private final byte[] digest;

    private Footer(
        final long segmentId,
        final long firstSequence,
        final long lastSequence,
        final long recordCount,
        final long validBytes,
        final long createdAt,
        final long sealedAt,
        final byte[] digest) {
      this.segmentId = segmentId;
      this.firstSequence = firstSequence;
      this.lastSequence = lastSequence;
      this.recordCount = recordCount;
      this.validBytes = validBytes;
      this.createdAt = createdAt;
      this.sealedAt = sealedAt;
      this.digest = digest;
    }

    public long getSegmentId() {
      return segmentId;
    }

    public long getFirstSequence() {
      return firstSequence;
    }

    public long getLastSequence() {
      return lastSequence;
    }

    public long getRecordCount() {
      return recordCount;
    }

    public long getValidBytes() {
      return validBytes;
    }

    public long getCreatedAt() {
      return createdAt;
    }

    public long getSealedAt() {
      return sealedAt;
    }

    public byte[] getDigest() {
      return digest;
    }
  }
}
