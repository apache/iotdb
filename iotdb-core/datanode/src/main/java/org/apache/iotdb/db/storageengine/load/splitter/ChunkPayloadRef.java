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
import org.apache.iotdb.db.storageengine.load.LoadTsFileManager;

import org.apache.tsfile.utils.ReadWriteIOUtils;

import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.RandomAccessFile;
import java.nio.file.Path;
import java.util.Arrays;

/**
 * Points at the payload bytes of one chunk inside the staged TsFile of a LOAD task.
 *
 * <p>The staged file keeps the only copy of a task's payloads: whoever forwards the piece reads
 * them back with {@link #readPayload()}.
 */
public class ChunkPayloadRef {

  private final String filePath;
  private final long offset;
  private final long size;

  public ChunkPayloadRef(final String filePath, final long offset, final long size) {
    // Bounds and overflow guard before the reference is ever used to seek the staged file: a
    // malformed or hostile reference must fail here instead of allocating a negative or oversized
    // byte array when its payload is read back. The same guard as the piece references of
    // LoadTsFileConsensusNode.PieceRef.
    //
    // The offset is deliberately not bounded: it is an absolute position inside the staged TsFile,
    // which is routinely larger than 2 GiB even when every single payload it holds is small. What
    // has to hold is that the end of the referenced range does not overflow, which is what the
    // subtraction-free form below checks for an offset of any magnitude.
    if (filePath == null
        || offset < 0
        || size < 0
        || size > Integer.MAX_VALUE
        || Long.MAX_VALUE - offset < size) {
      throw new IllegalArgumentException(
          String.format(
              StorageEngineMessages
                  .EXCEPTION_LOAD_CONSENSUS_INVALID_CHUNK_PAYLOAD_REF_PATH_ARG_OFFSET_ARG_SIZE_ARG_6B70A61B,
              filePath,
              offset,
              size));
    }
    this.filePath = filePath;
    this.offset = offset;
    this.size = size;
  }

  public String getFilePath() {
    return filePath;
  }

  public long getOffset() {
    return offset;
  }

  public long getSize() {
    return size;
  }

  public void serializeTo(final DataOutputStream stream) throws IOException {
    ReadWriteIOUtils.write(filePath, stream);
    ReadWriteIOUtils.write(offset, stream);
    ReadWriteIOUtils.write(size, stream);
  }

  public static ChunkPayloadRef deserializeFrom(final InputStream stream) throws IOException {
    final String filePath = ReadWriteIOUtils.readString(stream);
    final long offset = ReadWriteIOUtils.readLong(stream);
    final long size = ReadWriteIOUtils.readLong(stream);
    try {
      return new ChunkPayloadRef(filePath, offset, size);
    } catch (final IllegalArgumentException e) {
      // A reference that cannot describe bytes of any staged file is malformed input rather than a
      // programming error: deserializing it has to fail as an I/O failure of the request, so a
      // corrupt WAL entry or request is reported by the caller that reads it.
      throw new IOException(e.getMessage(), e);
    }
  }

  /**
   * Reads the referenced bytes back from the staged file.
   *
   * @throws ChunkPayloadUnavailableException if the file is gone, shorter than the reference or
   *     outside of this DataNode's LOAD directories
   */
  public byte[] readPayload() {
    if (size == 0) {
      // Nothing to read: an empty payload must not require the staged file to still exist.
      return new byte[0];
    }
    final File file = locatePayloadFile();
    try (final RandomAccessFile input = new RandomAccessFile(file, "r")) {
      final byte[] payload = new byte[Math.toIntExact(size)];
      input.seek(offset);
      try {
        input.readFully(payload);
        return payload;
      } catch (final EOFException e) {
        throw newStagedFileEofException(file, input.getFilePointer(), e);
      }
    } catch (final IOException | ArithmeticException e) {
      throw new ChunkPayloadUnavailableException(
          String.format(
              StorageEngineMessages
                  .EXCEPTION_FAILED_TO_READ_BACK_THE_STAGED_PIECE_FILE_ARG_ARG_3F54CB90,
              file.getAbsolutePath(),
              e.getMessage()),
          e);
    }
  }

  /**
   * Streams the referenced bytes into {@code stream} without materializing them in a byte array.
   * Used when a payload has to be inlined into a request but is still stored in the staged file.
   */
  public void writePayloadTo(final DataOutputStream stream) throws IOException {
    if (size == 0) {
      // Nothing to copy: an empty payload must not require the staged file to still exist.
      return;
    }
    final File file = locatePayloadFile();
    final byte[] buffer = new byte[(int) Math.min(size, 8192L)];
    try (final RandomAccessFile input = new RandomAccessFile(file, "r")) {
      input.seek(offset);
      long remaining = size;
      long position = offset;
      while (remaining > 0) {
        final int read = input.read(buffer, 0, (int) Math.min(buffer.length, remaining));
        if (read < 0) {
          // The file shrank after it was checked: report the position the file actually ends at.
          throw newStagedFileEofException(file, position, null);
        }
        stream.write(buffer, 0, read);
        remaining -= read;
        position += read;
      }
    } catch (final IOException | ArithmeticException e) {
      throw new ChunkPayloadUnavailableException(
          String.format(
              StorageEngineMessages
                  .EXCEPTION_FAILED_TO_READ_BACK_THE_STAGED_PIECE_FILE_ARG_ARG_3F54CB90,
              file.getAbsolutePath(),
              e.getMessage()),
          e);
    }
  }

  private ChunkPayloadUnavailableException newStagedFileEofException(
      final File file, final long position, final Throwable cause) {
    final String message =
        String.format(
            StorageEngineMessages
                .EXCEPTION_UNEXPECTED_END_OF_FILE_WHEN_READING_THE_STAGED_PIECE_ARG_ARG_BYTES_WERE_REQUESTED_FROM_OFFSET_ARG_BUT_THE_FILE_ENDED_AT_POSITION_ARG_3F9CA32B,
            file.getAbsolutePath(),
            size,
            offset,
            position);
    return cause == null
        ? new ChunkPayloadUnavailableException(message)
        : new ChunkPayloadUnavailableException(message, cause);
  }

  private File locatePayloadFile() {
    final File file = locateFile();
    // Check against the file before allocating: the subtraction form cannot overflow even for an
    // offset or size close to Long.MAX_VALUE.
    if (offset < 0 || size < 0 || size > file.length() - offset) {
      throw new ChunkPayloadUnavailableException(
          String.format(
              StorageEngineMessages
                  .EXCEPTION_THE_CHUNK_PAYLOAD_OF_ARG_BYTES_AT_OFFSET_ARG_IS_LONGER_THAN_THE_STAGED_FILE_ARG_14081256,
              size,
              offset,
              file.getAbsolutePath()));
    }
    return file;
  }

  /**
   * Resolves the staged file this reference points at.
   *
   * <p>Payload is only read back from files that live inside one of this DataNode's LOAD
   * directories, so a malformed or hostile reference cannot turn consensus replication into a read
   * of an arbitrary local file.
   */
  private File locateFile() {
    final String[] loadBaseDirs = LoadTsFileManager.getLoadBaseDirs();
    File file = new File(filePath);
    if (!file.isAbsolute()) {
      for (final String baseDir : loadBaseDirs) {
        final File candidate = new File(new File(baseDir), filePath);
        if (candidate.isFile()) {
          file = candidate;
          break;
        }
      }
    }
    if (!file.isFile()) {
      throw new ChunkPayloadUnavailableException(
          String.format(
              StorageEngineMessages
                  .EXCEPTION_STAGED_PIECE_FILE_ARG_IS_MISSING_AND_CANNOT_BE_READ_BACK_4F62F9C6,
              file.getAbsolutePath()));
    }
    try {
      final Path payloadPath = file.getCanonicalFile().toPath();
      for (final String baseDir : loadBaseDirs) {
        if (payloadPath.startsWith(new File(baseDir).getCanonicalFile().toPath())) {
          return file;
        }
      }
      throw new ChunkPayloadUnavailableException(
          String.format(
              StorageEngineMessages
                  .EXCEPTION_STAGED_PIECE_FILE_ARG_IS_OUTSIDE_THE_CONFIGURED_LOAD_DIRECTORIES_ARG_322721A9,
              payloadPath,
              Arrays.toString(loadBaseDirs)));
    } catch (final IOException e) {
      throw new ChunkPayloadUnavailableException(
          String.format(
              StorageEngineMessages
                  .EXCEPTION_FAILED_TO_READ_BACK_THE_STAGED_PIECE_FILE_ARG_ARG_3F54CB90,
              file.getAbsolutePath(),
              e.getMessage()),
          e);
    }
  }

  @Override
  public String toString() {
    return filePath + ":" + offset + "+" + size;
  }
}
