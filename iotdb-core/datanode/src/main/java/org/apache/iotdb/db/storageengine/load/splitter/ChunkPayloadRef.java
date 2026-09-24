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
 * <p>A piece is written to the WAL with the metadata of every chunk it carries (device, time
 * partition, chunk header, statistics and layout), while the payload of a chunk is only referenced
 * here: those bytes exist once, in the staged TsFile. Whoever forwards the piece reads them back
 * with {@link #readPayload()}, so the receiver gets the same piece the leader received.
 */
public class ChunkPayloadRef {

  private final String filePath;
  private final long offset;
  private final long size;

  public ChunkPayloadRef(final String filePath, final long offset, final long size) {
    // Bounds and overflow guard before the reference is ever used to seek the staged file: a
    // malformed or hostile reference must fail here instead of allocating a negative, oversized or
    // overflowing byte array when its payload is read back. The same guard as the piece references
    // of LoadTsFileConsensusNode.PieceRef.
    if (filePath == null
        || offset < 0
        || size < 0
        || size > Integer.MAX_VALUE
        || offset + size < 0
        || offset + size > Integer.MAX_VALUE) {
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
    final File file = locateFile();
    // Checked against the file before the buffer is allocated: without it a reference to bytes past
    // the end of the staged file would still allocate its full size and fail later, and a huge size
    // would be allocated only to be thrown away.
    if (offset + size > file.length()) {
      throw new ChunkPayloadUnavailableException(
          String.format(
              StorageEngineMessages
                  .EXCEPTION_THE_CHUNK_PAYLOAD_OF_ARG_BYTES_AT_OFFSET_ARG_IS_LONGER_THAN_THE_STAGED_FILE_ARG_14081256,
              size,
              offset,
              file.getAbsolutePath()));
    }
    try (final RandomAccessFile input = new RandomAccessFile(file, "r")) {
      final byte[] payload = new byte[(int) size];
      input.seek(offset);
      input.readFully(payload);
      return payload;
    } catch (final EOFException e) {
      throw new ChunkPayloadUnavailableException(
          String.format(
              StorageEngineMessages.EXCEPTION_LOAD_CONSENSUS_STAGED_FILE_EOF_8743387D,
              file.getAbsolutePath(),
              offset),
          e);
    } catch (final IOException | ArithmeticException | NegativeArraySizeException e) {
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
