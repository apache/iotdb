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
    return new ChunkPayloadRef(
        ReadWriteIOUtils.readString(stream),
        ReadWriteIOUtils.readLong(stream),
        ReadWriteIOUtils.readLong(stream));
  }

  /**
   * Reads the referenced bytes back from the staged file.
   *
   * @throws ChunkPayloadUnavailableException if the file is gone, shorter than the reference or
   *     outside of this DataNode's LOAD directories
   */
  public byte[] readPayload() {
    final File file = locateFile();
    try (final RandomAccessFile input = new RandomAccessFile(file, "r")) {
      final byte[] payload = new byte[Math.toIntExact(size)];
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
