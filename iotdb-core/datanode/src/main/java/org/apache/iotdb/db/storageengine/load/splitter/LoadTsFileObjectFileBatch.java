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
import org.apache.iotdb.commons.file.SystemFileFactory;
import org.apache.iotdb.commons.utils.TimePartitionUtils;

import org.apache.tsfile.utils.ReadWriteIOUtils;

import java.io.DataOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.RandomAccessFile;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class LoadTsFileObjectFileBatch implements TsFileData {

  private final TTimePartitionSlot lastTimePartitionSlot;

  private final List<ObjectFileChunk> objectFileChunks;

  public static class ObjectFileChunk {
    private final String objectRelativePath;
    private final long fileOffset;
    private final long totalFileLength;
    private final boolean includeLastByte;
    private final byte[] bytes;

    public ObjectFileChunk(
        final String objectRelativePath,
        final long fileOffset,
        final long totalFileLength,
        final boolean includeLastByte,
        final byte[] bytes) {
      this.objectRelativePath = objectRelativePath;
      this.fileOffset = fileOffset;
      this.totalFileLength = totalFileLength;
      this.includeLastByte = includeLastByte;
      this.bytes = bytes;
    }

    public String getObjectRelativePath() {
      return objectRelativePath;
    }

    public long getFileOffset() {
      return fileOffset;
    }

    public long getTotalFileLength() {
      return totalFileLength;
    }

    public boolean isIncludeLastByte() {
      return includeLastByte;
    }

    public byte[] getBytes() {
      return bytes;
    }

    public void writeChunk(final File baseDir, final String newDataRegionID) throws IOException {
      final Path rawPath = Paths.get(getObjectRelativePath());
      if (rawPath.getNameCount() < 2) {
        throw new IOException(
            String.format(
                "Invalid object relative path '%s': expected format 'dataRegionId/.../<filename>'",
                getObjectRelativePath()));
      }
      final Path subPath = rawPath.subpath(1, rawPath.getNameCount());
      final Path finalPath = baseDir.toPath().resolve(newDataRegionID).resolve(subPath);
      final File targetFile = SystemFileFactory.INSTANCE.getFile(finalPath.toString());

      final Path parentPath = targetFile.getParentFile().toPath();
      if (!Files.exists(parentPath)) {
        Files.createDirectories(parentPath);
      }

      try (RandomAccessFile raf = new RandomAccessFile(targetFile, "rw")) {
        final long currentLength = raf.length();
        if (currentLength != getFileOffset()) {
          throw new IOException(
              String.format(
                  "Object file write offset mismatch: file '%s' has size %d but incoming chunk offset is %d",
                  targetFile.getName(), currentLength, getFileOffset()));
        }

        raf.seek(getFileOffset());
        raf.write(getBytes());

        if (isIncludeLastByte() && raf.length() != getTotalFileLength()) {
          throw new IOException(
              String.format(
                  "Object file size mismatch after final chunk write: file '%s' has size %d but expected %d",
                  targetFile.getName(), raf.length(), getTotalFileLength()));
        }
      }
    }
  }

  public LoadTsFileObjectFileBatch(
      final List<ObjectFileChunk> objectFileChunks,
      final TTimePartitionSlot lastTimePartitionSlot) {
    this.lastTimePartitionSlot = lastTimePartitionSlot;
    this.objectFileChunks = objectFileChunks == null ? Collections.emptyList() : objectFileChunks;
  }

  public List<ObjectFileChunk> getObjectFileChunks() {
    return objectFileChunks;
  }

  public TTimePartitionSlot getLastTimePartitionSlot() {
    return lastTimePartitionSlot;
  }

  @Override
  public TsFileDataType getType() {
    return TsFileDataType.OBJECT_FILE_PAYLOAD;
  }

  @Override
  public long getDataSize() {
    return 0;
  }

  @Override
  public void serialize(final DataOutputStream stream) throws IOException {
    ReadWriteIOUtils.write(TsFileDataType.OBJECT_FILE_PAYLOAD.ordinal(), stream);
    ReadWriteIOUtils.write(lastTimePartitionSlot.getStartTime(), stream);
    ReadWriteIOUtils.write(objectFileChunks.size(), stream);
    for (ObjectFileChunk chunk : objectFileChunks) {
      ReadWriteIOUtils.write(chunk.getObjectRelativePath(), stream);
      ReadWriteIOUtils.write(chunk.getFileOffset(), stream);
      ReadWriteIOUtils.write(chunk.getTotalFileLength(), stream);
      ReadWriteIOUtils.write(chunk.isIncludeLastByte(), stream);
      ReadWriteIOUtils.write(chunk.getBytes().length, stream);
      stream.write(chunk.getBytes());
    }
  }

  public static LoadTsFileObjectFileBatch deserialize(final InputStream stream) throws IOException {
    final long lastPartitionStartTime = ReadWriteIOUtils.readLong(stream);
    final int chunkCount = ReadWriteIOUtils.readInt(stream);
    final List<ObjectFileChunk> chunks = deserializeChunks(stream, chunkCount);
    return new LoadTsFileObjectFileBatch(
        chunks, TimePartitionUtils.getTimePartitionSlot(lastPartitionStartTime));
  }

  private static List<ObjectFileChunk> deserializeChunks(
      final InputStream stream, final int chunkCount) throws IOException {
    final List<ObjectFileChunk> chunks = new ArrayList<>(chunkCount);
    for (int i = 0; i < chunkCount; i++) {
      final String relativePath = ReadWriteIOUtils.readString(stream);
      final long offset = ReadWriteIOUtils.readLong(stream);
      final long totalLength = ReadWriteIOUtils.readLong(stream);
      final boolean includeLastByte = ReadWriteIOUtils.readBool(stream);
      final int payloadLength = ReadWriteIOUtils.readInt(stream);
      final byte[] payload = new byte[payloadLength];
      int bytesRead = 0;
      while (bytesRead < payloadLength) {
        final int read = stream.read(payload, bytesRead, payloadLength - bytesRead);
        if (read < 0) {
          throw new IOException(
              String.format(
                  "Unexpected EOF while deserializing object file chunk %d/%d (relativePath=%s, read %d of %d bytes)",
                  i + 1, chunkCount, relativePath, bytesRead, payloadLength));
        }
        bytesRead += read;
      }
      chunks.add(new ObjectFileChunk(relativePath, offset, totalLength, includeLastByte, payload));
    }
    return chunks;
  }
}
