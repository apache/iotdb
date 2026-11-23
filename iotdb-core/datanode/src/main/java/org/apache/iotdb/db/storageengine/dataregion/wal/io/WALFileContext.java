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
package org.apache.iotdb.db.storageengine.dataregion.wal.io;

import org.apache.tsfile.file.metadata.enums.CompressionType;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

/**
 * One extension-format file. The caller owns the channel; the context owns only format resources.
 * Read methods return decoded segments and must distinguish an incomplete tail (EOFException) from
 * an unrecoverable format failure (WALFormatException).
 */
public interface WALFileContext extends AutoCloseable {
  int writeData(
      FileChannel channel,
      long index,
      CompressionType compression,
      int uncompressedSize,
      ByteBuffer data)
      throws IOException;

  /** Returns an uncompressed segment, or throws EOFException at metadata/the incomplete tail. */
  ByteBuffer readSegment(FileChannel channel, long endOffset, long index) throws IOException;

  /** Writes the format-specific metadata frame and trailer after the final data segment. */
  void seal(FileChannel channel, WALMetaData metadata, long index) throws IOException;

  /**
   * Releases a returned segment, including any sensitive scratch state; accepts null. The provider
   * owns its allocation and may recycle it. The caller must not separately free its native memory.
   */
  void release(ByteBuffer data);

  /** Releases format resources; repeated calls must be safe. The channel belongs to the caller. */
  @Override
  void close();
}
