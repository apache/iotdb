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
package org.apache.iotdb.tsfile.read.common;

import java.nio.ByteBuffer;
import org.apache.iotdb.tsfile.file.header.ChunkHeader;

/**
 * used in query.
 */
public class Chunk {

  private ChunkHeader chunkHeader;
  private ByteBuffer chunkData;
  /**
   * All data with timestamp <= deletedAt are considered deleted.
   */
  private long deletedAt;

  public Chunk(ChunkHeader header, ByteBuffer buffer, long deletedAt) {
    this.chunkHeader = header;
    this.chunkData = buffer;
    this.deletedAt = deletedAt;
  }

  public ChunkHeader getHeader() {
    return chunkHeader;
  }

  public ByteBuffer getData() {
    return chunkData;
  }

  public long getDeletedAt() {
    return deletedAt;
  }

  public void setDeletedAt(long deletedAt) {
    this.deletedAt = deletedAt;
  }
}
