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

import java.util.ArrayList;
import java.util.List;
import org.apache.iotdb.tsfile.common.cache.Accountable;
import org.apache.iotdb.tsfile.file.header.ChunkHeader;
import org.apache.iotdb.tsfile.utils.Pair;

/**
 * used in query.
 */
public class Chunk implements Accountable {

  private ChunkHeader chunkHeader;
  private ByteBuffer chunkData;
  /**
   * All data with timestamp <= deletedAt are considered deleted.
   */
  private long deletedAt;

  private List<Pair<Long, Long>> deleteRangeList = new ArrayList<>();

  private long ramSize;

  public Chunk(ChunkHeader header, ByteBuffer buffer, long deletedAt, List<Pair<Long, Long>> deleteRangeList) {
    this.chunkHeader = header;
    this.chunkData = buffer;
    this.deletedAt = deletedAt;
    this.deleteRangeList = deleteRangeList;
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

  public List<Pair<Long, Long>> getDeleteRangeList() {
    return deleteRangeList;
  }

  public void setDeleteRangeList(List<Pair<Long, Long>> list) {
    this.deleteRangeList = list;
  }

  @Override
  public void setRamSize(long size) {
    this.ramSize = size;
  }

  @Override
  public long getRamSize() {
    return ramSize;
  }
}
