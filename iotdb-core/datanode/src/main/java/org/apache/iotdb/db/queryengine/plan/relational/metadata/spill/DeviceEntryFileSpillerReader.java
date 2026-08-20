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

package org.apache.iotdb.db.queryengine.plan.relational.metadata.spill;

import org.apache.iotdb.db.queryengine.plan.relational.metadata.DeviceEntry;

import java.io.BufferedInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.NoSuchElementException;

public final class DeviceEntryFileSpillerReader implements DeviceEntryReader {

  private final List<Path> segments;
  private final boolean deleteSegmentAfterRead;
  private final DeviceEntryIOContext ioContext;
  private final boolean duringFetchSchema;
  private int segmentIndex;
  private DataInputStream input;
  private Path currentSegment;
  private DeviceEntry next;

  public DeviceEntryFileSpillerReader(List<Path> segments) {
    this(segments, false, null, false);
  }

  public DeviceEntryFileSpillerReader(List<Path> segments, boolean deleteSegmentAfterRead) {
    this(segments, deleteSegmentAfterRead, null, false);
  }

  public DeviceEntryFileSpillerReader(
      List<Path> segments,
      boolean deleteSegmentAfterRead,
      DeviceEntryIOContext ioContext,
      boolean duringFetchSchema) {
    this.segments = segments;
    this.deleteSegmentAfterRead = deleteSegmentAfterRead;
    this.ioContext = ioContext;
    this.duringFetchSchema = duringFetchSchema;
  }

  @Override
  public boolean hasNext() throws IOException {
    if (next != null) {
      return true;
    }
    while (true) {
      if (input == null && !openNextSegment()) {
        return false;
      }
      checkTimeout();
      long startNanos = System.nanoTime();
      Integer length = readRecordLength();
      if (length == null) {
        closeCurrentSegment(true);
        continue;
      }
      byte[] bytes = new byte[length];
      input.readFully(bytes);
      if (ioContext != null) {
        if (duringFetchSchema) {
          ioContext.recordDiskIODuringFetchSchema(Integer.BYTES + length, startNanos);
        } else {
          ioContext.recordDiskIODuringDistributionPlan(Integer.BYTES + length, startNanos);
        }
      }
      next = DeviceEntry.deserialize(bytes);
      return true;
    }
  }

  private void checkTimeout() {
    if (ioContext != null) {
      ioContext.checkTimeout();
    }
  }

  @Override
  public DeviceEntry next() throws IOException {
    if (!hasNext()) {
      throw new NoSuchElementException();
    }
    DeviceEntry result = next;
    next = null;
    return result;
  }

  private boolean openNextSegment() throws IOException {
    if (segmentIndex >= segments.size()) {
      return false;
    }
    currentSegment = segments.get(segmentIndex++);
    input = new DataInputStream(new BufferedInputStream(Files.newInputStream(currentSegment)));
    return true;
  }

  private void closeCurrentSegment(boolean fullyConsumed) throws IOException {
    input.close();
    input = null;
    if (fullyConsumed && deleteSegmentAfterRead) {
      try {
        Files.deleteIfExists(currentSegment);
      } catch (IOException ignored) {
        // Query cleanup will retry deleting a segment that could not be deleted eagerly.
      }
    }
    currentSegment = null;
  }

  private Integer readRecordLength() throws IOException {
    int firstByte = input.read();
    if (firstByte < 0) {
      return null;
    }
    int length =
        (firstByte << 24)
            | (input.readUnsignedByte() << 16)
            | (input.readUnsignedByte() << 8)
            | input.readUnsignedByte();
    if (length < 0) {
      throw new IOException();
    }
    return length;
  }

  @Override
  public void close() throws IOException {
    if (input != null) {
      closeCurrentSegment(false);
    }
  }
}
