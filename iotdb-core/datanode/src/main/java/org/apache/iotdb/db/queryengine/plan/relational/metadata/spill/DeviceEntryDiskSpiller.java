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

import java.io.BufferedOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.List;

public final class DeviceEntryDiskSpiller implements AutoCloseable {

  private final Path directory;
  private final long targetSegmentBytes;
  private final DeviceEntryIOContext ioContext;
  private final List<Path> sealedSegments = new ArrayList<>();

  private DataOutputStream output;
  private Path temporaryFile;
  private long currentBytes;
  private int nextSegmentId;

  public DeviceEntryDiskSpiller(Path directory, long targetSegmentBytes) throws IOException {
    this(directory, targetSegmentBytes, null);
  }

  public DeviceEntryDiskSpiller(
      Path directory, long targetSegmentBytes, DeviceEntryIOContext ioContext) throws IOException {
    this.directory = directory;
    this.targetSegmentBytes = targetSegmentBytes;
    this.ioContext = ioContext;
    Files.createDirectories(directory);
  }

  public void append(byte[] serializedEntry) throws IOException {
    checkTimeout();
    long startNanos = System.nanoTime();
    int recordBytes = Integer.BYTES + serializedEntry.length;
    if (currentBytes > 0 && currentBytes + recordBytes > targetSegmentBytes) {
      sealCurrentSegment();
    }
    ensureOutput();
    output.writeInt(serializedEntry.length);
    output.write(serializedEntry);
    currentBytes += recordBytes;
    recordDiskIO(recordBytes, startNanos);
  }

  public List<Path> finish() throws IOException {
    sealCurrentSegment();
    return List.copyOf(sealedSegments);
  }

  private void ensureOutput() throws IOException {
    if (output != null) {
      return;
    }
    temporaryFile = directory.resolve(String.format("segment-%06d.tmp", nextSegmentId));
    output = new DataOutputStream(new BufferedOutputStream(Files.newOutputStream(temporaryFile)));
    currentBytes = 0;
  }

  private void sealCurrentSegment() throws IOException {
    if (output == null) {
      return;
    }
    checkTimeout();
    long startNanos = System.nanoTime();
    output.close();
    output = null;
    Path sealedFile = directory.resolve(String.format("segment-%06d.bin", nextSegmentId++));
    try {
      Files.move(
          temporaryFile,
          sealedFile,
          StandardCopyOption.ATOMIC_MOVE,
          StandardCopyOption.REPLACE_EXISTING);
    } catch (IOException e) {
      Files.move(temporaryFile, sealedFile, StandardCopyOption.REPLACE_EXISTING);
    }
    sealedSegments.add(sealedFile);
    if (ioContext != null) {
      ioContext.recordDiskIO(0, startNanos);
    }
    temporaryFile = null;
    currentBytes = 0;
  }

  private void checkTimeout() {
    if (ioContext != null) {
      ioContext.checkTimeout();
    }
  }

  private void recordDiskIO(long bytes, long startNanos) {
    if (ioContext != null) {
      ioContext.recordDiskIO(bytes, startNanos);
    }
  }

  @Override
  public void close() throws IOException {
    if (output != null) {
      output.close();
      output = null;
    }
    if (temporaryFile != null) {
      Files.deleteIfExists(temporaryFile);
    }
  }
}
