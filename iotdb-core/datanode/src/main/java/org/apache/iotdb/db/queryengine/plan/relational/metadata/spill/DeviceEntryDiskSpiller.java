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

import org.apache.iotdb.db.i18n.DataNodeQueryMessages;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.file.AccessDeniedException;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.List;

public final class DeviceEntryDiskSpiller implements AutoCloseable {

  private static final Logger LOGGER = LoggerFactory.getLogger(DeviceEntryDiskSpiller.class);

  private final Path directory;
  private final long targetSegmentBytes;
  private final DeviceEntryIOContext ioContext;
  private final boolean duringFetchSchema;
  private final List<Path> sealedSegments = new ArrayList<>();

  private DataOutputStream output;
  private Path temporaryFile;
  private long currentBytes;
  private int nextSegmentId;

  public DeviceEntryDiskSpiller(
      Path directory,
      long targetSegmentBytes,
      DeviceEntryIOContext ioContext,
      boolean duringFetchSchema)
      throws IOException {
    this.directory = directory;
    this.targetSegmentBytes = targetSegmentBytes;
    this.ioContext = ioContext;
    this.duringFetchSchema = duringFetchSchema;
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
    recordDiskIO(recordBytes, startNanos);
    currentBytes += recordBytes;
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
      if (duringFetchSchema) {
        ioContext.recordDiskIODuringFetchSchema(bytes, startNanos);
      } else {
        ioContext.recordDiskIODuringDistributionPlan(bytes, startNanos);
      }
    }
  }

  @Override
  public void close() throws IOException {
    if (output != null) {
      output.close();
      output = null;
    }
    if (temporaryFile != null) {
      try {
        Files.deleteIfExists(temporaryFile);
      } catch (NoSuchFileException | AccessDeniedException e) {
        LOGGER.warn(
            String.format(
                DataNodeQueryMessages
                    .LOG_FAILED_TO_CLEAN_DEVICEENTRY_SPILL_DIRECTORY_FOR_QUERY_ARG_53D9C1FC,
                temporaryFile),
            e);
      }
    }
  }
}
