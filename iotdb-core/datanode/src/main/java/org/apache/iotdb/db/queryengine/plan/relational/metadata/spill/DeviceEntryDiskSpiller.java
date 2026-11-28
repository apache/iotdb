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

import java.io.BufferedOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.List;

public final class DeviceEntryDiskSpiller implements AutoCloseable {

  private final DeviceEntrySpillManager spillManager;
  private final String queryId;
  private final Path ownerDirectory;
  private final Path directory;
  private final long targetSegmentBytes;
  private final DeviceEntryIOContext ioContext;
  private final boolean duringFetchSchema;
  private final List<Path> sealedSegments = new ArrayList<>();

  private DataOutputStream output;
  private OutputStream fileOutput;
  private Path temporaryFile;
  private long currentBytes;
  private int nextSegmentId;
  private boolean closed;
  private boolean outputCloseFailed;
  private boolean segmentPublicationFailed;

  public DeviceEntryDiskSpiller(
      DeviceEntrySpillManager spillManager,
      String queryId,
      Path ownerDirectory,
      Path directory,
      long targetSegmentBytes,
      DeviceEntryIOContext ioContext,
      boolean duringFetchSchema)
      throws IOException {
    this.spillManager = spillManager;
    this.queryId = queryId;
    this.ownerDirectory = ownerDirectory;
    this.directory = directory;
    this.targetSegmentBytes = targetSegmentBytes;
    this.ioContext = ioContext;
    this.duringFetchSchema = duringFetchSchema;
    Files.createDirectories(directory);
  }

  public void append(byte[] serializedEntry) throws IOException {
    spillManager.appendSpiller(this, serializedEntry);
  }

  synchronized void appendInternal(byte[] serializedEntry) throws IOException {
    checkOpen();
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
    return spillManager.finishSpiller(this);
  }

  synchronized List<Path> finishInternal() throws IOException {
    checkOpen();
    sealCurrentSegment();
    return List.copyOf(sealedSegments);
  }

  String queryId() {
    return queryId;
  }

  Path ownerDirectory() {
    return ownerDirectory;
  }

  private void checkOpen() throws IOException {
    if (segmentPublicationFailed) {
      throw new IOException(
          DataNodeQueryMessages
              .EXCEPTION_DEVICEENTRY_SPILL_SEGMENT_COULD_NOT_BE_PUBLISHED_992816AF);
    }
    if (outputCloseFailed) {
      throw new IOException(
          DataNodeQueryMessages.EXCEPTION_DEVICEENTRY_SPILLER_OUTPUT_CLOSE_FAILED_327C3F35);
    }
    if (closed) {
      throw new IOException(
          String.format(
              DataNodeQueryMessages.EXCEPTION_DEVICEENTRY_SPILLER_IS_ALREADY_CLOSED_ARG_FB427109,
              directory));
    }
  }

  private void ensureOutput() throws IOException {
    if (output != null) {
      return;
    }
    temporaryFile = directory.resolve(String.format("segment-%06d.tmp", nextSegmentId));
    fileOutput = Files.newOutputStream(temporaryFile);
    output = new DataOutputStream(new BufferedOutputStream(fileOutput));
    currentBytes = 0;
  }

  private void sealCurrentSegment() throws IOException {
    if (output == null) {
      return;
    }
    closeOutput();
    Path sealedFile = directory.resolve(String.format("segment-%06d.bin", nextSegmentId++));
    try {
      Files.move(
          temporaryFile,
          sealedFile,
          StandardCopyOption.ATOMIC_MOVE,
          StandardCopyOption.REPLACE_EXISTING);
    } catch (IOException atomicMoveFailure) {
      try {
        Files.move(temporaryFile, sealedFile, StandardCopyOption.REPLACE_EXISTING);
      } catch (IOException moveFailure) {
        moveFailure.addSuppressed(atomicMoveFailure);
        segmentPublicationFailed = true;
        throw moveFailure;
      }
    }
    sealedSegments.add(sealedFile);
    temporaryFile = null;
    currentBytes = 0;
  }

  private void closeOutput() throws IOException {
    if (output == null) {
      return;
    }
    try {
      output.close();
    } catch (IOException e) {
      outputCloseFailed = true;
      throw e;
    }
    if (outputCloseFailed) {
      fileOutput.close();
    }
    output = null;
    fileOutput = null;
    outputCloseFailed = false;
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
    spillManager.closeSpiller(this);
  }

  synchronized void closeInternal() throws IOException {
    if (closed) {
      return;
    }
    closeOutput();
    if (temporaryFile != null) {
      try {
        Files.deleteIfExists(temporaryFile);
      } catch (NoSuchFileException ignored) {
      }
      temporaryFile = null;
    }
    closed = true;
  }
}
