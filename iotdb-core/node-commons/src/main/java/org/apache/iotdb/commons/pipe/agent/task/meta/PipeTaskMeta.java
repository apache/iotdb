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

package org.apache.iotdb.commons.pipe.agent.task.meta;

import org.apache.iotdb.commons.conf.IoTDBConstant;
import org.apache.iotdb.commons.consensus.index.ProgressIndex;
import org.apache.iotdb.commons.consensus.index.ProgressIndexType;
import org.apache.iotdb.commons.exception.pipe.PipeRuntimeConnectorCriticalException;
import org.apache.iotdb.commons.exception.pipe.PipeRuntimeCriticalException;
import org.apache.iotdb.commons.exception.pipe.PipeRuntimeException;
import org.apache.iotdb.commons.exception.pipe.PipeRuntimeExceptionType;
import org.apache.iotdb.commons.exception.pipe.PipeRuntimeNonCriticalException;
import org.apache.iotdb.commons.pipe.agent.runtime.PipePeriodicalJobExecutor;
import org.apache.iotdb.commons.pipe.config.PipeConfig;

import org.apache.commons.io.FileUtils;
import org.apache.tsfile.utils.PublicBAOS;
import org.apache.tsfile.utils.ReadWriteIOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

public class PipeTaskMeta {

  private static final Logger LOGGER = LoggerFactory.getLogger(PipeTaskMeta.class);
  private static final String PREFIX = "__progressIndex_";

  private final AtomicReference<ProgressIndex> progressIndex = new AtomicReference<>();
  private final AtomicInteger leaderNodeId = new AtomicInteger(0);

  private final AtomicLong updateCount = new AtomicLong(0);
  private final AtomicLong lastPersistCount = new AtomicLong(0);
  private final long checkPointGap =
      PipeConfig.getInstance().getPipeProgressIndexPersistCheckPointGap();
  private File progressIndexPersistFile;
  private final AtomicBoolean isRegisterPersistTask = new AtomicBoolean(false);
  private Future<?> persistProgressIndexFuture;

  /**
   * Stores the exceptions encountered during run time of each pipe task.
   *
   * <p>The exceptions are instances of {@link PipeRuntimeCriticalException}, {@link
   * PipeRuntimeConnectorCriticalException} and {@link PipeRuntimeNonCriticalException}.
   *
   * <p>The failure of them, respectively, will lead to the stop of the pipe, the stop of the pipes
   * sharing the same connector, and nothing.
   */
  private final Set<PipeRuntimeException> exceptionMessages =
      Collections.newSetFromMap(new ConcurrentHashMap<>());

  public PipeTaskMeta(
      /* @NotNull */ final ProgressIndex progressIndex,
      final int leaderNodeId,
      final int taskIndex,
      final boolean needPersistProgressIndex) {
    this.progressIndex.set(progressIndex);
    this.leaderNodeId.set(leaderNodeId);
    // PipeTaskMeta created in configNode doesn't need to persist progress index.
    if (needPersistProgressIndex) {
      this.progressIndexPersistFile =
          new File(
              IoTDBConstant.DN_DEFAULT_DATA_DIR
                  + File.separator
                  + IoTDBConstant.SYSTEM_FOLDER_NAME
                  + File.separator
                  + PipeConfig.getInstance().getPipeHardlinkBaseDirName()
                  + File.separator
                  + PipeConfig.getInstance().getPipeProgressIndexPersistDirName(),
              PREFIX + taskIndex);
    }
  }

  public ProgressIndex getProgressIndex() {
    return progressIndex.get();
  }

  public ProgressIndex updateProgressIndex(final ProgressIndex updateIndex) {
    // only pipeTaskMeta that need to updateProgressIndex will persist progress index
    // isRegisterPersistTask is used to avoid multiple threads registering persist task concurrently
    if (PipeConfig.getInstance().isPipeProgressIndexPersistEnabled()
        && this.persistProgressIndexFuture == null
        && !isRegisterPersistTask.getAndSet(true)) {
      this.persistProgressIndexFuture =
          PipePeriodicalJobExecutor.submitBackgroundJob(
              this::persistProgressIndex,
              0,
              PipeConfig.getInstance().getPipeProgressIndexFlushIntervalMs());
    }

    progressIndex.updateAndGet(
        index -> index.updateToMinimumEqualOrIsAfterProgressIndex(updateIndex));

    if (PipeConfig.getInstance().isPipeProgressIndexPersistEnabled()
        && updateCount.incrementAndGet() - lastPersistCount.get() > checkPointGap) {
      persistProgressIndex();
    }

    return progressIndex.get();
  }

  public synchronized void persistProgressIndex() {
    if (Objects.isNull(progressIndexPersistFile)
        // in case of multiple threads calling updateProgressIndex at the same time
        || lastPersistCount.get() == updateCount.get()) {
      return;
    }

    try (final PublicBAOS byteArrayOutputStream = new PublicBAOS();
        final DataOutputStream outputStream = new DataOutputStream(byteArrayOutputStream)) {
      progressIndex.get().serialize(outputStream);
      // append is false by default.
      FileUtils.writeByteArrayToFile(
          progressIndexPersistFile,
          byteArrayOutputStream.getBuf(),
          0,
          byteArrayOutputStream.size());
      lastPersistCount.set(updateCount.get());
    } catch (IOException e) {
      LOGGER.warn("Failed to persist progress index {} for {}", progressIndex.get(), this, e);
    }
  }

  public ProgressIndex restoreProgressIndex() {
    if (!progressIndexPersistFile.exists() || progressIndexPersistFile.length() == 0) {
      return progressIndex.get();
    }

    try {
      final byte[] fileData = Files.readAllBytes(progressIndexPersistFile.toPath());

      try (final ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(fileData);
          final DataInputStream inputStream = new DataInputStream(byteArrayInputStream)) {
        final ProgressIndex restoredIndex = ProgressIndexType.deserializeFrom(inputStream);
        progressIndex.get().updateToMinimumEqualOrIsAfterProgressIndex(restoredIndex);
        LOGGER.info(
            "{} successfully restored progress index from [{}], current index: {}",
            this,
            progressIndexPersistFile.getAbsolutePath(),
            progressIndex.get());
      }
    } catch (final IOException e) {
      LOGGER.warn(
          "{} failed to restore progress index from [{}].",
          this,
          progressIndexPersistFile.getAbsolutePath(),
          e);
    }
    return progressIndex.get();
  }

  public void cancelPersistProgressIndexFuture() {
    if (Objects.nonNull(progressIndexPersistFile)
        && isRegisterPersistTask.getAndSet(false)
        && persistProgressIndexFuture != null) {
      persistProgressIndexFuture.cancel(false);
      persistProgressIndexFuture = null;
    }
  }

  public int getLeaderNodeId() {
    return leaderNodeId.get();
  }

  public void setLeaderNodeId(final int leaderNodeId) {
    this.leaderNodeId.set(leaderNodeId);
  }

  public synchronized Iterable<PipeRuntimeException> getExceptionMessages() {
    return new ArrayList<>(exceptionMessages);
  }

  public synchronized String getExceptionMessagesString() {
    return exceptionMessages.toString();
  }

  public synchronized void trackExceptionMessage(final PipeRuntimeException exceptionMessage) {
    // Only keep the newest exception message to avoid excess rpc payload and
    // show pipe response
    // Here we still keep the map form to allow compatibility with legacy versions
    exceptionMessages.clear();
    exceptionMessages.add(exceptionMessage);
  }

  public synchronized boolean containsExceptionMessage(
      final PipeRuntimeException exceptionMessage) {
    return exceptionMessages.contains(exceptionMessage);
  }

  public synchronized boolean hasExceptionMessages() {
    return !exceptionMessages.isEmpty();
  }

  public synchronized void clearExceptionMessages() {
    exceptionMessages.clear();
  }

  public synchronized void serialize(final OutputStream outputStream) throws IOException {
    progressIndex.get().serialize(outputStream);

    ReadWriteIOUtils.write(leaderNodeId.get(), outputStream);

    ReadWriteIOUtils.write(exceptionMessages.size(), outputStream);
    for (final PipeRuntimeException pipeRuntimeException : exceptionMessages) {
      pipeRuntimeException.serialize(outputStream);
    }
  }

  public static PipeTaskMeta deserialize(
      final PipeRuntimeMetaVersion version,
      final ByteBuffer byteBuffer,
      final int taskIndex,
      final boolean needPersist) {
    final ProgressIndex progressIndex = ProgressIndexType.deserializeFrom(byteBuffer);

    final int leaderNodeId = ReadWriteIOUtils.readInt(byteBuffer);

    final PipeTaskMeta pipeTaskMeta =
        new PipeTaskMeta(progressIndex, leaderNodeId, taskIndex, needPersist);
    final int size = ReadWriteIOUtils.readInt(byteBuffer);
    for (int i = 0; i < size; ++i) {
      final PipeRuntimeException pipeRuntimeException =
          PipeRuntimeExceptionType.deserializeFrom(version, byteBuffer);
      pipeTaskMeta.exceptionMessages.add(pipeRuntimeException);
    }
    return pipeTaskMeta;
  }

  public static PipeTaskMeta deserialize(
      final PipeRuntimeMetaVersion version,
      final InputStream inputStream,
      final int taskIndex,
      final boolean needPersist)
      throws IOException {
    final ProgressIndex progressIndex = ProgressIndexType.deserializeFrom(inputStream);

    final int leaderNodeId = ReadWriteIOUtils.readInt(inputStream);

    final PipeTaskMeta pipeTaskMeta =
        new PipeTaskMeta(progressIndex, leaderNodeId, taskIndex, needPersist);
    final int size = ReadWriteIOUtils.readInt(inputStream);
    for (int i = 0; i < size; ++i) {
      final PipeRuntimeException pipeRuntimeException =
          PipeRuntimeExceptionType.deserializeFrom(version, inputStream);
      pipeTaskMeta.exceptionMessages.add(pipeRuntimeException);
    }
    return pipeTaskMeta;
  }

  @Override
  public boolean equals(final Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj == null || getClass() != obj.getClass()) {
      return false;
    }
    final PipeTaskMeta that = (PipeTaskMeta) obj;
    return progressIndex.get().equals(that.progressIndex.get())
        && leaderNodeId.get() == that.leaderNodeId.get()
        && exceptionMessages.equals(that.exceptionMessages);
  }

  @Override
  public int hashCode() {
    return Objects.hash(progressIndex.get(), leaderNodeId.get(), exceptionMessages);
  }

  @Override
  public String toString() {
    return "PipeTaskMeta{"
        + "progressIndex='"
        + progressIndex.get()
        + "', leaderNodeId="
        + leaderNodeId.get()
        + ", exceptionMessages='"
        + exceptionMessages
        + "'}";
  }
}
