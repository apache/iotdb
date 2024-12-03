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
package org.apache.iotdb.db.pipe.consensus.deletion.persist;

import org.apache.iotdb.commons.concurrent.IoTDBThreadPoolFactory;
import org.apache.iotdb.commons.concurrent.ThreadName;
import org.apache.iotdb.commons.consensus.index.ProgressIndex;
import org.apache.iotdb.commons.consensus.index.impl.MinimumProgressIndex;
import org.apache.iotdb.commons.consensus.index.impl.SimpleProgressIndex;
import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.pipe.consensus.ProgressIndexDataNodeManager;
import org.apache.iotdb.db.pipe.consensus.deletion.DeletionResource;
import org.apache.iotdb.db.pipe.consensus.deletion.DeletionResourceManager;
import org.apache.iotdb.db.utils.MmapUtil;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * The core idea of this buffer is to write deletion to the Page cache and fsync them to disk when
 * certain conditions are met. This design does not decouple serialization and writing, but provides
 * an easier way to maintain and understand.
 */
public class PageCacheDeletionBuffer implements DeletionBuffer {
  private static final Logger LOGGER = LoggerFactory.getLogger(PageCacheDeletionBuffer.class);
  private static final IoTDBConfig config = IoTDBDescriptor.getInstance().getConfig();
  // Buffer config keep consistent with WAL.
  private static final int ONE_THIRD_WAL_BUFFER_SIZE = config.getWalBufferSize() / 3;
  private static final double FSYNC_BUFFER_RATIO = 0.95;
  private static final int QUEUE_CAPACITY = config.getWalBufferQueueCapacity();
  private static final long MAX_WAIT_CLOSE_TIME_IN_MS = 10000;

  // DeletionResources received from storage engine, which is waiting to be persisted.
  private final BlockingQueue<DeletionResource> deletionResources =
      new PriorityBlockingQueue<>(
          QUEUE_CAPACITY,
          (o1, o2) ->
              o1.getProgressIndex().equals(o2.getProgressIndex())
                  ? 0
                  : (o1.getProgressIndex().isAfter(o2.getProgressIndex()) ? 1 : -1));
  // Data region id
  private final String dataRegionId;
  // directory to store .deletion files
  private final String baseDirectory;
  // single thread to serialize WALEntry to workingBuffer
  private final ExecutorService persistThread;
  private final Lock buffersLock = new ReentrantLock();
  // Total size of this batch.
  private final AtomicInteger totalSize = new AtomicInteger(0);
  // All deletions that will be handled in a single persist task
  private final List<DeletionResource> pendingDeletionsInOneTask = new ArrayList<>();

  // whether close method is called
  private volatile boolean isClosed = false;
  // Serialize buffer in current persist task
  private volatile ByteBuffer serializeBuffer;
  // Current Logging file.
  private volatile File logFile;
  private volatile FileOutputStream logStream;
  private volatile FileChannel logChannel;
  // Max progressIndex among current .deletion file.
  private ProgressIndex maxProgressIndexInCurrentFile = MinimumProgressIndex.INSTANCE;
  // Max progressIndex among last .deletion file. Used by PersistTask for naming .deletion file.
  // Since deletions are written serially, DAL is also written serially. This ensures that the
  // maxProgressIndex of each batch increases in the same order as the physical time.
  private volatile ProgressIndex maxProgressIndexInLastFile = MinimumProgressIndex.INSTANCE;

  public PageCacheDeletionBuffer(String dataRegionId, String baseDirectory) {
    this.dataRegionId = dataRegionId;
    this.baseDirectory = baseDirectory;
    allocateBuffers();
    persistThread =
        IoTDBThreadPoolFactory.newSingleThreadExecutor(
            ThreadName.PIPE_CONSENSUS_DELETION_SERIALIZE.getName()
                + "(group-"
                + dataRegionId
                + ")");
  }

  @Override
  public void start() {
    persistThread.submit(new PersistTask());
    try {
      // initial file is the minimumProgressIndex
      this.logFile =
          new File(
              baseDirectory,
              String.format("_%d-%d%s", 0, 0, DeletionResourceManager.DELETION_FILE_SUFFIX));
      this.logStream = new FileOutputStream(logFile, true);
      this.logChannel = logStream.getChannel();
      // Create file && write magic string
      if (!logFile.exists() || logFile.length() == 0) {
        this.logChannel.write(
            ByteBuffer.wrap(
                DeletionResourceManager.MAGIC_VERSION_V1.getBytes(StandardCharsets.UTF_8)));
      }
      LOGGER.info(
          "Deletion persist-{}: starting to persist, current writing: {}", dataRegionId, logFile);
    } catch (IOException e) {
      LOGGER.warn(
          "Deletion persist: Cannot create file {}, please check your file system manually.",
          logFile,
          e);
      throw new RuntimeException(e);
    }
  }

  @Override
  public boolean isAllDeletionFlushed() {
    buffersLock.lock();
    try {
      int pos = Optional.ofNullable(serializeBuffer).map(ByteBuffer::position).orElse(0);
      return deletionResources.isEmpty() && pos == 0;
    } finally {
      buffersLock.unlock();
    }
  }

  private void allocateBuffers() {
    try {
      serializeBuffer = ByteBuffer.allocateDirect(ONE_THIRD_WAL_BUFFER_SIZE);
    } catch (OutOfMemoryError e) {
      LOGGER.error(
          "Fail to allocate deletionBuffer-group-{}'s buffer because out of memory.",
          dataRegionId,
          e);
      close();
      throw e;
    }
  }

  public void registerDeletionResource(DeletionResource deletionResource) {
    if (isClosed) {
      LOGGER.error(
          "Fail to register DeletionResource into deletionBuffer-{} because this buffer is closed.",
          dataRegionId);
      return;
    }
    deletionResources.add(deletionResource);
  }

  private void appendCurrentBatch() throws IOException {
    serializeBuffer.flip();
    logChannel.write(serializeBuffer);
  }

  private void fsyncCurrentLoggingFile() throws IOException {
    LOGGER.info("Deletion persist-{}: current batch fsync due to timeout", dataRegionId);
    this.logChannel.force(false);
    pendingDeletionsInOneTask.forEach(DeletionResource::onPersistSucceed);
  }

  private void closeCurrentLoggingFile() throws IOException {
    LOGGER.info("Deletion persist-{}: current file has been closed", dataRegionId);
    // Close old resource to fsync.
    this.logStream.close();
    this.logChannel.close();
    pendingDeletionsInOneTask.forEach(DeletionResource::onPersistSucceed);
  }

  private void resetTaskAttribute() {
    this.pendingDeletionsInOneTask.clear();
    clearBuffer();
  }

  private void resetFileAttribute() {
    // Reset file attributes.
    this.totalSize.set(0);
    this.maxProgressIndexInLastFile = this.maxProgressIndexInCurrentFile;
    this.maxProgressIndexInCurrentFile = MinimumProgressIndex.INSTANCE;
  }

  private void rollbackFileAttribute(int currentBatchSize) {
    this.totalSize.addAndGet(-currentBatchSize);
  }

  private void clearBuffer() {
    // Clear serialize buffer
    buffersLock.lock();
    try {
      serializeBuffer.clear();
    } finally {
      buffersLock.unlock();
    }
  }

  private void switchLoggingFile() throws IOException {
    try {
      // PipeConsensus ensures that deleteDataNodes use recoverProgressIndex.
      ProgressIndex curProgressIndex =
          ProgressIndexDataNodeManager.extractLocalSimpleProgressIndex(maxProgressIndexInLastFile);
      if (!(curProgressIndex instanceof SimpleProgressIndex)) {
        throw new IOException("Invalid deletion progress index: " + curProgressIndex);
      }
      SimpleProgressIndex progressIndex = (SimpleProgressIndex) curProgressIndex;
      // Deletion file name format: "_{rebootTimes}_{memTableFlushOrderId}.deletion"
      this.logFile =
          new File(
              baseDirectory,
              String.format(
                  "_%d-%d%s",
                  progressIndex.getRebootTimes(),
                  progressIndex.getMemTableFlushOrderId(),
                  DeletionResourceManager.DELETION_FILE_SUFFIX));
      this.logStream = new FileOutputStream(logFile, true);
      this.logChannel = logStream.getChannel();
      // Create file && write magic string
      if (!logFile.exists() || logFile.length() == 0) {
        this.logChannel.write(
            ByteBuffer.wrap(
                DeletionResourceManager.MAGIC_VERSION_V1.getBytes(StandardCharsets.UTF_8)));
      }
      LOGGER.info(
          "Deletion persist-{}: switching to a new file, current writing: {}",
          dataRegionId,
          logFile);
    } finally {
      resetFileAttribute();
    }
  }

  @Override
  public void close() {
    isClosed = true;
    // Force sync existing data in memory to disk.
    // first waiting serialize and sync tasks finished, then release all resources
    waitUntilFlushAllDeletionsOrTimeOut();
    if (persistThread != null) {
      persistThread.shutdown();
    }
    // clean buffer
    MmapUtil.clean(serializeBuffer);
  }

  private void waitUntilFlushAllDeletionsOrTimeOut() {
    long currentTime = System.currentTimeMillis();
    while (!isAllDeletionFlushed()
        && System.currentTimeMillis() - currentTime < MAX_WAIT_CLOSE_TIME_IN_MS) {
      try {
        Thread.sleep(50);
      } catch (InterruptedException e) {
        LOGGER.error("Interrupted when waiting for all deletions flushed.");
        Thread.currentThread().interrupt();
      }
    }
  }

  private class PersistTask implements Runnable {
    // Batch size in current task, used to roll back.
    private final AtomicInteger currentTaskBatchSize = new AtomicInteger(0);

    @Override
    public void run() {
      try {
        persistDeletion();
      } catch (IOException e) {
        LOGGER.warn(
            "Deletion persist: Cannot write to {}, may cause data inconsistency.", logFile, e);
        // if any exception occurred, this batch will not be written to disk and lost.
        pendingDeletionsInOneTask.forEach(deletionResource -> deletionResource.onPersistFailed(e));
        rollbackFileAttribute(currentTaskBatchSize.get());
      } finally {
        if (!isClosed) {
          persistThread.submit(new PersistTask());
        }
      }
    }

    private boolean serializeDeletionToBatchBuffer(DeletionResource deletionResource) {
      LOGGER.debug(
          "Deletion persist-{}: serialize deletion resource {}", dataRegionId, deletionResource);
      ByteBuffer buffer = deletionResource.serialize();
      // if working buffer doesn't have enough space
      if (buffer.position() > serializeBuffer.remaining()) {
        return false;
      }
      serializeBuffer.put(buffer.array());
      totalSize.addAndGet(buffer.position());
      currentTaskBatchSize.addAndGet(buffer.position());
      return true;
    }

    private void persistDeletion() throws IOException {
      // For first deletion we use blocking take() method.
      try {
        DeletionResource firstDeletionResource = deletionResources.take();
        // Serialize deletion. The first serialization cannot fail because a deletion cannot exceed
        // size of serializeBuffer.
        serializeDeletionToBatchBuffer(firstDeletionResource);
        pendingDeletionsInOneTask.add(firstDeletionResource);
        maxProgressIndexInCurrentFile =
            maxProgressIndexInCurrentFile.updateToMinimumEqualOrIsAfterProgressIndex(
                firstDeletionResource.getProgressIndex());
      } catch (InterruptedException e) {
        LOGGER.warn(
            "Interrupted when waiting for taking DeletionResource from blocking queue to serialize.");
        Thread.currentThread().interrupt();
      }

      // For further deletion, we use non-blocking poll() method to persist existing deletion of
      // current batch in time.
      while (totalSize.get() < ONE_THIRD_WAL_BUFFER_SIZE * FSYNC_BUFFER_RATIO) {
        DeletionResource deletionResource = null;
        try {
          // Timeout config keep consistent with WAL async mode.
          deletionResource =
              deletionResources.poll(config.getWalAsyncModeFsyncDelayInMs(), TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
          LOGGER.warn(
              "Interrupted when waiting for taking WALEntry from blocking queue to serialize.");
          Thread.currentThread().interrupt();
        }
        // If timeout, flush deletions to disk.
        if (deletionResource == null) {
          // append to current file and not switch file
          appendCurrentBatch();
          fsyncCurrentLoggingFile();
          resetTaskAttribute();
          return;
        }
        // Serialize deletion
        if (!serializeDeletionToBatchBuffer(deletionResource)) {
          // if working buffer is exhausted, which means serialization failed.
          // 1. roll back
          deletionResources.add(deletionResource);
          // 2. fsync immediately and roll to a new file.
          appendCurrentBatch();
          closeCurrentLoggingFile();
          resetTaskAttribute();
          switchLoggingFile();
          return;
        }
        pendingDeletionsInOneTask.add(deletionResource);
        // Update max progressIndex in current file if serialized successfully.
        maxProgressIndexInCurrentFile =
            maxProgressIndexInCurrentFile.updateToMinimumEqualOrIsAfterProgressIndex(
                deletionResource.getProgressIndex());
      }
      // Persist deletions; Defensive programming here, just in case.
      if (totalSize.get() > 0) {
        appendCurrentBatch();
        closeCurrentLoggingFile();
        resetTaskAttribute();
        switchLoggingFile();
      }
    }
  }
}
