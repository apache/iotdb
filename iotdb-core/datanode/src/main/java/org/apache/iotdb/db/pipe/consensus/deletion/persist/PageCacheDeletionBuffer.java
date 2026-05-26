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
import org.apache.iotdb.commons.utils.TestOnly;
import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.i18n.DataNodePipeMessages;
import org.apache.iotdb.db.pipe.consensus.ReplicateProgressDataNodeManager;
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
import java.util.List;
import java.util.Optional;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * This buffer writes deletions to the page cache and fsyncs them to disk when certain conditions
 * are met. This design does not decouple serialization and writing, but it is easier to maintain
 * and understand.
 */
public class PageCacheDeletionBuffer implements DeletionBuffer {
  private static final Logger LOGGER = LoggerFactory.getLogger(PageCacheDeletionBuffer.class);
  private static final IoTDBConfig config = IoTDBDescriptor.getInstance().getConfig();
  private static final double FSYNC_BUFFER_RATIO = 0.95;
  private static final int QUEUE_CAPACITY = config.getDeletionAheadLogBufferQueueCapacity();
  private static final long MAX_WAIT_CLOSE_TIME_IN_MS = 10000;

  // Keep the buffer config consistent with WAL.
  public static int DAL_BUFFER_SIZE = config.getWalBufferSize() / 3;

  // DeletionResources received from the storage engine and waiting to be persisted.
  private final BlockingQueue<DeletionResource> deletionResources =
      new PriorityBlockingQueue<>(
          QUEUE_CAPACITY,
          (o1, o2) ->
              o1.getProgressIndex().equals(o2.getProgressIndex())
                  ? 0
                  : (o1.getProgressIndex().isAfter(o2.getProgressIndex()) ? 1 : -1));
  // Data region id
  private final int dataRegionId;
  // Directory for storing .deletion files.
  private final String baseDirectory;
  // Single thread for serializing WALEntry to workingBuffer.
  private final ExecutorService persistThread;
  private final Lock buffersLock = new ReentrantLock();
  // Total size of this batch.
  private final AtomicInteger totalSize = new AtomicInteger(0);
  // All deletions that will be handled in a single persist task.
  private final List<DeletionResource> pendingDeletionsInOneTask = new CopyOnWriteArrayList<>();

  // Whether the close method is called.
  private volatile boolean isClosed = false;
  // Serialization buffer in the current persist task.
  private volatile ByteBuffer serializeBuffer;
  // Current logging file.
  private volatile File logFile;
  private volatile FileOutputStream logStream;
  private volatile FileChannel logChannel;
  // Max progressIndex among current .deletion file. Used by PersistTask for naming .deletion file.
  // Since deletions are written serially, DAL is also written serially. This ensures that the
  // maxProgressIndex of each batch increases in the same order as the physical time.
  private ProgressIndex maxProgressIndexInCurrentFile = MinimumProgressIndex.INSTANCE;

  public PageCacheDeletionBuffer(int dataRegionId, String baseDirectory) {
    this.dataRegionId = dataRegionId;
    this.baseDirectory = baseDirectory;
    allocateBuffers();
    persistThread =
        IoTDBThreadPoolFactory.newSingleThreadExecutor(
            ThreadName.IOT_CONSENSUS_V2_DELETION_SERIALIZE.getName()
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
          DataNodePipeMessages.DELETION_PERSIST_STARTING_TO_PERSIST_CURRENT_WRITING,
          dataRegionId,
          logFile);
    } catch (IOException e) {
      LOGGER.warn(
          DataNodePipeMessages.DELETION_PERSIST_CANNOT_CREATE_FILE_PLEASE_CHECK, logFile, e);
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
      serializeBuffer = ByteBuffer.allocateDirect(DAL_BUFFER_SIZE);
    } catch (OutOfMemoryError e) {
      LOGGER.error(
          DataNodePipeMessages.FAIL_TO_ALLOCATE_DELETIONBUFFER_GROUP_S_BUFFER, dataRegionId, e);
      close();
      throw e;
    }
  }

  public void registerDeletionResource(DeletionResource deletionResource) {
    if (isClosed) {
      LOGGER.error(
          DataNodePipeMessages.FAIL_TO_REGISTER_DELETIONRESOURCE_INTO_DELETIONBUFFER_BECAUSE,
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
    LOGGER.info(DataNodePipeMessages.DELETION_PERSIST_CURRENT_BATCH_FSYNC_DUE_TO, dataRegionId);
    this.logChannel.force(false);
    pendingDeletionsInOneTask.forEach(DeletionResource::onPersistSucceed);
  }

  private void closeCurrentLoggingFile(boolean notifySuccess) throws IOException {
    LOGGER.info(DataNodePipeMessages.DELETION_PERSIST_CURRENT_FILE_HAS_BEEN_CLOSED, dataRegionId);
    // Close old resource to fsync.
    if (this.logStream != null) {
      this.logStream.close();
    }
    if (this.logChannel != null) {
      this.logChannel.close();
    }
    if (notifySuccess) {
      pendingDeletionsInOneTask.forEach(DeletionResource::onPersistSucceed);
    }
  }

  private void resetTaskAttribute() {
    this.pendingDeletionsInOneTask.clear();
    clearBuffer();
  }

  private void resetFileAttribute() {
    // Reset file attributes.
    this.totalSize.set(0);
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
      ProgressIndex curProgressIndex =
          ReplicateProgressDataNodeManager.extractLocalSimpleProgressIndex(
              maxProgressIndexInCurrentFile);
      // IoTConsensusV2 ensures that deleteDataNodes use recoverProgressIndex.
      if (!(curProgressIndex instanceof SimpleProgressIndex)) {
        throw new IOException(
            DataNodePipeMessages.INVALID_DELETION_PROGRESS_INDEX + curProgressIndex);
      }
      SimpleProgressIndex progressIndex = (SimpleProgressIndex) curProgressIndex;
      // Deletion file name format:
      // "_{lastFileMaxRebootTimes}_{lastFileMaxMemTableFlushOrderId}.deletion"
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
          DataNodePipeMessages.DELETION_PERSIST_SWITCHING_TO_A_NEW_FILE, dataRegionId, logFile);
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
      persistThread.shutdownNow();
      try {
        if (!persistThread.awaitTermination(30, TimeUnit.SECONDS)) {
          LOGGER.warn(DataNodePipeMessages.PERSISTTHREAD_DID_NOT_TERMINATE_WITHIN_S, 30);
        }
      } catch (InterruptedException e) {
        LOGGER.warn(DataNodePipeMessages.DAL_THREAD_STILL_DOESN_T_EXIT_AFTER, dataRegionId);
        Thread.currentThread().interrupt();
      }
    }
    // close file handler
    try {
      closeCurrentLoggingFile(false);
    } catch (IOException e) {
      LOGGER.error(DataNodePipeMessages.FAIL_TO_CLOSE_CURRENT_LOGGING_FILE_WHEN, e);
    }
    // clean buffer
    MmapUtil.clean(serializeBuffer);
    serializeBuffer = null;
  }

  private void waitUntilFlushAllDeletionsOrTimeOut() {
    long currentTime = System.currentTimeMillis();
    while (!isAllDeletionFlushed()
        && System.currentTimeMillis() - currentTime < MAX_WAIT_CLOSE_TIME_IN_MS) {
      try {
        Thread.sleep(50);
      } catch (InterruptedException e) {
        LOGGER.error(DataNodePipeMessages.INTERRUPTED_WHEN_WAITING_FOR_ALL_DELETIONS_FLUSHED);
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
        LOGGER.warn(DataNodePipeMessages.DELETION_PERSIST_CANNOT_WRITE_TO_MAY_CAUSE, logFile, e);
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
          DataNodePipeMessages.DELETION_PERSIST_SERIALIZE_DELETION_RESOURCE,
          dataRegionId,
          deletionResource);
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
        LOGGER.warn(DataNodePipeMessages.INTERRUPTED_WHEN_WAITING_FOR_TAKING_DELETIONRESOURCE_FROM);
        Thread.currentThread().interrupt();
        return;
      }

      // For further deletion, we use non-blocking poll() method to persist existing deletion of
      // current batch in time.
      while (totalSize.get() < DAL_BUFFER_SIZE * FSYNC_BUFFER_RATIO) {
        DeletionResource deletionResource = null;
        try {
          // Timeout config keep consistent with WAL async mode.
          deletionResource =
              deletionResources.poll(config.getWalAsyncModeFsyncDelayInMs(), TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
          LOGGER.warn(DataNodePipeMessages.INTERRUPTED_WHEN_WAITING_FOR_TAKING_WALENTRY_FROM);
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
          closeCurrentLoggingFile(true);
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
        closeCurrentLoggingFile(true);
        resetTaskAttribute();
        switchLoggingFile();
      }
    }
  }

  @TestOnly
  public static void setDalBufferSize(int dalBufferSize) {
    DAL_BUFFER_SIZE = dalBufferSize;
  }
}
