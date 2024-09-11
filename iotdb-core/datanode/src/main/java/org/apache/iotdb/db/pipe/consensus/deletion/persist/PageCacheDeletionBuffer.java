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
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * The core idea of this buffer is to delete writes to the Page cache and fsync when certain
 * conditions are met. This design does not decouple serialization and writing, but provides easier
 * writing.
 */
public class PageCacheDeletionBuffer implements DeletionBuffer {
  private static final Logger LOGGER = LoggerFactory.getLogger(TwoStageDeletionBuffer.class);
  private static final IoTDBConfig config = IoTDBDescriptor.getInstance().getConfig();
  // Buffer config keep consistent with WAL.
  private static final int ONE_THIRD_WAL_BUFFER_SIZE = config.getWalBufferSize() / 3;
  private static final double FSYNC_BUFFER_RATIO = 0.95;
  private static final int QUEUE_CAPACITY = config.getWalBufferQueueCapacity();

  // DeletionResources
  private final BlockingQueue<DeletionResource> deletionResources =
      new ArrayBlockingQueue<>(QUEUE_CAPACITY);
  // Data region id
  private final String groupId;
  // directory to store .deletion files
  private final String baseDirectory;
  // single thread to serialize WALEntry to workingBuffer
  private final ExecutorService persistThread;
  private final Lock buffersLock = new ReentrantLock();
  // Total size of this batch.
  private final AtomicInteger totalSize = new AtomicInteger(0);
  // All deletions that will be written to the current file
  private final List<DeletionResource> pendingDeletions = new ArrayList<>();

  // whether close method is called
  private volatile boolean isClosed = false;
  // Serialize buffer in current batch
  private volatile ByteBuffer serializeBuffer;
  // Current Logging file.
  private volatile File logFile;
  private volatile FileOutputStream logStream;
  private volatile FileChannel logChannel;
  // Max progressIndex among last batch. Used by PersistTask for naming .deletion file.
  // Since deletions are written serially, DAL is also written serially. This ensures that the
  // maxProgressIndex of each batch increases in the same order as the physical time.
  private volatile ProgressIndex maxProgressIndexInLastBatch = MinimumProgressIndex.INSTANCE;

  public PageCacheDeletionBuffer(String groupId, String baseDirectory) {
    this.groupId = groupId;
    this.baseDirectory = baseDirectory;
    allocateBuffers();
    persistThread =
        IoTDBThreadPoolFactory.newSingleThreadExecutor(
            ThreadName.PIPE_CONSENSUS_DELETION_SERIALIZE.getName() + "(group-" + groupId + ")");
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
      return deletionResources.isEmpty() && serializeBuffer.position() == 0;
    } finally {
      buffersLock.unlock();
    }
  }

  private void allocateBuffers() {
    try {
      serializeBuffer = ByteBuffer.allocateDirect(ONE_THIRD_WAL_BUFFER_SIZE);
    } catch (OutOfMemoryError e) {
      LOGGER.error(
          "Fail to allocate deletionBuffer-group-{}'s buffer because out of memory.", groupId, e);
      close();
      throw e;
    }
  }

  public void registerDeletionResource(DeletionResource deletionResource) {
    if (isClosed) {
      LOGGER.error(
          "Fail to register DeletionResource into deletionBuffer-{} because this buffer is closed.",
          groupId);
      return;
    }
    deletionResources.add(deletionResource);
  }

  private void appendCurrentBatch() throws IOException {
    serializeBuffer.flip();
    logChannel.write(serializeBuffer);
    // Mark DeletionResources to persisted once deletion has been written to page cache
    pendingDeletions.forEach(DeletionResource::onPersistSucceed);
    resetTaskAttribute();
  }

  private void fsyncCurrentLoggingFileAndReset(ProgressIndex curMaxProgressIndex)
      throws IOException {
    try {
      // Close old resource to fsync.
      this.logStream.close();
      this.logChannel.close();
    } finally {
      resetFileAttribute(curMaxProgressIndex);
    }
  }

  private void resetTaskAttribute() {
    this.pendingDeletions.clear();
    clearBuffer();
  }

  private void resetFileAttribute(ProgressIndex curMaxProgressIndex) {
    // Reset file attributes.
    this.totalSize.set(0);
    this.maxProgressIndexInLastBatch = curMaxProgressIndex;
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
    // PipeConsensus ensures that deleteDataNodes use recoverProgressIndex.
    ProgressIndex curProgressIndex =
        ProgressIndexDataNodeManager.extractLocalSimpleProgressIndex(maxProgressIndexInLastBatch);
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
  }

  private class PersistTask implements Runnable {
    // Max progressIndex among this batch. Used by SyncTask for naming .deletion file.
    private ProgressIndex maxProgressIndexInCurrentBatch = MinimumProgressIndex.INSTANCE;

    @Override
    public void run() {
      try {
        persistDeletion();
      } catch (IOException e) {
        LOGGER.warn(
            "Deletion persist: Cannot write to {}, may cause data inconsistency.", logFile, e);
        pendingDeletions.forEach(deletionResource -> deletionResource.onPersistFailed(e));
        resetFileAttribute(maxProgressIndexInLastBatch);
      } finally {
        if (!isClosed) {
          persistThread.submit(new PersistTask());
        }
      }
    }

    private boolean serializeDeletionToBatchBuffer(DeletionResource deletionResource) {
      ByteBuffer buffer = deletionResource.serialize();
      // if working buffer doesn't have enough space
      if (buffer.position() > serializeBuffer.remaining()) {
        return false;
      }
      serializeBuffer.put(buffer.array());
      totalSize.addAndGet(buffer.position());
      return true;
    }

    private void persistDeletion() throws IOException {
      // For first deletion we use blocking take() method.
      try {
        DeletionResource firstDeletionResource = deletionResources.take();
        pendingDeletions.add(firstDeletionResource);
        serializeDeletionToBatchBuffer(firstDeletionResource);
        maxProgressIndexInCurrentBatch =
            maxProgressIndexInCurrentBatch.updateToMinimumEqualOrIsAfterProgressIndex(
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
          return;
        }
        // Serialize deletion
        if (!serializeDeletionToBatchBuffer(deletionResource)) {
          // If working buffer is exhausted, fsync immediately and roll to a new file.
          appendCurrentBatch();
          fsyncCurrentLoggingFileAndReset(maxProgressIndexInCurrentBatch);
          switchLoggingFile();
          return;
        }
        // Update max progressIndex
        maxProgressIndexInCurrentBatch =
            maxProgressIndexInCurrentBatch.updateToMinimumEqualOrIsAfterProgressIndex(
                deletionResource.getProgressIndex());
        pendingDeletions.add(deletionResource);
      }
      // Persist deletions; Defensive programming here, just in case.
      if (totalSize.get() > 0) {
        appendCurrentBatch();
        fsyncCurrentLoggingFileAndReset(maxProgressIndexInCurrentBatch);
        switchLoggingFile();
      }
    }
  }

  @Override
  public void close() {
    isClosed = true;
    // Force sync existing data in memory to disk.
    // first waiting serialize and sync tasks finished, then release all resources
    if (persistThread != null) {
      shutdownThread(persistThread, ThreadName.PIPE_CONSENSUS_DELETION_SERIALIZE);
    }
    // clean buffer
    MmapUtil.clean(serializeBuffer);
  }

  private void shutdownThread(ExecutorService thread, ThreadName threadName) {
    thread.shutdown();
    try {
      if (!thread.awaitTermination(30, TimeUnit.SECONDS)) {
        LOGGER.warn("Waiting thread {} to be terminated is timeout", threadName.getName());
      }
    } catch (InterruptedException e) {
      LOGGER.warn("Thread {} still doesn't exit after 30s", threadName.getName());
      Thread.currentThread().interrupt();
    }
  }
}
