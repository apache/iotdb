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
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class DeletionBuffer implements AutoCloseable {
  private static final Logger LOGGER = LoggerFactory.getLogger(DeletionBuffer.class);
  private static final IoTDBConfig config = IoTDBDescriptor.getInstance().getConfig();
  // TODO: make it deletion own
  private static final int ONE_THIRD_WAL_BUFFER_SIZE = config.getWalBufferSize() / 3;
  private static final double FSYNC_BUFFER_RATIO = 0.95;
  private static final int QUEUE_CAPACITY = config.getWalBufferQueueCapacity();

  // whether close method is called
  private volatile boolean isClosed = false;
  // DeletionResources
  private final BlockingQueue<DeletionResource> deletionResources =
      new ArrayBlockingQueue<>(QUEUE_CAPACITY);
  // lock to provide synchronization for double buffers mechanism, protecting buffers status
  private final Lock buffersLock = new ReentrantLock();
  // condition to guarantee correctness of switching buffers
  private final Condition idleBufferReadyCondition = buffersLock.newCondition();
  private final String groupId;

  // region these variables should be protected by buffersLock
  /** two buffers switch between three statuses (there is always 1 buffer working). */
  // buffer in working status, only updated by serializeThread
  // it's safe to use volatile here to make this reference thread-safe.
  @SuppressWarnings("squid:S3077")
  private volatile ByteBuffer workingBuffer;

  // buffer in idle status
  // it's safe to use volatile here to make this reference thread-safe.
  @SuppressWarnings("squid:S3077")
  private volatile ByteBuffer idleBuffer;

  // buffer in syncing status, serializeThread makes sure no more writes to syncingBuffer
  // it's safe to use volatile here to make this reference thread-safe.
  @SuppressWarnings("squid:S3077")
  private volatile ByteBuffer syncingBuffer;

  // single thread to serialize WALEntry to workingBuffer
  private final ExecutorService serializeThread;
  // single thread to sync syncingBuffer to disk
  private final ExecutorService syncBufferThread;
  // directory to store .deletion files
  private final String baseDirectory;

  public DeletionBuffer(String groupId, String baseDirectory) {
    this.groupId = groupId;
    this.baseDirectory = baseDirectory;
    allocateBuffers();
    serializeThread =
        IoTDBThreadPoolFactory.newSingleThreadExecutor(
            ThreadName.PIPE_CONSENSUS_DELETION_SERIALIZE.getName() + "(group-" + groupId + ")");
    syncBufferThread =
        IoTDBThreadPoolFactory.newSingleThreadExecutor(
            ThreadName.PIPE_CONSENSUS_DELETION_SYNC.getName() + "(group-" + groupId + ")");
  }

  public void start() {
    // Start serialize and sync pipeline.
    serializeThread.submit(new SerializeTask());
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

  private void allocateBuffers() {
    try {
      workingBuffer = ByteBuffer.allocateDirect(ONE_THIRD_WAL_BUFFER_SIZE);
      idleBuffer = ByteBuffer.allocateDirect(ONE_THIRD_WAL_BUFFER_SIZE);
    } catch (OutOfMemoryError e) {
      LOGGER.error(
          "Fail to allocate deletionBuffer-group-{}'s buffer because out of memory.", groupId, e);
      close();
      throw e;
    }
  }

  /** Notice: this method only called when buffer is exhausted by SerializeTask. */
  private void syncWorkingBuffer(ProgressIndex maxProgressIndexInCurrentBatch, int deletionNum) {
    switchWorkingBufferToFlushing();
    try {
      syncBufferThread.submit(new SyncBufferTask(maxProgressIndexInCurrentBatch, deletionNum));
    } catch (IOException e) {
      LOGGER.warn(
          "Failed to submit syncBufferTask, May because file open error and cause data inconsistency. Please check your file system. ",
          e);
    }
  }

  // only called by serializeThread
  private void switchWorkingBufferToFlushing() {
    buffersLock.lock();
    try {
      while (idleBuffer == null) {
        idleBufferReadyCondition.await();
      }
      syncingBuffer = workingBuffer;
      workingBuffer = idleBuffer;
      workingBuffer.clear();
      idleBuffer = null;
    } catch (InterruptedException e) {
      LOGGER.warn("Interrupted When waiting for available working buffer.");
      Thread.currentThread().interrupt();
    } finally {
      buffersLock.unlock();
    }
  }

  private class SerializeTask implements Runnable {
    // Total size of this batch.
    private int totalSize = 0;
    // Deletion num of this batch.
    private int deletionNum = 0;
    // Max progressIndex among this batch. Used by SyncTask for naming .deletion file.
    private ProgressIndex maxProgressIndexInCurrentBatch = MinimumProgressIndex.INSTANCE;

    @Override
    public void run() {
      try {
        serialize();
      } finally {
        if (!isClosed) {
          serializeThread.submit(new SerializeTask());
        }
      }
    }

    private void serialize() {
      // For first deletion we use blocking take() method.
      try {
        DeletionResource firstDeletionResource = deletionResources.take();
        // For first serialization, we don't need to judge whether working buffer is exhausted.
        // Because a single DeleteDataNode can't exceed size of working buffer.
        serializeToWorkingBuffer(firstDeletionResource);
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
      while (totalSize < ONE_THIRD_WAL_BUFFER_SIZE * FSYNC_BUFFER_RATIO) {
        DeletionResource deletionResource = null;
        try {
          // TODO: add deletion timeout to config
          deletionResource =
              deletionResources.poll(config.getWalSyncModeFsyncDelayInMs(), TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
          LOGGER.warn(
              "Interrupted when waiting for taking WALEntry from blocking queue to serialize.");
          Thread.currentThread().interrupt();
        }
        // If timeout, flush deletions to disk.
        if (deletionResource == null) {
          break;
        }
        // Serialize deletion
        while (!serializeToWorkingBuffer(deletionResource)) {
          // If working buffer is exhausted, submit a syncTask to consume current batch and switch
          // buffer to start a new batch.
          syncWorkingBuffer(maxProgressIndexInCurrentBatch, deletionNum);
          // Reset maxProgressIndex and deletionNum for new batch.
          maxProgressIndexInCurrentBatch = MinimumProgressIndex.INSTANCE;
          deletionNum = 0;
        }
        // Update max progressIndex
        maxProgressIndexInCurrentBatch =
            maxProgressIndexInCurrentBatch.updateToMinimumEqualOrIsAfterProgressIndex(
                deletionResource.getProgressIndex());
      }
      // Persist deletions; Defensive programming here, just in case.
      if (totalSize > 0) {
        syncWorkingBuffer(maxProgressIndexInCurrentBatch, deletionNum);
      }
    }

    /**
     * Serialize deletionResource to working buffer. Return true if serialize successfully, false
     * otherwise.
     */
    private boolean serializeToWorkingBuffer(DeletionResource deletionResource) {
      ByteBuffer buffer = deletionResource.serialize();
      // if working buffer doesn't have enough space
      if (buffer.position() > workingBuffer.remaining()) {
        return false;
      }
      workingBuffer.put(buffer.array());
      totalSize += buffer.position();
      deletionNum++;
      return true;
    }
  }

  // only called by syncBufferThread
  private void switchSyncingBufferToIdle() {
    buffersLock.lock();
    try {
      // No need to judge whether idleBuffer is null because syncingBuffer is not null
      // and there is only one buffer can be null between syncingBuffer and idleBuffer
      idleBuffer = syncingBuffer;
      syncingBuffer = null;
      idleBufferReadyCondition.signalAll();
    } finally {
      buffersLock.unlock();
    }
  }

  private class SyncBufferTask implements Runnable {
    private final int deletionNum;
    private final File logFile;
    private final FileOutputStream logStream;
    private final FileChannel logChannel;

    public SyncBufferTask(ProgressIndex maxProgressIndexInCurrentBatch, int deletionNum)
        throws IOException {
      this.deletionNum = deletionNum;
      // PipeConsensus ensures that deleteDataNodes use recoverProgressIndex.
      ProgressIndex curProgressIndex =
          ProgressIndexDataNodeManager.extractLocalSimpleProgressIndex(
              maxProgressIndexInCurrentBatch);
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

    @Override
    public void run() {
      // Sync deletion to disk.
      workingBuffer.flip();
      try {
        // Write metaData.
        ByteBuffer metaData = ByteBuffer.allocate(4);
        metaData.putInt(deletionNum);
        metaData.flip();
        this.logChannel.write(metaData);
        // Write deletions.
        syncingBuffer.flip();
        this.logChannel.write(syncingBuffer);
      } catch (IOException e) {
        LOGGER.warn(
            "Deletion persist: Cannot write to {}, may cause data inconsistency.", logFile, e);
      } finally {
        switchSyncingBufferToIdle();
      }
      // Close resource.
      try {
        this.logChannel.close();
        this.logStream.close();
      } catch (IOException e) {
        LOGGER.warn("Failed to close deletion writing resource when writing to {}.", logFile, e);
      }
    }
  }

  @Override
  public void close() {
    isClosed = true;
    // Force sync existing data in memory to disk.
    // first waiting serialize and sync tasks finished, then release all resources
    if (serializeThread != null) {
      shutdownThread(serializeThread, ThreadName.PIPE_CONSENSUS_DELETION_SERIALIZE);
    }
    if (syncBufferThread != null) {
      shutdownThread(syncBufferThread, ThreadName.PIPE_CONSENSUS_DELETION_SYNC);
    }

    MmapUtil.clean(workingBuffer);
    MmapUtil.clean(workingBuffer);
    MmapUtil.clean(syncingBuffer);
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
