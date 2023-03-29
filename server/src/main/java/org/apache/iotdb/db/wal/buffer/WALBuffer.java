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
package org.apache.iotdb.db.wal.buffer;

import org.apache.iotdb.commons.concurrent.IoTDBThreadPoolFactory;
import org.apache.iotdb.commons.concurrent.ThreadName;
import org.apache.iotdb.commons.conf.CommonDescriptor;
import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.write.DeleteDataNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.write.InsertNode;
import org.apache.iotdb.db.service.metrics.recorder.WritingMetricsManager;
import org.apache.iotdb.db.utils.MmapUtil;
import org.apache.iotdb.db.wal.exception.WALNodeClosedException;
import org.apache.iotdb.db.wal.io.WALMetaData;
import org.apache.iotdb.db.wal.utils.WALFileStatus;
import org.apache.iotdb.db.wal.utils.listener.WALFlushListener;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import static org.apache.iotdb.db.wal.node.WALNode.DEFAULT_SEARCH_INDEX;

/**
 * This buffer guarantees the concurrent safety and uses double buffers mechanism to accelerate
 * writes and avoid waiting for buffer syncing to disk.
 */
public class WALBuffer extends AbstractWALBuffer {
  private static final Logger logger = LoggerFactory.getLogger(WALBuffer.class);
  private static final IoTDBConfig config = IoTDBDescriptor.getInstance().getConfig();
  private static final int HALF_WAL_BUFFER_SIZE = config.getWalBufferSize() / 2;
  private static final double FSYNC_BUFFER_RATIO = 0.95;
  private static final int QUEUE_CAPACITY = config.getWalBufferQueueCapacity();
  private static final WritingMetricsManager WRITING_METRICS = WritingMetricsManager.getInstance();

  /** whether close method is called */
  private volatile boolean isClosed = false;
  /** WALEntries */
  private final BlockingQueue<WALEntry> walEntries = new ArrayBlockingQueue<>(QUEUE_CAPACITY);
  /** lock to provide synchronization for double buffers mechanism, protecting buffers status */
  private final Lock buffersLock = new ReentrantLock();
  /** condition to guarantee correctness of switching buffers */
  private final Condition idleBufferReadyCondition = buffersLock.newCondition();
  // region these variables should be protected by buffersLock
  /** two buffers switch between three statuses (there is always 1 buffer working) */
  // buffer in working status, only updated by serializeThread
  private volatile ByteBuffer workingBuffer;
  // buffer in idle status
  private volatile ByteBuffer idleBuffer;
  // buffer in syncing status, serializeThread makes sure no more writes to syncingBuffer
  private volatile ByteBuffer syncingBuffer;
  // endregion
  /** file status of working buffer, updating file writer's status when syncing */
  protected volatile WALFileStatus currentFileStatus;
  /** single thread to serialize WALEntry to workingBuffer */
  private final ExecutorService serializeThread;
  /** single thread to sync syncingBuffer to disk */
  private final ExecutorService syncBufferThread;

  public WALBuffer(String identifier, String logDirectory) throws FileNotFoundException {
    this(identifier, logDirectory, 0, 0L);
  }

  public WALBuffer(
      String identifier, String logDirectory, long startFileVersion, long startSearchIndex)
      throws FileNotFoundException {
    super(identifier, logDirectory, startFileVersion, startSearchIndex);
    currentFileStatus = WALFileStatus.CONTAINS_NONE_SEARCH_INDEX;
    allocateBuffers();
    serializeThread =
        IoTDBThreadPoolFactory.newSingleThreadExecutor(
            ThreadName.WAL_SERIALIZE.getName() + "(node-" + identifier + ")");
    syncBufferThread =
        IoTDBThreadPoolFactory.newSingleThreadExecutor(
            ThreadName.WAL_SYNC.getName() + "(node-" + identifier + ")");
    // start receiving serialize tasks
    serializeThread.submit(new SerializeTask());
  }

  private void allocateBuffers() {
    try {
      workingBuffer = ByteBuffer.allocateDirect(HALF_WAL_BUFFER_SIZE);
      idleBuffer = ByteBuffer.allocateDirect(HALF_WAL_BUFFER_SIZE);
    } catch (OutOfMemoryError e) {
      logger.error("Fail to allocate wal node-{}'s buffer because out of memory.", identifier, e);
      close();
      throw e;
    }
  }

  @Override
  public void write(WALEntry walEntry) {
    if (isClosed) {
      logger.error(
          "Fail to write WALEntry into wal node-{} because this node is closed.", identifier);
      walEntry.getWalFlushListener().fail(new WALNodeClosedException(identifier));
      return;
    }
    // just add this WALEntry to queue
    try {
      walEntries.put(walEntry);
    } catch (InterruptedException e) {
      logger.warn("Interrupted when waiting for adding WALEntry to buffer.");
      Thread.currentThread().interrupt();
    }
  }

  // region Task of serializeThread
  /** This info class traverses some extra info from serializeThread to syncBufferThread */
  private static class SerializeInfo {
    final WALMetaData metaData = new WALMetaData();
    final List<WALFlushListener> fsyncListeners = new LinkedList<>();
    WALFlushListener rollWALFileWriterListener = null;
  }

  /** This task serializes WALEntry to workingBuffer and will call fsync at last. */
  private class SerializeTask implements Runnable {
    private final ByteBufferView byteBufferView = new ByteBufferView();
    private final SerializeInfo info = new SerializeInfo();
    private int totalSize = 0;

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

    /** In order to control memory usage of blocking queue, get 1 and then serialize 1 */
    private void serialize() {
      // try to get first WALEntry with blocking interface
      long start = System.nanoTime();
      try {
        WALEntry firstWALEntry = walEntries.take();
        boolean returnFlag = handleWALEntry(firstWALEntry);
        if (returnFlag) {
          WRITING_METRICS.recordSerializeWALEntryTotalCost(System.nanoTime() - start);
          return;
        }
      } catch (InterruptedException e) {
        logger.warn(
            "Interrupted when waiting for taking WALEntry from blocking queue to serialize.");
        Thread.currentThread().interrupt();
      }

      // try to get more WALEntries with blocking interface to enlarge write batch
      while (totalSize < HALF_WAL_BUFFER_SIZE * FSYNC_BUFFER_RATIO) {
        WALEntry walEntry = null;
        try {
          // for better fsync performance, wait a while to enlarge write batch
          walEntry = walEntries.poll(config.getFsyncWalDelayInMs(), TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
          logger.warn(
              "Interrupted when waiting for taking WALEntry from blocking queue to serialize.");
          Thread.currentThread().interrupt();
        }

        if (walEntry == null) {
          break;
        }
        boolean returnFlag = handleWALEntry(walEntry);
        if (returnFlag) {
          WRITING_METRICS.recordSerializeWALEntryTotalCost(System.nanoTime() - start);
          return;
        }
      }
      WRITING_METRICS.recordSerializeWALEntryTotalCost(System.nanoTime() - start);

      // call fsync at last and set fsyncListeners
      if (totalSize > 0) {
        fsyncWorkingBuffer(currentSearchIndex, currentFileStatus, info);
      }
    }

    /**
     * @return true if fsyncWorkingBuffer has been called, which means this serialization task
     *     should be ended.
     */
    private boolean handleWALEntry(WALEntry walEntry) {
      if (walEntry.isSignal()) {
        return handleSignalEntry((WALSignalEntry) walEntry);
      }

      boolean success = handleInfoEntry(walEntry);
      if (success) {
        info.fsyncListeners.add(walEntry.getWalFlushListener());
      }
      return false;
    }

    /**
     * Handle a normal WALEntry.
     *
     * @return true if serialization is successful.
     */
    private boolean handleInfoEntry(WALEntry walEntry) {
      int size = byteBufferView.position();
      try {
        long start = System.nanoTime();
        walEntry.serialize(byteBufferView);
        WRITING_METRICS.recordSerializeOneWALInfoEntryCost(System.nanoTime() - start);
        size = byteBufferView.position() - size;
      } catch (Exception e) {
        logger.error(
            "Fail to serialize WALEntry to wal node-{}'s buffer, discard it.", identifier, e);
        walEntry.getWalFlushListener().fail(e);
        return false;
      }
      // update search index
      long searchIndex = DEFAULT_SEARCH_INDEX;
      if (walEntry.getType().needSearch()) {
        if (walEntry.getType() == WALEntryType.DELETE_DATA_NODE) {
          searchIndex = ((DeleteDataNode) walEntry.getValue()).getSearchIndex();
        } else {
          searchIndex = ((InsertNode) walEntry.getValue()).getSearchIndex();
        }
        if (searchIndex != DEFAULT_SEARCH_INDEX) {
          currentSearchIndex = searchIndex;
          currentFileStatus = WALFileStatus.CONTAINS_SEARCH_INDEX;
        }
      }
      totalSize += size;
      info.metaData.add(size, searchIndex);
      return true;
    }

    /**
     * @return true if fsyncWorkingBuffer has been called, which means this serialization task
     *     should be ended.
     */
    private boolean handleSignalEntry(WALSignalEntry walSignalEntry) {
      switch (walSignalEntry.getType()) {
        case ROLL_WAL_LOG_WRITER_SIGNAL:
          if (logger.isDebugEnabled()) {
            logger.debug("Handle roll log writer signal for wal node-{}.", identifier);
          }
          info.rollWALFileWriterListener = walSignalEntry.getWalFlushListener();
          fsyncWorkingBuffer(currentSearchIndex, currentFileStatus, info);
          return true;
        case CLOSE_SIGNAL:
          if (logger.isDebugEnabled()) {
            logger.debug(
                "Handle close signal for wal node-{}, there are {} entries left.",
                identifier,
                walEntries.size());
          }
          boolean dataExists = totalSize > 0;
          if (dataExists) {
            fsyncWorkingBuffer(currentSearchIndex, currentFileStatus, info);
          }
          isClosed = true;
          return dataExists;
        default:
          return false;
      }
    }
  }

  /**
   * This view uses workingBuffer lock-freely because workingBuffer is only updated by
   * serializeThread and this class is only used by serializeThread.
   */
  private class ByteBufferView implements IWALByteBufferView {
    private int flushedBytesNum = 0;

    private void ensureEnoughSpace(int bytesNum) {
      if (workingBuffer.remaining() < bytesNum) {
        rollBuffer();
      }
    }

    private void rollBuffer() {
      flushedBytesNum += workingBuffer.position();
      syncWorkingBuffer(currentSearchIndex, currentFileStatus);
    }

    @Override
    public void put(byte b) {
      ensureEnoughSpace(Byte.BYTES);
      workingBuffer.put(b);
    }

    @Override
    public void put(byte[] src) {
      int offset = 0;
      while (true) {
        int leftCapacity = workingBuffer.remaining();
        int needCapacity = src.length - offset;
        if (leftCapacity >= needCapacity) {
          workingBuffer.put(src, offset, needCapacity);
          break;
        } else {
          workingBuffer.put(src, offset, leftCapacity);
          offset += leftCapacity;
          rollBuffer();
        }
      }
    }

    @Override
    public void putChar(char value) {
      ensureEnoughSpace(Character.BYTES);
      workingBuffer.putChar(value);
    }

    @Override
    public void putShort(short value) {
      ensureEnoughSpace(Short.BYTES);
      workingBuffer.putShort(value);
    }

    @Override
    public void putInt(int value) {
      ensureEnoughSpace(Integer.BYTES);
      workingBuffer.putInt(value);
    }

    @Override
    public void putLong(long value) {
      ensureEnoughSpace(Long.BYTES);
      workingBuffer.putLong(value);
    }

    @Override
    public void putFloat(float value) {
      ensureEnoughSpace(Float.BYTES);
      workingBuffer.putFloat(value);
    }

    @Override
    public void putDouble(double value) {
      ensureEnoughSpace(Double.BYTES);
      workingBuffer.putDouble(value);
    }

    @Override
    public int position() {
      return flushedBytesNum + workingBuffer.position();
    }
  }

  /** Notice: this method only called when buffer is exhausted by SerializeTask. */
  private void syncWorkingBuffer(long searchIndex, WALFileStatus fileStatus) {
    switchWorkingBufferToFlushing();
    syncBufferThread.submit(new SyncBufferTask(searchIndex, fileStatus, false));
    currentFileStatus = WALFileStatus.CONTAINS_NONE_SEARCH_INDEX;
  }

  /** Notice: this method only called at the last of SerializeTask. */
  private void fsyncWorkingBuffer(long searchIndex, WALFileStatus fileStatus, SerializeInfo info) {
    switchWorkingBufferToFlushing();
    syncBufferThread.submit(new SyncBufferTask(searchIndex, fileStatus, true, info));
    currentFileStatus = WALFileStatus.CONTAINS_NONE_SEARCH_INDEX;
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
      logger.warn("Interrupted When waiting for available working buffer.");
      Thread.currentThread().interrupt();
    } finally {
      buffersLock.unlock();
    }
  }
  // endregion

  // region Task of syncBufferThread
  /**
   * This task syncs syncingBuffer to disk. The precondition is that syncingBuffer cannot be null.
   */
  private class SyncBufferTask implements Runnable {
    private final long searchIndex;
    private final WALFileStatus fileStatus;
    private final boolean forceFlag;
    private final SerializeInfo info;

    public SyncBufferTask(long searchIndex, WALFileStatus fileStatus, boolean forceFlag) {
      this(searchIndex, fileStatus, forceFlag, null);
    }

    public SyncBufferTask(
        long searchIndex, WALFileStatus fileStatus, boolean forceFlag, SerializeInfo info) {
      this.searchIndex = searchIndex;
      this.fileStatus = fileStatus;
      this.forceFlag = forceFlag;
      this.info = info == null ? new SerializeInfo() : info;
    }

    @Override
    public void run() {
      long start = System.nanoTime();
      currentWALFileWriter.updateFileStatus(fileStatus);

      double usedRatio = (double) syncingBuffer.position() / syncingBuffer.capacity();
      WRITING_METRICS.recordWALBufferUsedRatio(usedRatio);
      logger.debug(
          "Sync wal buffer, forceFlag: {}, buffer used: {} / {} = {}%",
          forceFlag, syncingBuffer.position(), syncingBuffer.capacity(), usedRatio * 100);

      // flush buffer to os
      try {
        currentWALFileWriter.write(syncingBuffer, info.metaData);
      } catch (Throwable e) {
        logger.error(
            "Fail to sync wal node-{}'s buffer, change system mode to error.", identifier, e);
        CommonDescriptor.getInstance().getConfig().handleUnrecoverableError();
      } finally {
        switchSyncingBufferToIdle();
      }

      boolean forceSuccess = false;
      // try to roll log writer
      if (info.rollWALFileWriterListener != null
          || (forceFlag && currentWALFileWriter.size() >= config.getWalFileSizeThresholdInByte())) {
        try {
          rollLogWriter(searchIndex, currentWALFileWriter.getWalFileStatus());
          forceSuccess = true;
          if (info.rollWALFileWriterListener != null) {
            info.rollWALFileWriterListener.succeed();
          }
        } catch (IOException e) {
          logger.error(
              "Fail to roll wal node-{}'s log writer, change system mode to error.", identifier, e);
          if (info.rollWALFileWriterListener != null) {
            info.rollWALFileWriterListener.fail(e);
          }
          CommonDescriptor.getInstance().getConfig().handleUnrecoverableError();
        }
      } else if (forceFlag) { // force os cache to the storage device, avoid force twice by judging
        // after rolling file
        try {
          currentWALFileWriter.force();
          forceSuccess = true;
        } catch (IOException e) {
          logger.error(
              "Fail to fsync wal node-{}'s log writer, change system mode to error.",
              identifier,
              e);
          for (WALFlushListener fsyncListener : info.fsyncListeners) {
            fsyncListener.fail(e);
          }
          CommonDescriptor.getInstance().getConfig().handleUnrecoverableError();
        }
      }

      // notify all waiting listeners
      if (forceSuccess) {
        for (WALFlushListener fsyncListener : info.fsyncListeners) {
          fsyncListener.succeed();
        }
      }
      WRITING_METRICS.recordWALBufferEntriesCount(info.fsyncListeners.size());
      WRITING_METRICS.recordSyncWALBufferCost(System.nanoTime() - start, forceFlag);
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

  @Override
  public void waitForFlush() throws InterruptedException {
    buffersLock.lock();
    try {
      idleBufferReadyCondition.await();
    } finally {
      buffersLock.unlock();
    }
  }

  @Override
  public boolean waitForFlush(long time, TimeUnit unit) throws InterruptedException {
    buffersLock.lock();
    try {
      return idleBufferReadyCondition.await(time, unit);
    } finally {
      buffersLock.unlock();
    }
  }
  // endregion

  @Override
  public void close() {
    // first waiting serialize and sync tasks finished, then release all resources
    if (serializeThread != null) {
      // add close signal WALEntry to notify serializeThread
      try {
        walEntries.put(new WALSignalEntry(WALEntryType.CLOSE_SIGNAL));
      } catch (InterruptedException e) {
        logger.error("Fail to put CLOSE_SIGNAL to walEntries.", e);
      }
      isClosed = true;
      shutdownThread(serializeThread, ThreadName.WAL_SERIALIZE);
    }
    if (syncBufferThread != null) {
      shutdownThread(syncBufferThread, ThreadName.WAL_SYNC);
    }

    if (currentWALFileWriter != null) {
      try {
        currentWALFileWriter.close();
      } catch (IOException e) {
        logger.error("Fail to close wal node-{}'s log writer.", identifier, e);
      }
    }

    if (workingBuffer != null) {
      MmapUtil.clean((MappedByteBuffer) workingBuffer);
    }
    if (idleBuffer != null) {
      MmapUtil.clean((MappedByteBuffer) workingBuffer);
    }
    if (syncingBuffer != null) {
      MmapUtil.clean((MappedByteBuffer) syncingBuffer);
    }
  }

  private void shutdownThread(ExecutorService thread, ThreadName threadName) {
    thread.shutdown();
    try {
      if (!thread.awaitTermination(30, TimeUnit.SECONDS)) {
        logger.warn("Waiting thread {} to be terminated is timeout", threadName.getName());
      }
    } catch (InterruptedException e) {
      logger.warn("Thread {} still doesn't exit after 30s", threadName.getName());
      Thread.currentThread().interrupt();
    }
  }

  @Override
  public boolean isAllWALEntriesConsumed() {
    buffersLock.lock();
    try {
      return walEntries.isEmpty() && workingBuffer.position() == 0 && syncingBuffer == null;
    } finally {
      buffersLock.unlock();
    }
  }
}
