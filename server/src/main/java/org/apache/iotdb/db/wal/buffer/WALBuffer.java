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

import org.apache.iotdb.db.concurrent.IoTDBThreadPoolFactory;
import org.apache.iotdb.db.concurrent.ThreadName;
import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.qp.physical.crud.DeletePlan;
import org.apache.iotdb.db.utils.MmapUtil;
import org.apache.iotdb.db.wal.exception.WALNodeClosedException;
import org.apache.iotdb.db.wal.utils.listener.WALFlushListener;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * This buffer guarantees the concurrent safety and uses double buffers mechanism to accelerate
 * writes and avoid waiting for buffer syncing to disk.
 */
public class WALBuffer extends AbstractWALBuffer {
  private static final Logger logger = LoggerFactory.getLogger(WALBuffer.class);
  private static final IoTDBConfig config = IoTDBDescriptor.getInstance().getConfig();
  private static final long SYNC_WAL_DELAY_IN_MS = config.getSyncWalDelayInMs();
  private static final int WAL_BUFFER_SIZE = config.getWalBufferSize();
  private static final int BATCH_SIZE_LIMIT = 100;
  private static final WALEdit CLOSE_SIGNAL = new WALEdit(-1, new DeletePlan());

  /** whether close method is called */
  private volatile boolean isClosed = false;
  /** WALEdits */
  private final BlockingQueue<WALEdit> walEdits = new ArrayBlockingQueue<>(BATCH_SIZE_LIMIT * 10);
  /** two buffers switch between three statuses (there is always 1 buffer working) */
  // buffer in working status, only updated by serializeThread
  private volatile ByteBuffer workingBuffer;
  // buffer in idle status
  private volatile ByteBuffer idleBuffer;
  // buffer in syncing status, serializeThread makes sure no more writes to syncingBuffer
  private volatile ByteBuffer syncingBuffer;
  /** lock to provide synchronization for double buffers mechanism */
  private final Lock buffersLock = new ReentrantLock();
  /** condition to guarantee correctness of switching buffers */
  private final Condition idleBufferReadyCondition = buffersLock.newCondition();
  /** single thread to serialize WALEdit to workingBuffer */
  private final ExecutorService serializeThread;
  /** single thread to sync syncingBuffer to disk */
  private final ExecutorService syncBufferThread;

  public WALBuffer(String identifier, String logDirectory) throws FileNotFoundException {
    super(identifier, logDirectory);
    allocateBuffers();
    serializeThread =
        IoTDBThreadPoolFactory.newSingleThreadExecutor(
            ThreadName.WAL_SERIALIZE.getName() + "/" + identifier);
    syncBufferThread =
        IoTDBThreadPoolFactory.newSingleThreadExecutor(
            ThreadName.WAL_SYNC.getName() + "/" + identifier);
    // start receiving serialize tasks
    serializeThread.submit(new SerializeTask());
  }

  private void allocateBuffers() {
    try {
      workingBuffer = ByteBuffer.allocateDirect(WAL_BUFFER_SIZE / 2);
      idleBuffer = ByteBuffer.allocateDirect(WAL_BUFFER_SIZE / 2);
    } catch (OutOfMemoryError e) {
      logger.error("Fail to allocate wal node-{}'s buffer because out of memory.", identifier, e);
      close();
      throw e;
    }
  }

  @Override
  public void write(WALEdit edit) {
    if (isClosed) {
      logger.error(
          "Fail to write WALEdit into wal node-{} because this node is closed.", identifier);
      edit.getWalFlushListener().fail(new WALNodeClosedException(identifier));
      return;
    }
    // only add this WALEdit to queue
    try {
      walEdits.put(edit);
    } catch (InterruptedException e) {
      logger.warn("Interrupted when waiting for adding WalEdit to buffer.");
      Thread.currentThread().interrupt();
    }
  }

  // region Task of serializeThread
  /** This task serializes WALEdit to workingBuffer and will call fsync at last. */
  private class SerializeTask implements Runnable {
    private final IWALByteBufferView byteBufferVew = new ByteBufferView();
    private final List<WALFlushListener> fsyncListeners = new LinkedList<>();

    @Override
    public void run() {
      try {
        serialize();
      } finally {
        serializeThread.submit(new SerializeTask());
      }
    }

    /** In order to control memory usage of blocking queue, get 1 and then serialize 1 */
    private void serialize() {
      // for better fsync performance, sleep a while to enlarge write batch
      if (SYNC_WAL_DELAY_IN_MS > 0) {
        try {
          Thread.sleep(SYNC_WAL_DELAY_IN_MS);
        } catch (InterruptedException e) {
          logger.warn("Interrupted when sleeping a while to enlarge wal write batch.");
          Thread.currentThread().interrupt();
        }
      }
      // try to get first WALEdit with blocking interface
      int batchSize = 0;
      try {
        WALEdit edit = walEdits.take();
        try {
          if (edit != CLOSE_SIGNAL) {
            edit.serialize(byteBufferVew);
            ++batchSize;
            fsyncListeners.add(edit.getWalFlushListener());
          }
        } catch (Exception e) {
          logger.error(
              "Fail to serialize WALEdit to wal node-{}'s buffer, discard it.", identifier, e);
          edit.getWalFlushListener().fail(e);
        }
      } catch (InterruptedException e) {
        logger.warn(
            "Interrupted when waiting for taking WALEdit from blocking queue to serialize.");
        Thread.currentThread().interrupt();
      }
      // try to get more WALEdits with non-blocking interface to enlarge write batch
      while (walEdits.peek() != null && batchSize < BATCH_SIZE_LIMIT) {
        WALEdit edit = walEdits.poll();
        if (edit == null || edit == CLOSE_SIGNAL) {
          break;
        } else {
          try {
            edit.serialize(byteBufferVew);
          } catch (Exception e) {
            logger.error(
                "Fail to serialize WALEdit to wal node-{}'s buffer, discard it.", identifier, e);
            edit.getWalFlushListener().fail(e);
            continue;
          }
          ++batchSize;
          fsyncListeners.add(edit.getWalFlushListener());
        }
      }
      // call fsync at last and set fsyncListeners
      if (batchSize > 0) {
        fsyncWorkingBuffer(fsyncListeners);
      }
    }
  }

  /**
   * This view uses workingBuffer lock-freely because workingBuffer is only updated by
   * serializeThread and this class is only used by serializeThread.
   */
  private class ByteBufferView implements IWALByteBufferView {
    private void ensureEnoughSpace(int bytesNum) {
      if (workingBuffer.remaining() < bytesNum) {
        rollBuffer();
      }
    }

    private void rollBuffer() {
      syncWorkingBuffer();
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
  }

  /** Notice: this method only called when buffer is exhausted by SerializeTask. */
  private void syncWorkingBuffer() {
    switchIdleBufferToWorking();
    syncBufferThread.submit(new SyncBufferTask(false));
  }

  /** Notice: this method only called at the last of SerializeTask. */
  private void fsyncWorkingBuffer(List<WALFlushListener> fsyncListeners) {
    switchIdleBufferToWorking();
    syncBufferThread.submit(new SyncBufferTask(true, fsyncListeners));
  }

  // only called by serializeThread
  private void switchIdleBufferToWorking() {
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
    private final boolean force;
    private final List<WALFlushListener> fsyncListeners;

    public SyncBufferTask(boolean force) {
      this(force, Collections.emptyList());
    }

    public SyncBufferTask(boolean force, List<WALFlushListener> fsyncListeners) {
      this.force = force;
      this.fsyncListeners = fsyncListeners == null ? Collections.emptyList() : fsyncListeners;
    }

    @Override
    public void run() {
      // flush buffer to os
      try {
        currentLogWriter.write(syncingBuffer);
      } catch (Throwable e) {
        logger.error(
            "Fail to sync wal node-{}'s buffer, change system mode to read-only.", identifier, e);
        config.setReadOnly(true);
      } finally {
        switchSyncingBufferToIdle();
      }

      if (force) {
        // force os cache to the storage device
        try {
          currentLogWriter.force();
        } catch (IOException e) {
          logger.error(
              "Fail to fsync wal node-{}'s log writer, change system mode to read-only.",
              identifier,
              e);
          for (WALFlushListener fsyncListener : fsyncListeners) {
            fsyncListener.fail(e);
          }
          config.setReadOnly(true);
        }
        for (WALFlushListener fsyncListener : fsyncListeners) {
          fsyncListener.succeed();
        }
        // try to roll log writer
        try {
          tryRollingLogWriter();
        } catch (IOException e) {
          logger.error(
              "Fail to roll wal node-{}'s log writer, change system mode to read-only.",
              identifier,
              e);
          config.setReadOnly(true);
        }
      }
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
      idleBufferReadyCondition.signal();
    } finally {
      buffersLock.unlock();
    }
  }
  // endregion

  @Override
  public void close() {
    isClosed = true;
    // first waiting serialize and sync tasks finished, then release all resources
    if (serializeThread != null) {
      // add close signal WALEdit to notify serializeThread
      walEdits.add(CLOSE_SIGNAL);
      shutdownThread(serializeThread, ThreadName.WAL_SERIALIZE);
    }
    if (syncBufferThread != null) {
      shutdownThread(syncBufferThread, ThreadName.WAL_SYNC);
    }

    if (currentLogWriter != null) {
      try {
        currentLogWriter.close();
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
  public boolean isAllWALEditConsumed() {
    return walEdits.isEmpty();
  }
}
