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
package org.apache.iotdb.db.writelog.node;

import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.conf.directories.DirectoryManager;
import org.apache.iotdb.db.engine.fileSystem.SystemFileFactory;
import org.apache.iotdb.db.qp.physical.PhysicalPlan;
import org.apache.iotdb.db.writelog.io.ILogReader;
import org.apache.iotdb.db.writelog.io.ILogWriter;
import org.apache.iotdb.db.writelog.io.LogWriter;
import org.apache.iotdb.db.writelog.io.MultiFileLogReader;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.BufferOverflowException;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.util.Arrays;
import java.util.Comparator;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.locks.ReentrantLock;

/** This WriteLogNode is used to manage insert ahead logs of a TsFile. */
public class ExclusiveWriteLogNode implements WriteLogNode, Comparable<ExclusiveWriteLogNode> {

  public static final String WAL_FILE_NAME = "wal";
  private static final Logger logger = LoggerFactory.getLogger(ExclusiveWriteLogNode.class);

  private String identifier;

  private String logDirectory;

  private ILogWriter currentFileWriter;

  private IoTDBConfig config = IoTDBDescriptor.getInstance().getConfig();

  private volatile ByteBuffer logBufferWorking;
  private volatile ByteBuffer logBufferIdle;
  private volatile ByteBuffer logBufferFlushing;

  // used for the convenience of deletion
  private volatile ByteBuffer[] bufferArray;

  private final Object switchBufferCondition = new Object();
  private ReentrantLock lock = new ReentrantLock();
  private static final ExecutorService FLUSH_BUFFER_THREAD_POOL =
      Executors.newCachedThreadPool(
          new ThreadFactoryBuilder().setNameFormat("Flush-WAL-Thread-%d").setDaemon(true).build());

  private long fileId = 0;
  private long lastFlushedId = 0;

  private int bufferedLogNum = 0;

  private boolean deleted = false;

  /**
   * constructor of ExclusiveWriteLogNode.
   *
   * @param identifier ExclusiveWriteLogNode identifier
   */
  public ExclusiveWriteLogNode(String identifier) {
    this.identifier = identifier;
    this.logDirectory =
        DirectoryManager.getInstance().getWALFolder() + File.separator + this.identifier;
    if (SystemFileFactory.INSTANCE.getFile(logDirectory).mkdirs()) {
      logger.info("create the WAL folder {}.", logDirectory);
    }
  }

  @Override
  public void initBuffer(ByteBuffer[] byteBuffers) {
    this.logBufferWorking = byteBuffers[0];
    this.logBufferIdle = byteBuffers[1];
    this.bufferArray = byteBuffers;
  }

  @Override
  public void write(PhysicalPlan plan) throws IOException {
    if (deleted) {
      throw new IOException("WAL node deleted");
    }
    long start = System.currentTimeMillis();
    lock.lock();
    long elapse = System.currentTimeMillis() - start;
    if (elapse > 5000) {
      logger.warn("[wal] ExclusiveWriteLogNode lock got cost: {}ms", elapse);
    }
    try {
      putLog(plan);
      if (bufferedLogNum >= config.getFlushWalThreshold()) {
        sync();
      }
    } catch (BufferOverflowException e) {
      throw new IOException("Log cannot fit into the buffer, please increase wal_buffer_size", e);
    } finally {
      lock.unlock();
    }
  }

  private void putLog(PhysicalPlan plan) {
    logBufferWorking.mark();
    try {
      plan.serialize(logBufferWorking);
    } catch (BufferOverflowException e) {
      logger.info("WAL BufferOverflow !");
      logBufferWorking.reset();
      sync();
      plan.serialize(logBufferWorking);
    }
    bufferedLogNum++;
  }

  @Override
  public void close() {
    sync();
    forceWal();
    lock.lock();
    try {
      synchronized (switchBufferCondition) {
        while (logBufferFlushing != null && !deleted) {
          switchBufferCondition.wait();
        }
        switchBufferCondition.notifyAll();
      }

      if (this.currentFileWriter != null) {
        this.currentFileWriter.close();
        logger.debug("WAL file {} is closed", currentFileWriter);
        this.currentFileWriter = null;
      }
      logger.debug("Log node {} closed successfully", identifier);
    } catch (IOException e) {
      logger.warn("Cannot close log node {} because:", identifier, e);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      logger.warn("Waiting for current buffer being flushed interrupted");
    } finally {
      lock.unlock();
    }
  }

  @Override
  public void forceSync() {
    if (deleted) {
      return;
    }
    sync();
    forceWal();
  }

  @Override
  public void notifyStartFlush() throws FileNotFoundException {
    lock.lock();
    try {
      close();
      nextFileWriter();
    } finally {
      lock.unlock();
    }
  }

  @Override
  public void notifyEndFlush() {
    lock.lock();
    try {
      File logFile =
          SystemFileFactory.INSTANCE.getFile(logDirectory, WAL_FILE_NAME + ++lastFlushedId);
      discard(logFile);
    } finally {
      lock.unlock();
    }
  }

  @Override
  public String getIdentifier() {
    return identifier;
  }

  @Override
  public String getLogDirectory() {
    return logDirectory;
  }

  @Override
  public ByteBuffer[] delete() throws IOException {
    lock.lock();
    try {
      close();
      FileUtils.deleteDirectory(SystemFileFactory.INSTANCE.getFile(logDirectory));
      deleted = true;
      return this.bufferArray;
    } finally {
      lock.unlock();
    }
  }

  @Override
  public ILogReader getLogReader() {
    File[] logFiles = SystemFileFactory.INSTANCE.getFile(logDirectory).listFiles();
    Arrays.sort(
        logFiles,
        Comparator.comparingInt(f -> Integer.parseInt(f.getName().replace(WAL_FILE_NAME, ""))));
    return new MultiFileLogReader(logFiles);
  }

  private void discard(File logFile) {
    if (!logFile.exists()) {
      logger.info("Log file does not exist");
    } else {
      try {
        FileUtils.forceDelete(logFile);
        logger.info("Log node {} cleaned old file", identifier);
      } catch (IOException e) {
        logger.warn("Old log file {} of {} cannot be deleted", logFile.getName(), identifier, e);
      }
    }
  }

  private void forceWal() {
    logger.warn("[wal] {} forceWal try to start", this.hashCode());
    lock.lock();
    logger.warn("[wal] {} forceWal start", this.hashCode());
    try {
      try {
        if (currentFileWriter != null) {
          currentFileWriter.force();
        }
      } catch (IOException e) {
        logger.warn("Log node {} force failed.", identifier, e);
      }
    } finally {
      lock.unlock();
      logger.warn("[wal] {} forceWal end", this.hashCode());
    }
  }

  private void sync() {
    logger.warn("[wal] {} sync try to start", this.hashCode());
    lock.lock();
    logger.warn("[wal] {} sync start", this.hashCode());
    try {
      if (bufferedLogNum == 0) {
        return;
      }
      switchBufferWorkingToFlushing();
      ILogWriter currWriter = getCurrentFileWriter();
      FLUSH_BUFFER_THREAD_POOL.submit(() -> flushBuffer(currWriter));
      switchBufferIdleToWorking();

      bufferedLogNum = 0;
      logger.debug("Log node {} ends sync.", identifier);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      logger.warn("Waiting for available buffer interrupted");
    } catch (FileNotFoundException e) {
      logger.warn("can not found file {}", identifier, e);
    } finally {
      lock.unlock();
      logger.warn("[wal] {} sync end", this.hashCode());
    }
  }

  private void flushBuffer(ILogWriter writer) {
    logger.warn("[wal] {} flushBuffer start", this.hashCode());
    long start = System.currentTimeMillis();
    try {
      writer.write(logBufferFlushing);
    } catch (ClosedChannelException e) {
      // ignore
    } catch (IOException e) {
      logger.warn("Log node {} sync failed, change system mode to read-only", identifier, e);
      IoTDBDescriptor.getInstance().getConfig().setReadOnly(true);
      return;
    } finally {
      long elapse = System.currentTimeMillis() - start;
      if (elapse > 5000) {
        logger.warn("[wal] flushBuffer cost: {}ms", elapse);
      }
    }
    logBufferFlushing.clear();

    try {
      switchBufferFlushingToIdle();
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
    logger.warn("[wal] {} flushBuffer end", this.hashCode());
  }

  private void switchBufferWorkingToFlushing() throws InterruptedException {
    logger.warn("[wal] {} switchBufferWorkingToFlushing start", this.hashCode());
    long start = System.currentTimeMillis();
    synchronized (switchBufferCondition) {
      while (logBufferFlushing != null && !deleted) {
        switchBufferCondition.wait(100);
      }
      logBufferFlushing = logBufferWorking;
      logBufferWorking = null;
      switchBufferCondition.notifyAll();
    }
    long elapse = System.currentTimeMillis() - start;
    if (elapse > 5000) {
      logger.warn("[wal] switch Working -> Flushing cost: {}ms", elapse);
    }
    logger.warn("[wal] {} switchBufferWorkingToFlushing end", this.hashCode());
  }

  private void switchBufferIdleToWorking() throws InterruptedException {
    logger.warn("[wal] {} switchBufferIdleToWorking start", this.hashCode());
    long start = System.currentTimeMillis();
    synchronized (switchBufferCondition) {
      while (logBufferIdle == null && !deleted) {
        switchBufferCondition.wait(100);
      }
      logBufferWorking = logBufferIdle;
      logBufferIdle = null;
      switchBufferCondition.notifyAll();
    }
    long elapse = System.currentTimeMillis() - start;
    if (elapse > 5000) {
      logger.warn("[wal] switch Idle -> Working cost: {}ms", elapse);
    }
    logger.warn("[wal] {} switchBufferIdleToWorking end", this.hashCode());
  }

  private void switchBufferFlushingToIdle() throws InterruptedException {
    logger.warn("[wal] {} switchBufferFlushingToIdle start", this.hashCode());
    long start = System.currentTimeMillis();
    synchronized (switchBufferCondition) {
      while (logBufferIdle != null && !deleted) {
        switchBufferCondition.wait(100);
      }
      logBufferIdle = logBufferFlushing;
      logBufferIdle.clear();
      logBufferFlushing = null;
      switchBufferCondition.notifyAll();
    }
    long elapse = System.currentTimeMillis() - start;
    if (elapse > 5000) {
      logger.warn("[wal] switch Flushing -> Idle cost: {}ms", elapse);
    }
    logger.warn("[wal] {} switchBufferFlushingToIdle end", this.hashCode());
  }

  private ILogWriter getCurrentFileWriter() throws FileNotFoundException {
    if (currentFileWriter == null) {
      nextFileWriter();
    }
    return currentFileWriter;
  }

  private void nextFileWriter() throws FileNotFoundException {
    fileId++;
    File newFile = SystemFileFactory.INSTANCE.getFile(logDirectory, WAL_FILE_NAME + fileId);
    if (newFile.getParentFile().mkdirs()) {
      logger.info("create WAL parent folder {}.", newFile.getParent());
    }
    logger.debug("WAL file {} is opened", newFile);
    currentFileWriter = new LogWriter(newFile, config.getForceWalPeriodInMs() == 0);
  }

  @Override
  public int hashCode() {
    return identifier.hashCode();
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj == null) {
      return false;
    }
    if (getClass() != obj.getClass()) {
      return false;
    }

    return compareTo((ExclusiveWriteLogNode) obj) == 0;
  }

  @Override
  public String toString() {
    return "Log node " + identifier;
  }

  @Override
  public int compareTo(ExclusiveWriteLogNode o) {
    return this.identifier.compareTo(o.identifier);
  }
}
