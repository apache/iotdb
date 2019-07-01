/**
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements.  See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership.  The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with the License.  You may obtain
 * a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied.  See the License for the specific language governing permissions and limitations
 * under the License.
 */
package org.apache.iotdb.db.writelog.node;

import java.io.File;
import java.io.IOException;
import java.nio.BufferOverflowException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Comparator;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import org.apache.commons.io.FileUtils;
import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.conf.directories.DirectoryManager;
import org.apache.iotdb.db.engine.StorageEngine;
import org.apache.iotdb.db.qp.physical.PhysicalPlan;
import org.apache.iotdb.db.writelog.io.ILogReader;
import org.apache.iotdb.db.writelog.io.ILogWriter;
import org.apache.iotdb.db.writelog.io.LogWriter;
import org.apache.iotdb.db.writelog.io.MultiFileLogReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This WriteLogNode is used to manage insert ahead logs of a TsFile.
 */
public class ExclusiveWriteLogNode implements WriteLogNode, Comparable<ExclusiveWriteLogNode> {

  public static final String WAL_FILE_NAME = "wal";
  private static final Logger LOGGER = LoggerFactory.getLogger(ExclusiveWriteLogNode.class);
  private static int logBufferSize = IoTDBDescriptor.getInstance().getConfig().getWalBufferSize();

  private String identifier;

  private String logDirectory;

  private ILogWriter currentFileWriter;

  private IoTDBConfig config = IoTDBDescriptor.getInstance().getConfig();

  private ByteBuffer logBuffer = ByteBuffer.allocate(logBufferSize);

  private ReadWriteLock lock = new ReentrantReadWriteLock();

  private long fileId = 0;
  private long lastFlushedId = 0;

  private int bufferedLogNum = 0;

  /**
   * constructor of ExclusiveWriteLogNode.
   *
   * @param identifier ExclusiveWriteLogNode identifier
   */
  public ExclusiveWriteLogNode(String identifier) {
    this.identifier = identifier;
    this.logDirectory =
        DirectoryManager.getInstance().getWALFolder() + File.separator + this.identifier;
    new File(logDirectory).mkdirs();
  }

  @Override
  public void write(PhysicalPlan plan) throws IOException {
    long lockStartTime = System.currentTimeMillis();
    lock.writeLock().lock();
    long lockElapsed = System.currentTimeMillis() - lockStartTime;
    if (lockElapsed > 1000) {
      LOGGER.info("WAL waiting for lock costs {} ms", lockElapsed);
    }
    try {
      long start = System.currentTimeMillis();

      putLog(plan);

      if (bufferedLogNum >= config.getFlushWalThreshold()) {
        sync();
      }
      long elapse = System.currentTimeMillis() - start;
      if (elapse > 1000) {
        LOGGER.info("WAL insert cost {} ms", elapse);
      }
    } catch (BufferOverflowException e) {
      throw new IOException("Log cannot fit into buffer", e);
    } finally {
      lock.writeLock().unlock();
    }
  }

  private void putLog(PhysicalPlan plan) {
    logBuffer.mark();
    try {
      plan.serializeTo(logBuffer);
    } catch (BufferOverflowException e) {
      LOGGER.info("WAL BufferOverflow !");
      logBuffer.reset();
      sync();
      plan.serializeTo(logBuffer);
    }
    bufferedLogNum ++;
  }

  @Override
  public void close() {
    sync();
    forceWal();
    long lockStartTime = System.currentTimeMillis();
    lock.writeLock().lock();
    long lockElapsed = System.currentTimeMillis() - lockStartTime;
    if (lockElapsed > 1000) {
      LOGGER.info("WAL waiting for lock costs {} ms", lockElapsed);
    }
    long start = System.currentTimeMillis();
    try {
      if (this.currentFileWriter != null) {
        this.currentFileWriter.close();
        this.currentFileWriter = null;
      }
      LOGGER.debug("Log node {} closed successfully", identifier);
    } catch (IOException e) {
      LOGGER.error("Cannot close log node {} because:", identifier, e);
    } finally {
      lock.writeLock().unlock();
    }
    long elapse = System.currentTimeMillis() - start;
    if (elapse > 1000) {
      LOGGER.info("WAL log node {} close cost {} ms", identifier, elapse);
    }
  }

  @Override
  public void forceSync() {
    sync();
    forceWal();
  }


  @Override
  public void notifyStartFlush() {
    long lockStartTime = System.currentTimeMillis();
    lock.writeLock().lock();
    long lockElapsed = System.currentTimeMillis() - lockStartTime;
    if (lockElapsed > 1000) {
      LOGGER.info("WAL waiting for lock costs {} ms", lockElapsed);
    }
    try {
      long start = System.currentTimeMillis();
      close();
      nextFileWriter();
      long elapse = System.currentTimeMillis() - start;
      if (elapse > 1000) {
        LOGGER.info("WAL notifyStartFlush cost {} ms", elapse);
      }
    } finally {
      lock.writeLock().unlock();
    }
  }

  @Override
  public void notifyEndFlush() {
    long lockStartTime = System.currentTimeMillis();
    lock.writeLock().lock();
    long lockElapsed = System.currentTimeMillis() - lockStartTime;
    if (lockElapsed > 1000) {
      LOGGER.info("WAL waiting for lock costs {} ms", lockElapsed);
    }
    try {
      long start = System.currentTimeMillis();
      File logFile = new File(logDirectory, WAL_FILE_NAME + ++lastFlushedId);
      discard(logFile);
      long elapse = System.currentTimeMillis() - start;
      if (elapse > 1000) {
        LOGGER.info("WAL notifyEndFlush cost {} ms", elapse);
      }
    } finally {
      lock.writeLock().unlock();
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
  public void delete() throws IOException {
    long lockStartTime = System.currentTimeMillis();
    lock.writeLock().lock();
    long lockElapsed = System.currentTimeMillis() - lockStartTime;
    if (lockElapsed > 1000) {
      LOGGER.info("WAL waiting for lock costs {} ms", lockElapsed);
    }
    try {
      long start = System.currentTimeMillis();
      logBuffer.clear();
      close();
      FileUtils.deleteDirectory(new File(logDirectory));
      long elapse = System.currentTimeMillis() - start;
      if (elapse > 1000) {
        LOGGER.info("WAL delete cost {} ms", elapse);
      }
    } finally {
      lock.writeLock().unlock();
    }
  }

  @Override
  public ILogReader getLogReader() {
    File[] logFiles = new File(logDirectory).listFiles();
    Arrays.sort(logFiles,
        Comparator.comparingInt(f -> Integer.parseInt(f.getName().replace(WAL_FILE_NAME, ""))));
    return new MultiFileLogReader(logFiles);
  }

  private void discard(File logFile) {
    if (!logFile.exists()) {
      LOGGER.info("Log file does not exist");
    } else {
      try {
        FileUtils.forceDelete(logFile);
        LOGGER.info("Log node {} cleaned old file", identifier);
      } catch (IOException e) {
        LOGGER.error("Old log file {} of {} cannot be deleted", logFile.getName(), identifier, e);
      }
    }
  }

  private void forceWal() {
    long lockStartTime = System.currentTimeMillis();
    lock.writeLock().lock();
    long lockElapsed = System.currentTimeMillis() - lockStartTime;
    if (lockElapsed > 1000) {
      LOGGER.info("WAL waiting for lock costs {} ms", lockElapsed);
    }
    try {
      long start = System.currentTimeMillis();
      try {
        if (currentFileWriter != null) {
          currentFileWriter.force();
        }
      } catch (IOException e) {
        LOGGER.error("Log node {} force failed.", identifier, e);
      }
      long elapse = System.currentTimeMillis() - start;
      if (elapse > 1000) {
        LOGGER.info("WAL forceWal cost {} ms", elapse);
      }
    } finally {
      lock.writeLock().unlock();
    }
  }

  private void sync() {
    lock.writeLock().lock();
    try {
      long start = System.currentTimeMillis();
      if (bufferedLogNum == 0) {
        return;
      }
      try {
        getCurrentFileWriter().write(logBuffer);
      } catch (IOException e) {
        StorageEngine.getInstance().setReadOnly(true);
        LOGGER.error("Log node {} sync failed", identifier, e);
        return;
      }
      logBuffer.clear();
      bufferedLogNum = 0;
      LOGGER.debug("Log node {} ends sync.", identifier);
      long elapse = System.currentTimeMillis() - start;
      if (elapse > 1000) {
        LOGGER.info("WAL sync cost {} ms", elapse);
      }
    } finally {
      lock.writeLock().unlock();
    }
  }

  private ILogWriter getCurrentFileWriter() {
    if (currentFileWriter == null) {
      nextFileWriter();
    }
    return currentFileWriter;
  }

  private void nextFileWriter() {
    fileId++;
    File newFile = new File(logDirectory, WAL_FILE_NAME + fileId);
    newFile.getParentFile().mkdirs();
    currentFileWriter = new LogWriter(newFile);
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
