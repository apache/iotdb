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
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import org.apache.commons.io.FileUtils;
import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.conf.directories.Directories;
import org.apache.iotdb.db.exception.RecoverException;
import org.apache.iotdb.db.qp.physical.PhysicalPlan;
import org.apache.iotdb.db.qp.physical.transfer.PhysicalPlanLogTransfer;
import org.apache.iotdb.db.writelog.io.ILogWriter;
import org.apache.iotdb.db.writelog.io.LogWriter;
import org.apache.iotdb.db.writelog.recover.RecoverPerformer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This WriteLogNode is used to manage write ahead logs of BufferWrite or Overflow of a single
 * StorageGroup.
 */
public class ExclusiveWriteLogNode implements WriteLogNode, Comparable<ExclusiveWriteLogNode> {

  public static final String WAL_FILE_NAME = "wal";
  public static final String OLD_SUFFIX = "-old";
  private static final Logger logger = LoggerFactory.getLogger(ExclusiveWriteLogNode.class);
  /**
   * This should be the same as the corresponding StorageGroup's name.
   */
  private String identifier;

  private String logDirectory;

  private ILogWriter currentFileWriter;

  private RecoverPerformer recoverPerformer;

  private IoTDBConfig config = IoTDBDescriptor.getInstance().getConfig();

  private List<byte[]> logCache = new ArrayList<>(config.getFlushWalThreshold());

  private ReadWriteLock lock = new ReentrantReadWriteLock();

  private ReadWriteLock forceLock = new ReentrantReadWriteLock();

  private long fileId;

  /**
   * constructor of ExclusiveWriteLogNode.
   *
   * @param identifier             ExclusiveWriteLogNode identifier
   */
  public ExclusiveWriteLogNode(String identifier) {
    this.identifier = identifier;
    this.logDirectory =
        Directories.getInstance().getWALFolder() + File.separator + this.identifier;
    new File(logDirectory).mkdirs();

  }

  public void setRecoverPerformer(RecoverPerformer recoverPerformer) {
    this.recoverPerformer = recoverPerformer;
  }

  /*
   * Return value is of no use in this implementation.
   */
  @Override
  public void write(PhysicalPlan plan) throws IOException {
    lockForWrite();
    try {
      long start = System.currentTimeMillis();
      byte[] logBytes = PhysicalPlanLogTransfer.planToLog(plan);
      logCache.add(logBytes);

      if (logCache.size() >= config.getFlushWalThreshold()) {
        sync();
      }
      long elapse = System.currentTimeMillis() - start;
      if( elapse > 1000){
        logger.info("WAL write cost {} ms", elapse);
      }
    } finally {
      unlockForWrite();
    }
  }

  @Override
  public void recover() throws RecoverException {
    recoverPerformer.recover();
  }

  @Override
  public void close() {
    sync();
    forceWal();
    lockForOther();
    lockForForceOther();
    long start = System.currentTimeMillis();
    try {
      if (this.currentFileWriter != null) {
        this.currentFileWriter.close();
        this.currentFileWriter = null;
      }
      logger.debug("Log node {} closed successfully", identifier);
    } catch (IOException e) {
      logger.error("Cannot close log node {} because:", identifier, e);
    }
    long elapse = System.currentTimeMillis() - start;
    if (elapse > 1000) {
      logger.info("WAL close cost {} ms", elapse);
    }
    unlockForForceOther();
    unlockForOther();
  }

  @Override
  public void forceSync() {
    sync();
    forceWal();
  }


  @Override
  public long notifyStartFlush() {
    lockForWrite();
    try {
      long start = System.currentTimeMillis();
      close();
      nextFileWriter();
      long elapse = System.currentTimeMillis() - start;
      if( elapse > 1000){
        logger.info("WAL notifyStartFlush cost {} ms", elapse);
      }
      return fileId;
    } finally {
      unlockForWrite();
    }
  }

  @Override
  public void notifyEndFlush(long fileId) {
    lockForWrite();
    try {
      long start = System.currentTimeMillis();
      File logFile = new File(logDirectory, WAL_FILE_NAME + fileId);
      discard(logFile);
      long elapse = System.currentTimeMillis() - start;
      if( elapse > 1000){
        logger.info("WAL notifyEndFlush cost {} ms", elapse);
      }
    } finally {
      unlockForWrite();
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
    lockForOther();
    try {
      long start = System.currentTimeMillis();
      logCache.clear();
      if (currentFileWriter != null) {
        currentFileWriter.close();
      }
      FileUtils.deleteDirectory(new File(logDirectory));
      long elapse = System.currentTimeMillis() - start;
      if (elapse > 1000) {
        logger.info("WAL delete cost {} ms", elapse);
      }
    } finally {
      unlockForOther();
    }
  }

  private void lockForWrite() {
    lock.writeLock().lock();
  }

  // other means sync and delete
  private void lockForOther() {
    lock.writeLock().lock();
  }

  private void unlockForWrite() {
    lock.writeLock().unlock();
  }

  private void unlockForOther() {
    lock.writeLock().unlock();
  }

  private void lockForForceOther() {
    forceLock.writeLock().lock();
  }

  private void unlockForForceOther() {
    forceLock.writeLock().unlock();
  }

  private void sync() {
    lockForOther();
    try {
      long start = System.currentTimeMillis();
      logger.debug("Log node {} starts sync, {} logs to be synced", identifier, logCache.size());
      if (logCache.isEmpty()) {
        return;
      }
      try {
        getCurrentFileWriter().write(logCache);
      } catch (IOException e) {
        logger.error("Log node {} sync failed", identifier, e);
      }
      logCache.clear();
      logger.debug("Log node {} ends sync.", identifier);
      long elapse = System.currentTimeMillis() - start;
      if (elapse > 1000) {
        logger.info("WAL sync cost {} ms", elapse);
      }
    } finally {
      unlockForOther();
    }
  }

  private void forceWal() {
    lockForForceOther();
    try {
      long start = System.currentTimeMillis();
      logger.debug("Log node {} starts force, {} logs to be forced", identifier, logCache.size());
      try {
        if (currentFileWriter != null) {
          currentFileWriter.force();
        }
      } catch (IOException e) {
        logger.error("Log node {} force failed.", identifier, e);
      }
      logger.debug("Log node {} ends force.", identifier);
      long elapse = System.currentTimeMillis() - start;
      if (elapse > 1000) {
        logger.info("WAL forceWal cost {} ms", elapse);
      }
    } finally {
      unlockForForceOther();
    }
  }

  private void discard(File logFile) {
    if (!logFile.exists()) {
      logger.info("Log file does not exist");
    } else {
      if (!logFile.delete()) {
        logger.error("Old log file of {} cannot be deleted", identifier);
      } else {
        logger.info("Log node {} cleaned old file", identifier);
      }
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
    currentFileWriter = new LogWriter(newFile);
  }


  @Override
  public String toString() {
    return "Log node " + identifier;
  }

  public String getFileNodeName() {
    return identifier.split("-")[0];
  }

  @Override
  public int compareTo(ExclusiveWriteLogNode o) {
    return this.identifier.compareTo(o.identifier);
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
}
