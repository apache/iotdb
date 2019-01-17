/**
 * Copyright © 2019 Apache IoTDB(incubating) (dev@iotdb.apache.org)
 *
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
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
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
import org.apache.iotdb.db.exception.RecoverException;
import org.apache.iotdb.db.qp.physical.PhysicalPlan;
import org.apache.iotdb.db.utils.MemUtils;
import org.apache.iotdb.db.writelog.LogPosition;
import org.apache.iotdb.db.writelog.io.ILogWriter;
import org.apache.iotdb.db.writelog.io.LogWriter;
import org.apache.iotdb.db.writelog.recover.ExclusiveLogRecoverPerformer;
import org.apache.iotdb.db.writelog.recover.RecoverPerformer;
import org.apache.iotdb.db.writelog.transfer.PhysicalPlanLogTransfer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This WriteLogNode is used to manage write ahead logs of a single FileNode.
 */
public class ExclusiveWriteLogNode implements WriteLogNode, Comparable<ExclusiveWriteLogNode> {

  public static final String WAL_FILE_NAME = "wal";
  public static final String OLD_SUFFIX = "-old";
  private static final Logger logger = LoggerFactory.getLogger(ExclusiveWriteLogNode.class);
  /**
   * This should be the same as the corresponding FileNode's name.
   */
  private String identifier;

  private String logDirectory;

  private ILogWriter currentFileWriter;

  private RecoverPerformer recoverPerformer;

  private IoTDBConfig config = IoTDBDescriptor.getInstance().getConfig();

  private List<byte[]> logCache = new ArrayList<>(config.flushWalThreshold);

  private ReadWriteLock lock = new ReentrantReadWriteLock();

  /**
   * constructor of ExclusiveWriteLogNode.
   *
   * @param identifier ExclusiveWriteLogNode identifier
   * @param restoreFilePath restore file path
   * @param processorStoreFilePath processor store file path
   */
  public ExclusiveWriteLogNode(String identifier, String restoreFilePath,
      String processorStoreFilePath) {
    this.identifier = identifier;
    this.logDirectory = config.walFolder + File.separator + this.identifier;
    new File(logDirectory).mkdirs();

    recoverPerformer = new ExclusiveLogRecoverPerformer(restoreFilePath, processorStoreFilePath,
        this);
    currentFileWriter = new LogWriter(logDirectory + File.separator + WAL_FILE_NAME);
  }

  public void setRecoverPerformer(RecoverPerformer recoverPerformer) {
    this.recoverPerformer = recoverPerformer;
  }

  /*
   * Return value is of no use in this implementation.
   */
  @Override
  public LogPosition write(PhysicalPlan plan) throws IOException {
    lockForWrite();
    try {
      byte[] logBytes = PhysicalPlanLogTransfer.operatorToLog(plan);
      logCache.add(logBytes);

      if (logCache.size() >= config.flushWalThreshold) {
        sync();
      }
    } finally {
      unlockForWrite();
    }
    return null;
  }

  @Override
  public void recover() throws RecoverException {
    try {
      close();
    } catch (IOException e) {
      logger.error("Cannot close write log {} node before recover! Because {}", identifier,
          e.getMessage());
      throw new RecoverException(
          String.format("Cannot close write log %s node before recover!", identifier));
    }
    recoverPerformer.recover();
  }

  @Override
  public void close() throws IOException {
    sync();
    lockForOther();
    try {
      this.currentFileWriter.close();
      logger.debug("Log node {} closed successfully", identifier);
    } catch (IOException e) {
      logger.error("Cannot close log node {} because {}", identifier, e.getMessage());
    }
    unlockForOther();
  }

  @Override
  public void forceSync() throws IOException {
    sync();
  }

  /*
   * Warning : caller must have lock.
   */
  @Override
  public void notifyStartFlush() throws IOException {
    close();
    File oldLogFile = new File(logDirectory + File.separator + WAL_FILE_NAME);
    File newLogFile = new File(logDirectory + File.separator + WAL_FILE_NAME + OLD_SUFFIX);
    if (!oldLogFile.exists()) {
      return;
    }
    if (!oldLogFile.renameTo(newLogFile)) {
      logger.error("Log node {} renaming log file failed!", identifier);
    } else {
      logger.info("Log node {} renamed log file, file size is {}", identifier,
          MemUtils.bytesCntToStr(newLogFile.length()));
    }
  }

  /*
   * Warning : caller must have lock.
   */
  @Override
  public void notifyEndFlush(List<LogPosition> logPositions) {
    discard();
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
      logCache.clear();
      if (currentFileWriter != null) {
        currentFileWriter.close();
      }
      FileUtils.deleteDirectory(new File(logDirectory));
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

  private void sync() {
    lockForOther();
    try {
      logger.debug("Log node {} starts sync, {} logs to be synced", identifier, logCache.size());
      if (logCache.size() == 0) {
        return;
      }
      try {
        currentFileWriter.write(logCache);
      } catch (IOException e) {
        logger.error("Log node {} sync failed because {}.", identifier, e.getMessage());
      }
      logCache.clear();
      logger.debug("Log node {} ends sync.", identifier);
    } finally {
      unlockForOther();
    }
  }

  private void discard() {
    File oldLogFile = new File(logDirectory + File.separator + WAL_FILE_NAME + OLD_SUFFIX);
    if (!oldLogFile.exists()) {
      logger.info("No old log to be deleted");
    } else {
      if (!oldLogFile.delete()) {
        logger.error("Old log file of {} cannot be deleted", identifier);
      } else {
        logger.info("Log node {} cleaned old file", identifier);
      }
    }
  }

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
}
