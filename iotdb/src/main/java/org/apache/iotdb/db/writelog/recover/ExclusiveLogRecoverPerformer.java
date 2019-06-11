/**
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
package org.apache.iotdb.db.writelog.recover;

import static org.apache.iotdb.db.writelog.RecoverStage.BACK_UP;
import static org.apache.iotdb.db.writelog.RecoverStage.CLEAN_UP;
import static org.apache.iotdb.db.writelog.RecoverStage.INIT;
import static org.apache.iotdb.db.writelog.RecoverStage.RECOVER_FILE;
import static org.apache.iotdb.db.writelog.RecoverStage.REPLAY_LOG;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.commons.io.FileUtils;
import org.apache.iotdb.db.conf.IoTDBConstant;
import org.apache.iotdb.db.engine.filenode.FileNodeManager;
import org.apache.iotdb.db.exception.FileNodeManagerException;
import org.apache.iotdb.db.exception.ProcessorException;
import org.apache.iotdb.db.exception.RecoverException;
import org.apache.iotdb.db.qp.physical.PhysicalPlan;
import org.apache.iotdb.db.writelog.RecoverStage;
import org.apache.iotdb.db.writelog.io.RAFLogReader;
import org.apache.iotdb.db.writelog.node.ExclusiveWriteLogNode;
import org.apache.iotdb.db.writelog.replay.ConcreteLogReplayer;
import org.apache.iotdb.db.writelog.replay.LogReplayer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ExclusiveLogRecoverPerformer implements RecoverPerformer {

  public static final String RECOVER_FLAG_NAME = "recover-flag";
  public static final String RECOVER_SUFFIX = "-recover";
  public static final String FLAG_SEPERATOR = "-";
  private static final Logger logger = LoggerFactory.getLogger(ExclusiveLogRecoverPerformer.class);
  // The two fields can be made static only because the recovery is a serial process.
  private static RAFLogReader rafLogReader = new RAFLogReader();
  private ExclusiveWriteLogNode writeLogNode;
  private String recoveryFlagPath;
  private String restoreFilePath;
  private String processorStoreFilePath;
  private RecoverStage currStage;
  private LogReplayer replayer = new ConcreteLogReplayer();
  private RecoverPerformer fileNodeRecoverPerformer;
  // recovery of Overflow maybe different from BufferWrite
  private boolean isOverflow;

  /**
   * constructor of ExclusiveLogRecoverPerformer.
   */
  public ExclusiveLogRecoverPerformer(String restoreFilePath, String processorStoreFilePath,
      ExclusiveWriteLogNode logNode) {
    this.restoreFilePath = restoreFilePath;
    this.processorStoreFilePath = processorStoreFilePath;
    this.writeLogNode = logNode;
    this.fileNodeRecoverPerformer = new StorageGroupRecoverPerformer(writeLogNode.getIdentifier());
    this.isOverflow = logNode.getFileNodeName().contains(IoTDBConstant.OVERFLOW_LOG_NODE_SUFFIX);
  }

  public void setFileNodeRecoverPerformer(RecoverPerformer fileNodeRecoverPerformer) {
    this.fileNodeRecoverPerformer = fileNodeRecoverPerformer;
  }

  public void setReplayer(LogReplayer replayer) {
    this.replayer = replayer;
  }

  @Override
  public void recover() throws RecoverException {
    currStage = determineStage();
    if (currStage != null) {
      recoverAtStage(currStage);
    }
  }

  private RecoverStage determineStage() throws RecoverException {
    File logDir = new File(writeLogNode.getLogDirectory());
    if (!logDir.exists()) {
      logger.error("Log node {} directory does not exist, recover failed",
          writeLogNode.getLogDirectory());
      throw new RecoverException("No directory for log node " + writeLogNode.getIdentifier());
    }
    // search for the flag file
    File[] files = logDir.listFiles((dir, name) -> name.contains(RECOVER_FLAG_NAME));

    if (files == null || files.length == 0) {
      File[] logFiles = logDir
          .listFiles((dir, name) -> name.contains(ExclusiveWriteLogNode.WAL_FILE_NAME));
      // no flag is set, and there exists log file, start from beginning.
      if (logFiles != null && logFiles.length > 0) {
        return RecoverStage.BACK_UP;
      } else {
        // no flag is set, and there is no log file, do not recover.
        return null;
      }
    }

    File flagFile = files[0];
    String flagName = flagFile.getName();
    recoveryFlagPath = flagFile.getPath();
    // the flag name is like "recover-flag-{flagType}"
    String[] parts = flagName.split(FLAG_SEPERATOR);
    if (parts.length != 3) {
      logger.error("Log node {} invalid recover flag name {}", writeLogNode.getIdentifier(),
          flagName);
      throw new RecoverException("Illegal recover flag " + flagName);
    }
    String stageName = parts[2];
    // if a flag of stage X is found, that means X had finished, so start from next stage
    if (stageName.equals(BACK_UP.name())) {
      return RECOVER_FILE;
    } else if (stageName.equals(REPLAY_LOG.name())) {
      return CLEAN_UP;
    } else {
      logger.error("Log node {} invalid recover flag name {}", writeLogNode.getIdentifier(),
          flagName);
      throw new RecoverException("Illegal recover flag " + flagName);
    }
  }

  private void recoverAtStage(RecoverStage stage) throws RecoverException {
    switch (stage) {
      case INIT:
      case BACK_UP:
        backup();
        break;
      case RECOVER_FILE:
        recoverFile();
        break;
      case REPLAY_LOG:
        replayLog();
        break;
      case CLEAN_UP:
        cleanup();
        break;
      default:
        logger.error("Invalid stage {}", stage);
    }
  }

  private void setFlag(RecoverStage stage) {
    if (recoveryFlagPath == null) {
      recoveryFlagPath =
          writeLogNode.getLogDirectory() + File.separator + RECOVER_FLAG_NAME + FLAG_SEPERATOR
              + stage.name();
      try {
        File flagFile = new File(recoveryFlagPath);
        if (!flagFile.createNewFile()) {
          logger
              .error("Log node {} cannot set flag at stage {}", writeLogNode.getLogDirectory(),
                  stage.name());
        }
      } catch (IOException e) {
        logger.error("Log node {} cannot set flag at stage {}", writeLogNode.getLogDirectory(),
            stage.name(), e);
      }
    } else {
      File flagFile = new File(recoveryFlagPath);
      recoveryFlagPath = recoveryFlagPath.replace(FLAG_SEPERATOR + currStage.name(),
          FLAG_SEPERATOR + stage.name());
      if (!flagFile.renameTo(new File(recoveryFlagPath))) {
        logger
            .error("Log node {} cannot update flag at stage {}", writeLogNode.getLogDirectory(),
                stage.name());
      }
    }
  }

  private void cleanFlag() throws RecoverException {
    if (recoveryFlagPath != null) {
      File flagFile = new File(recoveryFlagPath);
      if (!flagFile.delete()) {
        logger.error("Log node {} cannot clean flag ", writeLogNode.getLogDirectory());
        throw new RecoverException("Cannot clean flag");
      }
    }
  }

  private void backup() throws RecoverException {
    String recoverRestoreFilePath = restoreFilePath + RECOVER_SUFFIX;
    File recoverRestoreFile = new File(recoverRestoreFilePath);
    File restoreFile = new File(restoreFilePath);
    if (!recoverRestoreFile.exists() && restoreFile.exists()) {
      try {
        FileUtils.copyFile(restoreFile, recoverRestoreFile);
      } catch (Exception e) {
        logger.error("Log node {} cannot backup restore file",
            writeLogNode.getLogDirectory(), e);
        throw new RecoverException("Cannot backup restore file, recovery aborted.");
      }
    }

    String recoverProcessorStoreFilePath = processorStoreFilePath + RECOVER_SUFFIX;
    File recoverProcessorStoreFile = new File(recoverProcessorStoreFilePath);
    File processorStoreFile = new File(processorStoreFilePath);
    if (!recoverProcessorStoreFile.exists() && processorStoreFile.exists()) {
      try {
        FileUtils.copyFile(processorStoreFile, recoverProcessorStoreFile);
      } catch (Exception e) {
        logger.error("Log node {} cannot backup processor file",
            writeLogNode.getLogDirectory(), e);
        throw new RecoverException("Cannot backup processor file, recovery aborted.");
      }
    }

    setFlag(BACK_UP);
    currStage = RECOVER_FILE;
    logger.info("Log node {} backup ended", writeLogNode.getLogDirectory());
    recoverFile();
  }

  private void recoverFile() throws RecoverException {
    String recoverRestoreFilePath = restoreFilePath + RECOVER_SUFFIX;
    File recoverRestoreFile = new File(recoverRestoreFilePath);
    try {
      if (recoverRestoreFile.exists()) {
        FileUtils.copyFile(recoverRestoreFile, new File(restoreFilePath));
      }
    } catch (Exception e) {
      logger.error("Log node {} cannot recover restore file.",
          writeLogNode.getLogDirectory(), e);
      throw new RecoverException("Cannot recover restore file, recovery aborted.");
    }

    String recoverProcessorStoreFilePath = processorStoreFilePath + RECOVER_SUFFIX;
    File recoverProcessorStoreFile = new File(recoverProcessorStoreFilePath);
    try {
      if (recoverProcessorStoreFile.exists()) {
        FileUtils.copyFile(recoverProcessorStoreFile, new File(processorStoreFilePath));
      }
    } catch (Exception e) {
      throw new RecoverException(String.format("Log node %s cannot recover processor file,"
          + " recovery aborted.", writeLogNode.getLogDirectory()), e);
    }

    fileNodeRecoverPerformer.recover();

    currStage = REPLAY_LOG;
    logger.info("Log node {} recover files ended", writeLogNode.getLogDirectory());
    replayLog();
  }

  private int replayLogFile(File logFile) throws RecoverException, IOException {
    int failedCnt = 0;
    if (logFile.exists()) {
      try {
        rafLogReader.open(logFile);
      } catch (FileNotFoundException e) {
        logger
            .error("Log node {} cannot read old log file, because ", writeLogNode.getIdentifier(),
                e);
        throw new RecoverException("Cannot read old log file, recovery aborted.");
      }
      while (rafLogReader.hasNext()) {
        try {
          PhysicalPlan physicalPlan = rafLogReader.next();
          if (physicalPlan == null) {
            logger.error("Log node {} read a bad log", writeLogNode.getIdentifier());
            throw new RecoverException("Cannot read old log file, recovery aborted.");
          }
          replayer.replay(physicalPlan, isOverflow);
        } catch (ProcessorException e) {
          failedCnt++;
          logger.error("Log node {}", writeLogNode.getLogDirectory(), e);
        }
      }
      rafLogReader.close();
    }
    return failedCnt;
  }

  private void  replayLog() throws RecoverException {
    int failedEntryCnt = 0;
    // if old log file exists, replay it first.
    File logFolder = new File(writeLogNode.getLogDirectory());
    for (File file : logFolder.listFiles( name -> name.getName().startsWith(ExclusiveWriteLogNode.WAL_FILE_NAME
        + ExclusiveWriteLogNode.OLD_SUFFIX))) {
      try {
        failedEntryCnt += replayLogFile(file);
      } catch (IOException e) {
        throw new RecoverException(e);
      }
    }

    // then replay new log
    File newLogFile = new File(
        writeLogNode.getLogDirectory() + File.separator + ExclusiveWriteLogNode.WAL_FILE_NAME);
    try {
      failedEntryCnt += replayLogFile(newLogFile);
    } catch (IOException e) {
      throw new RecoverException(e);
    }
    // TODO : do we need to proceed if there are failed logs ?
    if (failedEntryCnt > 0) {
      throw new RecoverException(
          "There are " + failedEntryCnt
              + " logs failed to recover, see logs above for details");
    }
    try {
      FileNodeManager.getInstance().closeOneFileNode(writeLogNode.getFileNodeName());
    } catch (FileNodeManagerException e) {
      throw new RecoverException(String.format("Log node %s cannot perform flush"
              + " after replaying logs!", writeLogNode.getIdentifier()), e);
    }
    currStage = CLEAN_UP;
    setFlag(REPLAY_LOG);
    logger.info("Log node {} replay ended.", writeLogNode.getLogDirectory());
    cleanup();
  }

  private void cleanup() throws RecoverException {
    // clean recovery files
    List<String> failedFiles = new ArrayList<>();
    String recoverRestoreFilePath = restoreFilePath + RECOVER_SUFFIX;
    File recoverRestoreFile = new File(recoverRestoreFilePath);
    if (recoverRestoreFile.exists() && !recoverRestoreFile.delete()) {
        logger
            .error("Log node {} cannot delete backup restore file", writeLogNode.getLogDirectory());
        failedFiles.add(recoverRestoreFilePath);
    }
    String recoverProcessorStoreFilePath = processorStoreFilePath + RECOVER_SUFFIX;
    File recoverProcessorStoreFile = new File(recoverProcessorStoreFilePath);
    if (recoverProcessorStoreFile.exists() && !recoverProcessorStoreFile.delete()) {
        logger.error("Log node {} cannot delete backup processor store file",
            writeLogNode.getLogDirectory());
        failedFiles.add(recoverProcessorStoreFilePath);
    }
    // clean log file
    File logFolder = new File(writeLogNode.getLogDirectory());
    for (File file : logFolder.listFiles( name -> name.getName().startsWith(ExclusiveWriteLogNode.WAL_FILE_NAME
        + ExclusiveWriteLogNode.OLD_SUFFIX))) {
      if (file.exists() && !file.delete()) {
        logger.error("Log node {} cannot delete old log file", writeLogNode.getLogDirectory());
        failedFiles.add(file.getPath());
      }
    }

    File newLogFile = new File(
        writeLogNode.getLogDirectory() + File.separator + ExclusiveWriteLogNode.WAL_FILE_NAME);
    if (newLogFile.exists() && !newLogFile.delete()) {
        logger.error("Log node {} cannot delete new log file", writeLogNode.getLogDirectory());
        failedFiles.add(newLogFile.getPath());
    }
    if (!failedFiles.isEmpty()) {
      throw new RecoverException(
          "File clean failed. Failed files are " + failedFiles.toString());
    }
    // clean flag
    currStage = INIT;
    cleanFlag();
    logger.info("Log node {} cleanup ended.", writeLogNode.getLogDirectory());
  }
}
