/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.iotdb.db.engine.archive;

import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBConstant;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.engine.fileSystem.SystemFileFactory;
import org.apache.iotdb.db.engine.storagegroup.TsFileResource;
import org.apache.iotdb.tsfile.utils.FilePathUtils;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * ArchiveRecover class encapsulates the recover logic, it retrieves the ArchiveTasks and finishes
 * archiving the tsfiles.
 */
public class ArchiveRecover {
  private static final Logger logger = LoggerFactory.getLogger(ArchiveRecover.class);
  private static final IoTDBConfig config = IoTDBDescriptor.getInstance().getConfig();

  private static final File LOG_FILE =
      SystemFileFactory.INSTANCE.getFile(
          Paths.get(
                  FilePathUtils.regularizePath(config.getSystemDir()),
                  IoTDBConstant.ARCHIVE_FOLDER_NAME,
                  "log.bin")
              .toString());
  private static final File ARCHIVING_LOG_DIR =
      SystemFileFactory.INSTANCE.getFile(
          Paths.get(
                  FilePathUtils.regularizePath(config.getSystemDir()),
                  IoTDBConstant.ARCHIVE_FOLDER_NAME,
                  IoTDBConstant.ARCHIVE_LOG_FOLDER_NAME)
              .toString());
  private Map<Long, ArchiveTask> archiveTasks;
  private long currentTaskId = 0;

  public List<ArchiveTask> recover() {
    // first recover archiving tsfiles
    recoverArchivingFiles();

    archiveTasks = new HashMap<>();
    currentTaskId = 0;

    // read from logReader
    try (ArchiveOperateReader logReader = new ArchiveOperateReader(LOG_FILE);
        ArchiveOperateWriter logWriter = new ArchiveOperateWriter(LOG_FILE)) {
      Set<Long> errorSet = new HashSet<>();

      while (logReader.hasNext()) {
        ArchiveOperate operate = logReader.next();
        long taskId = operate.getTask().getTaskId();

        switch (operate.getType()) {
          case SET:
            setArchiveFromLog(operate.getTask());
            break;
          case CANCEL:
            operateFromLog(ArchiveOperate.ArchiveOperateType.CANCEL, taskId);
            break;
          case START:
            // if task started but didn't finish, then error occurred
            errorSet.add(taskId);
            break;
          case PAUSE:
            errorSet.remove(taskId);
            operateFromLog(ArchiveOperate.ArchiveOperateType.PAUSE, taskId);
            break;
          case RESUME:
            operateFromLog(ArchiveOperate.ArchiveOperateType.RESUME, taskId);
            break;
          case FINISHED:
            // finished task => remove from list and remove from potential error task
            errorSet.remove(taskId);
            archiveTasks.remove(taskId);
            operateFromLog(ArchiveOperate.ArchiveOperateType.FINISHED, taskId);
            break;
          case ERROR:
            // already put error in log
            errorSet.remove(taskId);
            operateFromLog(ArchiveOperate.ArchiveOperateType.ERROR, taskId);
            break;
          default:
            logger.error("read archive log: unknown type");
        }
      }

      // for each task in errorSet, the task started but didn't finish (an error)
      for (long errTaskId : errorSet) {
        if (archiveTasks.containsKey(errTaskId)) {
          // write to log and set task in ERROR in memory
          logWriter.log(ArchiveOperate.ArchiveOperateType.ERROR, archiveTasks.get(errTaskId));
          operateFromLog(ArchiveOperate.ArchiveOperateType.ERROR, errTaskId);
        } else {
          logger.error("unknown error taskId");
        }
      }
    } catch (Exception e) {
      logger.error("Cannot read log for archive.");
    }

    recoverArchivingFiles();

    return new ArrayList<>(archiveTasks.values());
  }

  /** add archive task to archiveTasks from log, does not write to log */
  public void setArchiveFromLog(ArchiveTask newTask) {
    if (currentTaskId > newTask.getTaskId()) {
      logger.error("set archive error, current index larger than log index");
    }

    archiveTasks.put(newTask.getTaskId(), newTask);
    currentTaskId = newTask.getTaskId() + 1;
  }

  private ArchiveTask.ArchiveTaskStatus statusFromOperateType(
      ArchiveOperate.ArchiveOperateType archiveOperateType) {
    switch (archiveOperateType) {
      case RESUME:
        return ArchiveTask.ArchiveTaskStatus.READY;
      case CANCEL:
        return ArchiveTask.ArchiveTaskStatus.CANCELED;
      case START:
        return ArchiveTask.ArchiveTaskStatus.RUNNING;
      case PAUSE:
        return ArchiveTask.ArchiveTaskStatus.PAUSED;
      case FINISHED:
        return ArchiveTask.ArchiveTaskStatus.FINISHED;
      case ERROR:
        return ArchiveTask.ArchiveTaskStatus.ERROR;
    }
    return null;
  }

  /** operate Archive to archiveTasks from log, does not write to log */
  public boolean operateFromLog(ArchiveOperate.ArchiveOperateType operateType, long taskId) {
    if (archiveTasks.containsKey(taskId)) {
      archiveTasks.get(taskId).setStatus(statusFromOperateType(operateType));
      return true;
    } else {
      return false;
    }
  }

  /** finish the unfinished ArchiveTask using log files under ARCHIVING_LOG_DIR */
  public void recoverArchivingFiles() {
    File[] archivingLogFiles = ARCHIVING_LOG_DIR.listFiles();
    for (File logFile : archivingLogFiles) {
      FileInputStream logFileInput;
      File targetDir;
      String tsfilePath;
      File tsfile;

      try {
        logFileInput = new FileInputStream(logFile);
        String targetDirPath = ReadWriteIOUtils.readString(logFileInput);

        targetDir = SystemFileFactory.INSTANCE.getFile(targetDirPath);
        tsfilePath = ReadWriteIOUtils.readString(logFileInput);

        if (targetDir.exists()) {
          if (!targetDir.isDirectory()) {
            logger.error("target dir {} not a directory", targetDirPath);
            continue;
          }
        } else if (!targetDir.mkdirs()) {
          logger.error("create target dir {} failed", targetDirPath);
          continue;
        }
      } catch (IOException e) {
        // could not read log file, continue to next log
        logger.error("ArchiveRecover: log file not found");
        continue;
      }

      while (tsfilePath != null && !tsfilePath.isEmpty()) {
        tsfile = SystemFileFactory.INSTANCE.getFile(tsfilePath);

        TsFileResource resource = new TsFileResource(tsfile);
        resource.archive(targetDir);

        try {
          tsfilePath = ReadWriteIOUtils.readString(logFileInput);
        } catch (IOException e) {
          // finished reading all tsfile paths
          break;
        }
      }

      try {
        logFileInput.close();
      } catch (IOException e) {
        logger.error("Archiving Log File Error", e);
        break;
      }

      logFile.delete();
    }
  }

  public List<ArchiveTask> getArchiveTasks() {
    return new ArrayList<>(archiveTasks.values());
  }

  public long getCurrentTaskId() {
    return currentTaskId;
  }
}
