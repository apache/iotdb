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
package org.apache.iotdb.db.engine.archiving;

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
 * ArchivingRecover class encapsulates the recover logic, it retrieves the ArchivingTasks and
 * finishes archiving the tsfiles.
 */
public class ArchivingRecover {
  private static final Logger logger = LoggerFactory.getLogger(ArchivingRecover.class);
  private static final IoTDBConfig config = IoTDBDescriptor.getInstance().getConfig();

  private static final File LOG_FILE =
      SystemFileFactory.INSTANCE.getFile(
          Paths.get(
                  FilePathUtils.regularizePath(config.getSystemDir()),
                  IoTDBConstant.ARCHIVING_FOLDER_NAME,
                  "log.bin")
              .toString());
  private static final File ARCHIVING_LOG_DIR =
      SystemFileFactory.INSTANCE.getFile(
          Paths.get(
                  FilePathUtils.regularizePath(config.getSystemDir()),
                  IoTDBConstant.ARCHIVING_FOLDER_NAME,
                  IoTDBConstant.ARCHIVING_LOG_FOLDER_NAME)
              .toString());
  private Map<Long, ArchivingTask> archivingTasks;
  private long currentTaskId = 0;

  public List<ArchivingTask> recover() {
    // first recover archiving tsfiles
    recoverArchivingFiles();

    archivingTasks = new HashMap<>();
    currentTaskId = 0;

    // read from logReader
    try (ArchivingOperateReader logReader = new ArchivingOperateReader(LOG_FILE);
        ArchivingOperateWriter logWriter = new ArchivingOperateWriter(LOG_FILE)) {
      Set<Long> errorSet = new HashSet<>();

      while (logReader.hasNext()) {
        ArchivingOperate operate = logReader.next();
        long taskId = operate.getTask().getTaskId();

        switch (operate.getType()) {
          case SET:
            setArchivingFromLog(operate.getTask());
            break;
          case CANCEL:
            operateFromLog(ArchivingOperate.ArchivingOperateType.CANCEL, taskId);
            break;
          case START:
            // if task started but didn't finish, then error occurred
            errorSet.add(taskId);
            break;
          case PAUSE:
            errorSet.remove(taskId);
            operateFromLog(ArchivingOperate.ArchivingOperateType.PAUSE, taskId);
            break;
          case RESUME:
            operateFromLog(ArchivingOperate.ArchivingOperateType.RESUME, taskId);
            break;
          case FINISHED:
            // finished task => remove from list and remove from potential error task
            errorSet.remove(taskId);
            archivingTasks.remove(taskId);
            operateFromLog(ArchivingOperate.ArchivingOperateType.FINISHED, taskId);
            break;
          case ERROR:
            // already put error in log
            errorSet.remove(taskId);
            operateFromLog(ArchivingOperate.ArchivingOperateType.ERROR, taskId);
            break;
          default:
            logger.error("read archiving log: unknown type");
        }
      }

      // for each task in errorSet, the task started but didn't finish (an error)
      for (long errTaskId : errorSet) {
        if (archivingTasks.containsKey(errTaskId)) {
          // write to log and set task in ERROR in memory
          logWriter.log(ArchivingOperate.ArchivingOperateType.ERROR, archivingTasks.get(errTaskId));
          operateFromLog(ArchivingOperate.ArchivingOperateType.ERROR, errTaskId);
        } else {
          logger.error("unknown error taskId");
        }
      }
    } catch (Exception e) {
      logger.error("Cannot read log for archiving.");
    }

    recoverArchivingFiles();

    return new ArrayList<>(archivingTasks.values());
  }

  /** add archiving task to archivingTasks from log, does not write to log */
  public void setArchivingFromLog(ArchivingTask newTask) {
    if (currentTaskId > newTask.getTaskId()) {
      logger.error("set archiving error, current index larger than log index");
    }

    archivingTasks.put(newTask.getTaskId(), newTask);
    currentTaskId = newTask.getTaskId() + 1;
  }

  private ArchivingTask.ArchivingTaskStatus statusFromOperateType(
      ArchivingOperate.ArchivingOperateType archivingOperateType) {
    switch (archivingOperateType) {
      case RESUME:
        return ArchivingTask.ArchivingTaskStatus.READY;
      case CANCEL:
        return ArchivingTask.ArchivingTaskStatus.CANCELED;
      case START:
        return ArchivingTask.ArchivingTaskStatus.RUNNING;
      case PAUSE:
        return ArchivingTask.ArchivingTaskStatus.PAUSED;
      case FINISHED:
        return ArchivingTask.ArchivingTaskStatus.FINISHED;
      case ERROR:
        return ArchivingTask.ArchivingTaskStatus.ERROR;
    }
    return null;
  }

  /** operate Archiving to archivingTasks from log, does not write to log */
  public boolean operateFromLog(ArchivingOperate.ArchivingOperateType operateType, long taskId) {
    if (archivingTasks.containsKey(taskId)) {
      archivingTasks.get(taskId).setStatus(statusFromOperateType(operateType));
      return true;
    } else {
      return false;
    }
  }

  /** finish the unfinished ArchivingTask using log files under ARCHIVING_LOG_DIR */
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
        logger.error("ArchivingRecover: log file not found");
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

  public List<ArchivingTask> getArchivingTasks() {
    return new ArrayList<>(archivingTasks.values());
  }

  public long getCurrentTaskId() {
    return currentTaskId;
  }
}
