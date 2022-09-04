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
package org.apache.iotdb.db.engine.migration;

import org.apache.iotdb.db.conf.IoTDBConfig;
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
 * MigrationRecover class encapsulates the recover logic, it retrieves the MigrationTasks and
 * finishes migrating the tsfiles.
 */
public class MigrationRecover {
  private static final Logger logger = LoggerFactory.getLogger(MigrationRecover.class);
  private static final IoTDBConfig config = IoTDBDescriptor.getInstance().getConfig();
  private static final String LOG_FILE_NAME =
      Paths.get(FilePathUtils.regularizePath(config.getSystemDir()), "migration", "log.bin")
          .toString();
  private static final File MIGRATING_LOG_DIR =
      SystemFileFactory.INSTANCE.getFile(
          Paths.get(FilePathUtils.regularizePath(config.getSystemDir()), "migration", "migrating")
              .toString());
  private Map<Long, MigrationTask> migrationTasks;
  private long currentTaskId = 0;

  public List<MigrationTask> recover() {
    // first recover migrating tsfiles
    recoverMigratingFiles();

    migrationTasks = new HashMap<>();
    currentTaskId = 0;

    // read from logReader
    try (MigrationOperateReader logReader = new MigrationOperateReader(LOG_FILE_NAME);
        MigrationOperateWriter logWriter = new MigrationOperateWriter(LOG_FILE_NAME)) {
      Set<Long> errorSet = new HashSet<>();

      while (logReader.hasNext()) {
        MigrationOperate operate = logReader.next();
        long taskId = operate.getTask().getTaskId();

        switch (operate.getType()) {
          case SET:
            setMigrationFromLog(operate.getTask());
            break;
          case CANCEL:
            operateFromLog(MigrationOperate.MigrationOperateType.CANCEL, taskId);
            break;
          case START:
            // if task started but didn't finish, then error occurred
            errorSet.add(taskId);
            break;
          case PAUSE:
            errorSet.remove(taskId);
            operateFromLog(MigrationOperate.MigrationOperateType.PAUSE, taskId);
            break;
          case RESUME:
            operateFromLog(MigrationOperate.MigrationOperateType.RESUME, taskId);
            break;
          case FINISHED:
            // finished task => remove from list and remove from potential error task
            errorSet.remove(taskId);
            migrationTasks.remove(taskId);
            operateFromLog(MigrationOperate.MigrationOperateType.FINISHED, taskId);
            break;
          case ERROR:
            // already put error in log
            errorSet.remove(taskId);
            operateFromLog(MigrationOperate.MigrationOperateType.ERROR, taskId);
            break;
          default:
            logger.error("read migration log: unknown type");
        }
      }

      // for each task in errorSet, the task started but didn't finish (an error)
      for (long errTaskId : errorSet) {
        if (migrationTasks.containsKey(errTaskId)) {
          // write to log and set task in ERROR in memory
          logWriter.log(MigrationOperate.MigrationOperateType.ERROR, migrationTasks.get(errTaskId));
          operateFromLog(MigrationOperate.MigrationOperateType.ERROR, errTaskId);
        } else {
          logger.error("unknown error taskId");
        }
      }
    } catch (Exception e) {
      logger.error("Cannot read log for migration.");
    }

    recoverMigratingFiles();

    return new ArrayList<>(migrationTasks.values());
  }

  /** add migration task to migrationTasks from log, does not write to log */
  public void setMigrationFromLog(MigrationTask newTask) {
    if (currentTaskId > newTask.getTaskId()) {
      logger.error("set migration error, current index larger than log index");
    }

    migrationTasks.put(newTask.getTaskId(), newTask);
    currentTaskId = newTask.getTaskId() + 1;
  }

  private MigrationTask.MigrationTaskStatus statusFromOperateType(
      MigrationOperate.MigrationOperateType migrationOperateType) {
    switch (migrationOperateType) {
      case RESUME:
        return MigrationTask.MigrationTaskStatus.READY;
      case CANCEL:
        return MigrationTask.MigrationTaskStatus.CANCELED;
      case START:
        return MigrationTask.MigrationTaskStatus.RUNNING;
      case PAUSE:
        return MigrationTask.MigrationTaskStatus.PAUSED;
      case FINISHED:
        return MigrationTask.MigrationTaskStatus.FINISHED;
      case ERROR:
        return MigrationTask.MigrationTaskStatus.ERROR;
    }
    return null;
  }

  /** operate Migration to migrationTasks from log, does not write to log */
  public boolean operateFromLog(MigrationOperate.MigrationOperateType operateType, long taskId) {
    if (migrationTasks.containsKey(taskId)) {
      migrationTasks.get(taskId).setStatus(statusFromOperateType(operateType));
      return true;
    } else {
      return false;
    }
  }

  /** finish the unfinished MigrationTasks using log files under MIGRATING_LOG_DIR */
  public void recoverMigratingFiles() {
    File[] migratingLogFiles = MIGRATING_LOG_DIR.listFiles();
    for (File logFile : migratingLogFiles) {
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
        logger.error("MigratingFileLogManager: log file not found");
        continue;
      }

      while (tsfilePath != null && !tsfilePath.isEmpty()) {
        tsfile = SystemFileFactory.INSTANCE.getFile(tsfilePath);

        TsFileResource resource = new TsFileResource(tsfile);
        resource.migrate(targetDir);

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
        logger.error("Migration Log File Error", e);
        break;
      }

      logFile.delete();
    }
  }

  public List<MigrationTask> getMigrationTasks() {
    return new ArrayList<>(migrationTasks.values());
  }

  public long getCurrentTaskId() {
    return currentTaskId;
  }
}
