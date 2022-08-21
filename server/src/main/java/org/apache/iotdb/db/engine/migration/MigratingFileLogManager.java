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

import org.apache.iotdb.db.engine.fileSystem.SystemFileFactory;
import org.apache.iotdb.db.engine.storagegroup.TsFileResource;
import org.apache.iotdb.tsfile.utils.FilePathUtils;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Paths;

import static org.apache.iotdb.db.metadata.idtable.IDTable.config;
import static org.apache.iotdb.db.metadata.idtable.IDTable.logger;

/**
 * To assure that migration of tsFiles is pesudo-atomic operator, MigratingFileLogManager writes
 * files to migratingFileDir when a tsFile (and its resource/mod files) are being migrated, then
 * deletes it after it has finished operation.
 */
public class MigratingFileLogManager {

  private static File MIGRATING_LOG_DIR =
      SystemFileFactory.INSTANCE.getFile(
          Paths.get(FilePathUtils.regularizePath(config.getSystemDir()), "migration", "migrating")
              .toString());

  protected MigratingFileLogManager() {
    if (MIGRATING_LOG_DIR == null) {
      logger.error("MIGRATING_LOG_DIR is null");
    }

    if (!MIGRATING_LOG_DIR.exists()) {
      if (MIGRATING_LOG_DIR.mkdirs()) {
        logger.info("MIGRATING_LOG_DIR {} created successfully", MIGRATING_LOG_DIR);
      } else {
        logger.error("MIGRATING_LOG_DIR {} create error", MIGRATING_LOG_DIR);
      }
      return;
    }

    if (!MIGRATING_LOG_DIR.isDirectory()) {
      logger.error("{} already exists but is not directory", MIGRATING_LOG_DIR);
    }
  }

  // singleton
  private static class MigratingFileLogManagerHolder {
    private MigratingFileLogManagerHolder() {}

    private static final MigratingFileLogManager INSTANCE = new MigratingFileLogManager();
  }

  public static MigratingFileLogManager getInstance() {
    return MigratingFileLogManagerHolder.INSTANCE;
  }

  /** started migrating tsfile and its resource/mod files */
  public boolean start(File tsfile, File targetDir) throws IOException {
    File logFile = SystemFileFactory.INSTANCE.getFile(MIGRATING_LOG_DIR, tsfile.getName() + ".log");
    if (!logFile.createNewFile()) {
      return false;
    }

    FileOutputStream logFileOutput = new FileOutputStream(logFile);
    ReadWriteIOUtils.write(tsfile.getAbsolutePath(), logFileOutput);
    ReadWriteIOUtils.write(targetDir.getAbsolutePath(), logFileOutput);
    logFileOutput.flush();
    logFileOutput.close();

    return true;
  }

  /** finished migrating tsfile and related files */
  public void finish(File tsfile) {
    File logFile = SystemFileFactory.INSTANCE.getFile(MIGRATING_LOG_DIR, tsfile.getName() + ".log");
    if (logFile.exists()) {
      logFile.delete();
    }
  }

  /** finish the unfinished MigrationTasks using log files under MIGRATING_LOG_DIR */
  public void recover() throws IOException {
    for (File logFile : MIGRATING_LOG_DIR.listFiles()) {
      FileInputStream logFileInput = new FileInputStream(logFile);

      String tsfilePath = ReadWriteIOUtils.readString(logFileInput);
      String targetDirPath = ReadWriteIOUtils.readString(logFileInput);

      File tsfile = SystemFileFactory.INSTANCE.getFile(tsfilePath);
      File targetDir = SystemFileFactory.INSTANCE.getFile(targetDirPath);

      if (targetDir.exists()) {
        if (!targetDir.isDirectory()) {
          logger.error("target dir {} not a directory", targetDirPath);
          return;
        }
      } else if (!targetDir.mkdirs()) {
        logger.error("create target dir {} failed", targetDirPath);
        return;
      }

      TsFileResource resource = new TsFileResource(tsfile);
      resource.migrate(targetDir);
      finish(tsfile);
    }
  }
}
