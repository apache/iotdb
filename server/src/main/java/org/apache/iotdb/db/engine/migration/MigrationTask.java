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
package org.apache.iotdb.db.engine.migration;

import org.apache.iotdb.db.engine.modification.ModificationFile;
import org.apache.iotdb.db.engine.storagegroup.TsFileResource;
import org.apache.iotdb.db.engine.storagegroup.TsFileResourceStatus;
import org.apache.iotdb.tsfile.fileSystem.FSFactoryProducer;
import org.apache.iotdb.tsfile.fileSystem.fsFactory.FSFactory;
import org.apache.iotdb.tsfile.utils.FSUtils;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public abstract class MigrationTask implements Runnable {
  protected static final FSFactory fsFactory = FSFactoryProducer.getFSFactory();

  protected final MigrationCause cause;
  protected final TsFileResource tsFileResource;
  protected final String targetDir;
  protected final File srcFile;
  protected final File destTsFile;
  protected final File srcResourceFile;
  protected final File destResourceFile;
  protected final File srcModsFile;
  protected final File destModsFile;

  protected final List<File> filesShouldDelete = new ArrayList<>();

  protected MigrationTask(MigrationCause cause, TsFileResource tsFileResource, String targetDir)
      throws IOException {
    this.cause = cause;
    this.tsFileResource = tsFileResource;
    this.targetDir = targetDir;
    this.srcFile = tsFileResource.getTsFile();
    this.destTsFile = fsFactory.getFile(targetDir, getDestTsFilePath(srcFile));
    this.srcResourceFile =
        fsFactory.getFile(
            srcFile.getParentFile(), srcFile.getName() + TsFileResource.RESOURCE_SUFFIX);
    this.destResourceFile =
        fsFactory.getFile(targetDir, getDestTsFilePath(srcFile) + TsFileResource.RESOURCE_SUFFIX);
    this.srcModsFile =
        fsFactory.getFile(
            srcFile.getParentFile(), srcFile.getName() + ModificationFile.FILE_SUFFIX);
    this.destModsFile =
        fsFactory.getFile(targetDir, getDestTsFilePath(srcFile) + ModificationFile.FILE_SUFFIX);
  }

  private String getDestTsFilePath(File src) throws IOException {
    return FSUtils.getLocalTsFileShortPath(src, FSUtils.PATH_FROM_DATABASE_LEVEL);
  }

  public static MigrationTask newTask(
      MigrationCause cause, TsFileResource sourceTsFile, String targetDir) throws IOException {
    if (FSUtils.isLocal(targetDir)) {
      return new LocalMigrationTask(cause, sourceTsFile, targetDir);
    } else {
      return new RemoteMigrationTask(cause, sourceTsFile, targetDir);
    }
  }

  @Override
  public void run() {
    try {
      migrate();
    } finally {
      // try to set the final status to NORMAL to avoid migrate failure
      // TODO: this setting may occur side effects
      tsFileResource.setStatus(TsFileResourceStatus.NORMAL);
    }
  }

  public abstract void migrate();

  protected void migrateFile(File src, File dest) throws IOException {
    fsFactory.copyFile(src, dest);
    filesShouldDelete.add(dest);
  }

  protected void cleanup() {
    filesShouldDelete.forEach(this::deleteIfExist);
    filesShouldDelete.clear();
  }

  protected void deleteIfExist(File file) {
    if (file.exists()) {
      file.delete();
    }
  }
}
