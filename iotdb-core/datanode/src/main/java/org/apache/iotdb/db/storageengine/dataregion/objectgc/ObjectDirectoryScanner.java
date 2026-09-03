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

package org.apache.iotdb.db.storageengine.dataregion.objectgc;

import org.apache.iotdb.calc.utils.IObjectPath;
import org.apache.iotdb.calc.utils.ObjectPathNaming;
import org.apache.iotdb.calc.utils.ObjectTypeUtils;
import org.apache.iotdb.commons.utils.TimePartitionUtils;
import org.apache.iotdb.db.i18n.StorageEngineMessages;
import org.apache.iotdb.db.storageengine.dataregion.modification.TableDeletionEntry;
import org.apache.iotdb.db.storageengine.rescon.disk.TierManager;

import com.timecho.iotdb.os.fileSystem.OSFile;
import org.apache.tsfile.fileSystem.FSFactoryProducer;
import org.apache.tsfile.fileSystem.FSType;
import org.apache.tsfile.fileSystem.fsFactory.FSFactory;
import org.apache.tsfile.utils.FSUtils;
import org.slf4j.Logger;

import java.io.File;
import java.io.IOException;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.StandardCopyOption;
import java.nio.file.attribute.BasicFileAttributes;

/**
 * Scans {@code object/} table directories: lock-held DELETE unlinks matching {@code .tmp}/{@code
 * .back}; async GC unlinks {@code .bin} files that pass the version gate.
 */
public final class ObjectDirectoryScanner {

  /**
   * Reserved sibling under {@code object/{regionId}/} for DROP TABLE tombstones. Live table lookups
   * never resolve this name, so a same-name recreate cannot collide with async GC.
   */
  public static final String OBJECT_GC_TOMBSTONE_DIR = ".iotdb-objectgc";

  private ObjectDirectoryScanner() {}

  public static void scanAndDelete(
      String databaseName,
      int regionId,
      ObjectGcRecord record,
      Logger failureLogger,
      String failureMessage) {
    TableDeletionEntry deletion = record.getDeletion();
    if (deletion == null) {
      return;
    }
    String tableName = deletion.getTableName();
    for (File tableDir :
        TierManager.getInstance().getAllMatchedObjectDirs(String.valueOf(regionId), tableName)) {
      scanTableDir(
          databaseName, regionId, tableDir, record, deletion, failureLogger, failureMessage);
    }
  }

  static void scanTableDir(
      String databaseName,
      int regionId,
      File tableDir,
      ObjectGcRecord record,
      TableDeletionEntry deletion,
      Logger failureLogger,
      String failureMessage) {
    if (tableDir == null) {
      return;
    }
    boolean remote = FSUtils.getFSType(tableDir) != FSType.LOCAL;
    if (!remote && !tableDir.exists()) {
      return;
    }
    File objectRoot =
        tableDir.getParentFile() == null ? null : tableDir.getParentFile().getParentFile();
    if (objectRoot == null) {
      return;
    }
    if (remote) {
      scanRemoteTableDir(
          databaseName,
          regionId,
          tableDir,
          objectRoot,
          record,
          deletion,
          failureLogger,
          failureMessage);
      return;
    }
    try {
      Files.walkFileTree(
          tableDir.toPath(),
          new SimpleFileVisitor<Path>() {
            @Override
            public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) {
              visitObjectBin(
                  databaseName,
                  regionId,
                  file.toFile(),
                  objectRoot,
                  record,
                  deletion,
                  failureLogger,
                  failureMessage);
              return FileVisitResult.CONTINUE;
            }
          });
    } catch (IOException e) {
      failureLogger.error(failureMessage, tableDir, e);
    }
  }

  private static void scanRemoteTableDir(
      String databaseName,
      int regionId,
      File tableDir,
      File objectRoot,
      ObjectGcRecord record,
      TableDeletionEntry deletion,
      Logger failureLogger,
      String failureMessage) {
    File[] bins =
        FSFactoryProducer.getFSFactory()
            .listFilesBySuffix(tableDir.getPath(), ObjectTypeUtils.OBJECT_FILE_SUFFIX);
    if (bins == null) {
      return;
    }
    for (File file : bins) {
      visitObjectBin(
          databaseName,
          regionId,
          file,
          objectRoot,
          record,
          deletion,
          failureLogger,
          failureMessage);
    }
  }

  private static void visitObjectBin(
      String databaseName,
      int regionId,
      File file,
      File objectRoot,
      ObjectGcRecord record,
      TableDeletionEntry deletion,
      Logger failureLogger,
      String failureMessage) {
    String name = ObjectPathNaming.baseFileName(file.getPath());
    if (name.endsWith(ObjectTypeUtils.OBJECT_TEMP_FILE_SUFFIX)
        || name.endsWith(ObjectTypeUtils.OBJECT_BACK_FILE_SUFFIX)
        || !name.endsWith(ObjectTypeUtils.OBJECT_FILE_SUFFIX)) {
      return;
    }
    long timestamp = ObjectPathNaming.parseTime(name);
    if (timestamp < 0) {
      return;
    }
    try {
      String relative = ObjectPathNaming.relativize(objectRoot.getPath(), file.getPath());
      IObjectPath objectPath = IObjectPath.fromRelativePath(relative);
      if (!deletion.affects(objectPath.getDeviceID(), timestamp, timestamp)
          || !deletion.affects(objectPath.getMeasurement())) {
        return;
      }
      long version = ObjectPathNaming.parseVersion(name);
      boolean delete =
          version == ObjectPathNaming.LEGACY_VERSION
              || record.shouldDeleteVersion(
                  TimePartitionUtils.getTimePartitionId(timestamp), version);
      if (delete) {
        ObjectTypeUtils.deleteObjectPath(
            databaseName,
            regionId,
            TimePartitionUtils.getTimePartitionId(timestamp),
            deletion.getTableName(),
            file);
      }
    } catch (Exception e) {
      failureLogger.error(failureMessage, file, e);
    }
  }

  /**
   * Under the DataRegion write lock: unlink {@code {time}.bin.tmp} / {@code {time}.bin.back} that
   * match {@code deletion}. These names carry no TsFile version, so async GC must not touch them (a
   * lock-free scan would race a new in-progress write). DROP TABLE already moves the whole prefix
   * and should not call this.
   */
  public static void unlinkMatchingTempAndBack(
      int regionId, TableDeletionEntry deletion, Logger failureLogger, String failureMessage) {
    if (deletion == null || deletion.isDroppingTable()) {
      return;
    }
    for (File tableDir :
        TierManager.getInstance()
            .getAllMatchedObjectDirs(String.valueOf(regionId), deletion.getTableName())) {
      unlinkTempAndBackInTableDir(tableDir, deletion, failureLogger, failureMessage);
    }
  }

  static void unlinkTempAndBackInTableDir(
      File tableDir, TableDeletionEntry deletion, Logger failureLogger, String failureMessage) {
    if (tableDir == null) {
      return;
    }
    boolean remote = FSUtils.getFSType(tableDir) != FSType.LOCAL;
    if (!remote && !tableDir.exists()) {
      return;
    }
    File objectRoot =
        tableDir.getParentFile() == null ? null : tableDir.getParentFile().getParentFile();
    if (objectRoot == null) {
      return;
    }
    if (remote) {
      unlinkRemoteTempAndBack(tableDir, objectRoot, deletion, failureLogger, failureMessage);
      return;
    }
    try {
      Files.walkFileTree(
          tableDir.toPath(),
          new SimpleFileVisitor<Path>() {
            @Override
            public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) {
              visitTempOrBack(file.toFile(), objectRoot, deletion, failureLogger, failureMessage);
              return FileVisitResult.CONTINUE;
            }
          });
    } catch (IOException e) {
      failureLogger.error(failureMessage, tableDir, e);
    }
  }

  private static void unlinkRemoteTempAndBack(
      File tableDir,
      File objectRoot,
      TableDeletionEntry deletion,
      Logger failureLogger,
      String failureMessage) {
    FSFactory fsFactory = FSFactoryProducer.getFSFactory();
    File[] tmpFiles =
        fsFactory.listFilesBySuffix(tableDir.getPath(), ObjectTypeUtils.OBJECT_TEMP_FILE_SUFFIX);
    File[] backFiles =
        fsFactory.listFilesBySuffix(tableDir.getPath(), ObjectTypeUtils.OBJECT_BACK_FILE_SUFFIX);
    if (tmpFiles != null) {
      for (File file : tmpFiles) {
        visitTempOrBack(file, objectRoot, deletion, failureLogger, failureMessage);
      }
    }
    if (backFiles != null) {
      for (File file : backFiles) {
        visitTempOrBack(file, objectRoot, deletion, failureLogger, failureMessage);
      }
    }
  }

  private static void visitTempOrBack(
      File file,
      File objectRoot,
      TableDeletionEntry deletion,
      Logger failureLogger,
      String failureMessage) {
    String name = ObjectPathNaming.baseFileName(file.getPath());
    if (!name.endsWith(ObjectTypeUtils.OBJECT_TEMP_FILE_SUFFIX)
        && !name.endsWith(ObjectTypeUtils.OBJECT_BACK_FILE_SUFFIX)) {
      return;
    }
    long timestamp = ObjectPathNaming.parseTime(name);
    if (timestamp < 0) {
      return;
    }
    try {
      String relative = ObjectPathNaming.relativize(objectRoot.getPath(), file.getPath());
      IObjectPath objectPath = IObjectPath.fromRelativePath(relative);
      if (!deletion.affects(objectPath.getDeviceID(), timestamp, timestamp)
          || !deletion.affects(objectPath.getMeasurement())) {
        return;
      }
      FSFactoryProducer.getFSFactory().deleteIfExists(file);
    } catch (Exception e) {
      failureLogger.error(failureMessage, file, e);
    }
  }

  /**
   * Under write lock: move a live table object directory to a tombstone path that cannot collide
   * with a same-name recreate. Returns {@code null} when the local source does not exist.
   */
  public static File renameTableDirForDrop(File tableDir, long taskId) throws IOException {
    if (tableDir == null) {
      return null;
    }
    File regionDir = tableDir.getParentFile();
    if (regionDir == null) {
      throw new IOException(
          String.format(
              StorageEngineMessages.EXCEPTION_OBJECT_TABLE_DIR_PARENT_IS_MISSING_ARG_8F41142A,
              tableDir));
    }
    FSFactory fsFactory = FSFactoryProducer.getFSFactory();
    File tombstoneRoot = fsFactory.getFile(regionDir, OBJECT_GC_TOMBSTONE_DIR);
    File dest = fsFactory.getFile(tombstoneRoot, taskId + "-" + tableDir.getName());
    if (FSUtils.getFSType(tableDir) == FSType.LOCAL) {
      if (!tableDir.exists()) {
        return null;
      }
      if (!tombstoneRoot.exists() && !tombstoneRoot.mkdirs()) {
        throw new IOException(
            String.format(
                StorageEngineMessages.EXCEPTION_CANNOT_CREATE_OBJECT_GC_TOMBSTONE_DIR_ARG_8E11E07B,
                tombstoneRoot));
      }
      try {
        Files.move(tableDir.toPath(), dest.toPath(), StandardCopyOption.ATOMIC_MOVE);
      } catch (IOException atomicUnsupported) {
        Files.move(tableDir.toPath(), dest.toPath());
      }
      return dest;
    }
    renameRemoteTablePrefix(tableDir, dest);
    return dest;
  }

  private static void renameRemoteTablePrefix(File tableDir, File dest) throws IOException {
    FSFactory fsFactory = FSFactoryProducer.getFSFactory();
    // Empty suffix matches all keys under the table prefix in one ListObjects.
    File[] objects = fsFactory.listFilesBySuffix(tableDir.getPath(), "");
    if (objects == null || objects.length == 0) {
      return;
    }
    String srcPrefix = tableDir.getPath();
    if (!srcPrefix.endsWith("/") && !srcPrefix.endsWith(File.separator)) {
      srcPrefix = srcPrefix + "/";
    }
    for (File src : objects) {
      String srcPath = src.getPath();
      String relative;
      if (srcPath.startsWith(srcPrefix)) {
        relative = srcPath.substring(srcPrefix.length());
      } else if (srcPath.startsWith(tableDir.getPath())) {
        relative = srcPath.substring(tableDir.getPath().length());
        if (relative.startsWith("/") || relative.startsWith(File.separator)) {
          relative = relative.substring(1);
        }
      } else {
        continue;
      }
      File target = fsFactory.getFile(dest, relative);
      if (!src.renameTo(target)) {
        throw new IOException(
            String.format(
                StorageEngineMessages
                    .EXCEPTION_FAILED_TO_RENAME_OBJECT_TABLE_DIR_ARG_TO_ARG_5DFF237E,
                src,
                target));
      }
    }
  }

  /**
   * Off the write lock: remove DROP TABLE tombstone prefixes. Local trees use recursive delete;
   * OBJECT_STORAGE uses {@link OSFile#deleteObjectsByPrefix()} so nested keys under the tombstone
   * are actually removed ({@code deleteDirectory} does not).
   */
  public static void dropTableDirs(
      Iterable<String> dirs, Logger failureLogger, String failureMessage) {
    for (String dir : dirs) {
      try {
        File file = FSFactoryProducer.getFSFactory().getFile(dir);
        if (FSUtils.getFSType(file) == FSType.LOCAL) {
          org.apache.iotdb.commons.utils.FileUtils.deleteFileOrDirectory(file, true);
        } else if (file instanceof OSFile) {
          ((OSFile) file).deleteObjectsByPrefix();
        } else {
          FSFactoryProducer.getFSFactory().deleteDirectory(file.getPath());
        }
      } catch (Exception e) {
        failureLogger.error(failureMessage, dir, e);
      }
    }
  }
}
