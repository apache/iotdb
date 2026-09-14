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

package com.timecho.iotdb.dataregion.migration;

import org.apache.iotdb.calc.utils.ObjectPathNaming;
import org.apache.iotdb.commons.exception.DiskSpaceInsufficientException;
import org.apache.iotdb.db.storageengine.rescon.disk.TierManager;

import com.timecho.iotdb.i18n.TimechoServerMessages;
import org.apache.tsfile.fileSystem.FSFactoryProducer;
import org.apache.tsfile.fileSystem.fsFactory.FSFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;

/**
 * Helpers for independent OBJECT {@code .bin} tier migration (copy to next tier, then delete source
 * — same lifecycle as TsFile migration). Cross-tier lookup keeps reads working during and after the
 * move.
 */
public final class ObjectFileMigrationUtils {

  private static final FSFactory FS_FACTORY = FSFactoryProducer.getFSFactory();

  private ObjectFileMigrationUtils() {}

  /**
   * Relative path of an object file under its object-root folder (e.g. {@code
   * regionId/table/.../ts.bin}).
   */
  public static String toRelativeObjectPath(File objectFile, String objectRoot) throws IOException {
    Path rootPath = new File(objectRoot).getCanonicalFile().toPath();
    Path filePath = objectFile.getCanonicalFile().toPath();
    if (!filePath.startsWith(rootPath)) {
      throw new IOException(
          String.format(
              TimechoServerMessages.EXCEPTION_OBJECT_FILE_ARG_IS_NOT_UNDER_OBJECT_ROOT_ARG_1E9ADBF8,
              objectFile,
              objectRoot));
    }
    return rootPath.relativize(filePath).toString();
  }

  /** Parse object timestamp from {@code {time}.bin} or {@code {time}_{ver}.bin}. */
  public static long parseObjectTimestamp(String fileName) {
    return ObjectPathNaming.parseTime(fileName);
  }

  public static boolean hasTempSibling(File objectFile) {
    long time = parseObjectTimestamp(objectFile.getName());
    if (time < 0) {
      return new File(objectFile.getPath() + ".tmp").exists()
          || new File(objectFile.getPath() + ".back").exists();
    }
    File parent = objectFile.getParentFile();
    if (parent == null) {
      return false;
    }
    return new File(parent, ObjectPathNaming.toTempFileName(time)).exists()
        || new File(parent, ObjectPathNaming.toBackFileName(time)).exists();
  }

  /**
   * Copy {@code srcFile} to {@code destTier} under the same relative path if absent, then delete
   * the source (like TsFile migration).
   *
   * @return destination file, or {@code null} if the source was deleted concurrently
   */
  public static File migrateObjectFile(File srcFile, String relativePath, int destTier)
      throws IOException, DiskSpaceInsufficientException {
    String destRoot = TierManager.getInstance().getNextFolderForObjectFile(destTier);
    File destFile = FS_FACTORY.getFile(destRoot, relativePath);
    if (!destFile.exists()) {
      File parent = destFile.getParentFile();
      if (parent != null) {
        parent.mkdirs();
      }
      long size = srcFile.length();
      MigrationTaskManager.getInstance()
          .acquireMigrateSpeedLimiter(Math.max(0, destTier - 1), size);
      FS_FACTORY.copyFile(srcFile, destFile);
    }
    if (!destFile.exists()) {
      throw new IOException(
          String.format(
              TimechoServerMessages.EXCEPTION_OBJECT_DESTINATION_MISSING_AFTER_COPY_ARG_57B61C6C,
              destFile));
    }
    if (!srcFile.exists()) {
      // The source may be deleted by a concurrent metadata-insertion rollback while the copy is in
      // progress. The rollback deletes tiers from hot to cold, so remove the destination copied
      // after its cold-tier check to avoid leaving an object without metadata.
      deleteIfExistsWithRetry(destFile);
      return null;
    }
    // Same as TsFile migration: source can be removed after a successful move.
    if (!srcFile.equals(destFile)) {
      FS_FACTORY.deleteIfExists(srcFile);
    }
    return destFile;
  }

  private static void deleteIfExistsWithRetry(File file) throws IOException {
    IOException lastException = null;
    for (int attempt = 0; attempt < 2; attempt++) {
      try {
        FS_FACTORY.deleteIfExists(file);
        return;
      } catch (IOException e) {
        lastException = e;
      }
    }
    throw lastException;
  }
}
