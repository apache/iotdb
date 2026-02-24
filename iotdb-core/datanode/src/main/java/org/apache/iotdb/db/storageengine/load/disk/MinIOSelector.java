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

package org.apache.iotdb.db.storageengine.load.disk;

import org.apache.iotdb.db.exception.DiskSpaceInsufficientException;
import org.apache.iotdb.db.exception.load.LoadFileException;
import org.apache.iotdb.metrics.utils.FileStoreUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.nio.file.FileStore;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

public class MinIOSelector extends InheritSystemMultiDisksStrategySelector {

  private static final Logger logger = LoggerFactory.getLogger(MinIOSelector.class);

  private final Map<String, String> rootDisks2DataDirsMapForLoad;

  public MinIOSelector(final String[] dirs, final DiskDirectorySelector selector) {
    super(selector);
    if (dirs == null || dirs.length == 0) {
      rootDisks2DataDirsMapForLoad = Collections.emptyMap();
      logger.warn("MinIO selector requires at least one directory");
      return;
    }
    // init data dirs' root disks
    this.rootDisks2DataDirsMapForLoad = new HashMap<>(dirs.length);
    Arrays.stream(dirs)
        .filter(Objects::nonNull)
        .forEach(
            dataDirPath -> {
              File dataDirFile = new File(dataDirPath);
              try {
                FileStore fileStore = FileStoreUtils.getFileStore(dataDirFile.getCanonicalPath());
                if (fileStore != null) {
                  String mountPoint = fileStore.toString();
                  this.rootDisks2DataDirsMapForLoad.put(mountPoint, dataDirPath);
                  logger.info("Add {}'s mount point {}", dataDirPath, mountPoint);
                } else {
                  logger.info(
                      "Failed to find mount point {}, skip register it to map", dataDirPath);
                }
              } catch (Exception e) {
                logger.warn(
                    "Exception occurs when reading data dir's mount point {}", dataDirPath, e);
              }
            });
  }

  @Override
  public File selectTargetDirectory(
      final File sourceDirectory,
      final String fileName,
      final boolean appendFileName,
      final int tierLevel)
      throws DiskSpaceInsufficientException, LoadFileException {
    String fileDirRoot = null;
    try {
      if (sourceDirectory != null) {
        fileDirRoot =
            Optional.ofNullable(FileStoreUtils.getFileStore(sourceDirectory.getCanonicalPath()))
                .map(Object::toString)
                .orElse(null);
      }
    } catch (Exception e) {
      logger.warn(
          "Exception occurs when reading target file's mount point {}",
          sourceDirectory.getAbsoluteFile(),
          e);
    }

    File targetFile = null;
    if (rootDisks2DataDirsMapForLoad.containsKey(fileDirRoot)) {
      if (appendFileName) {
        // if there is an overlap between firDirRoot and data directories' disk roots, try to get
        // targetFile in the same disk
        targetFile = fsFactory.getFile(rootDisks2DataDirsMapForLoad.get(fileDirRoot), fileName);
      } else {
        targetFile = new File(rootDisks2DataDirsMapForLoad.get(fileDirRoot));
      }

      return targetFile;
    }

    // if there isn't an overlap, downgrade to storage balance(sequence) strategy.
    return super.selectTargetDirectory(sourceDirectory, fileName, appendFileName, tierLevel);
  }
}
