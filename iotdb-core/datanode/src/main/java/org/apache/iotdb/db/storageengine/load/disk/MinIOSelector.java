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

import org.apache.iotdb.commons.conf.IoTDBConstant;
import org.apache.iotdb.db.exception.DiskSpaceInsufficientException;
import org.apache.iotdb.db.storageengine.rescon.disk.FolderManager;
import org.apache.iotdb.db.storageengine.rescon.disk.TierManager;
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

  public MinIOSelector(String[] dirs) {
    if (dirs == null || dirs.length == 0) {
      rootDisks2DataDirsMapForLoad = Collections.emptyMap();
      logger.warn("MinIO selector requires at least one directory");
      return;
    }
    // init data dirs' root disks
    this.rootDisks2DataDirsMapForLoad = new HashMap<>(dirs.length);
    Arrays.stream(dirs)
        .filter(Objects::nonNull)
        .map(v -> fsFactory.getFile(v, IoTDBConstant.UNSEQUENCE_FOLDER_NAME).getPath())
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
  public File getTargetDir(
      File fileToLoad, TierManager tierManager, String tsfileName, int tierLevel)
      throws DiskSpaceInsufficientException {
    File targetFile;
    String fileDirRoot = null;
    try {
      fileDirRoot =
          Optional.ofNullable(FileStoreUtils.getFileStore(fileToLoad.getCanonicalPath()))
              .map(Object::toString)
              .orElse(null);
    } catch (Exception e) {
      logger.warn(
          "Exception occurs when reading target file's mount point {}",
          fileToLoad.getAbsoluteFile(),
          e);
    }

    if (rootDisks2DataDirsMapForLoad.containsKey(fileDirRoot)) {
      // if there is an overlap between firDirRoot and data directories' disk roots, try to get
      // targetFile in the same disk
      targetFile = fsFactory.getFile(rootDisks2DataDirsMapForLoad.get(fileDirRoot), tsfileName);

      return targetFile;
    }

    // if there isn't an overlap, downgrade to storage balance(sequence) strategy.
    return super.getTargetDir(fileToLoad, tierManager, tsfileName, tierLevel);
  }

  @Override
  public String getTargetDir(File fileToLoad, FolderManager folderManager)
      throws DiskSpaceInsufficientException {
    File targetFile;
    String fileDirRoot = null;
    try {
      fileDirRoot =
          Optional.ofNullable(FileStoreUtils.getFileStore(fileToLoad.getCanonicalPath()))
              .map(Object::toString)
              .orElse(null);
    } catch (Exception e) {
      logger.warn(
          "Exception occurs when reading target file's mount point {}",
          fileToLoad.getAbsoluteFile(),
          e);
    }

    if (rootDisks2DataDirsMapForLoad.containsKey(fileDirRoot)) {
      // if there is an overlap between firDirRoot and data directories' disk roots, try to get
      // targetFile in the same disk
      return rootDisks2DataDirsMapForLoad.get(fileDirRoot);
    }

    // if there isn't an overlap, downgrade to storage balance(sequence) strategy.
    return super.getTargetDir(fileToLoad, folderManager);
  }

  @Override
  public LoadDiskSelectorType getLoadDiskSelectorType() {
    return LoadDiskSelectorType.MIN_IO_FIRST;
  }
}
