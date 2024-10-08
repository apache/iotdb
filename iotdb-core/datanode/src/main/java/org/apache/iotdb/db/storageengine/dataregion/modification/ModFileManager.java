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

package org.apache.iotdb.db.storageengine.dataregion.modification;

import org.apache.iotdb.commons.utils.FileUtils;
import org.apache.iotdb.commons.utils.TestOnly;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.service.metrics.FileMetrics;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileID;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResource;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;

/** A ModFileManager manages the ModificationFiles of a Time Partition. */
@SuppressWarnings({"SynchronizationOnLocalVariableOrMethodParameter"})
public class ModFileManager {

  private static final Logger LOGGER = LoggerFactory.getLogger(ModFileManager.class);
  // levelNum -> modFileNum -> modFile
  private final Map<Long, TreeMap<Long, ModificationFile>> allLevelModsFileMap =
      new ConcurrentHashMap<>();

  private final int levelModFileCntThreshold;
  private final long singleModFileSizeThreshold;

  @TestOnly
  public ModFileManager() {
    this.levelModFileCntThreshold =
        IoTDBDescriptor.getInstance().getConfig().getLevelModFileCntThreshold();
    this.singleModFileSizeThreshold =
        IoTDBDescriptor.getInstance().getConfig().getSingleModFileSizeThreshold();
  }

  public ModFileManager(int levelModFileCntThreshold, long singleModFileSizeThreshold) {
    this.levelModFileCntThreshold = levelModFileCntThreshold;
    this.singleModFileSizeThreshold = singleModFileSizeThreshold;
  }

  public ModificationFile recoverModFile(String modFilePath, TsFileResource resource) {
    File file = new File(modFilePath);
    String name = file.getName();
    long[] levelNumAndModNum = ModificationFile.parseFileName(name);

    ModificationFile modificationFile =
        allLevelModsFileMap
            .computeIfAbsent(levelNumAndModNum[0], k -> new TreeMap<>())
            .computeIfAbsent(
                levelNumAndModNum[1],
                k -> {
                  FileMetrics.getInstance().increaseModFileNum(1);
                  FileMetrics.getInstance().increaseModFileSize(file.length());
                  return new ModificationFile(file, resource);
                });
    modificationFile.addReference(resource);
    return modificationFile;
  }

  public void removeModFile(ModificationFile modificationFile) {
    File file = modificationFile.getFile();
    String name = file.getName();
    long[] levelNumAndModNum = ModificationFile.parseFileName(name);
    allLevelModsFileMap.get(levelNumAndModNum[0]).remove(levelNumAndModNum[1]);

    try {
      modificationFile.close();
      FileMetrics.getInstance().decreaseModFileNum(1);
      FileMetrics.getInstance().decreaseModFileSize(modificationFile.getSize());
      FileUtils.deleteFileOrDirectory(modificationFile.getFile());
    } catch (Exception e) {
      LOGGER.warn("Failed to close mod file {}", modificationFile.getFile(), e);
    }
  }

  private long maxModNum(long levelNum) {
    TreeMap<Long, ModificationFile> levelModFileMap =
        allLevelModsFileMap.computeIfAbsent(levelNum, k -> new TreeMap<>());
    if (levelModFileMap.isEmpty()) {
      return -1;
    } else {
      return levelModFileMap.lastKey();
    }
  }

  /**
   * Allocate a Mod File by newing or sharing to the give TsFile.
   *
   * @param resource tsFile to allocate
   */
  public ModificationFile allocate(TsFileResource resource) {
    TsFileID tsFileID = resource.getTsFileID();
    long levelNum = tsFileID.getInnerCompactionCount();

    // find the nearest TsFile that has a mod, i.e, share candidate
    TsFileResource prev = resource.getPrev();
    TsFileResource next = resource.getNext();
    ModificationFile allocatedModFile;
    while (prev != null || next != null) {
      if (prev != null) {
        allocatedModFile = allocate(resource, prev, levelNum);
        if (allocatedModFile != null) {
          return allocatedModFile;
        }
        prev = prev.getPrev();
      }

      if (next != null) {
        allocatedModFile = allocate(resource, next, levelNum);
        if (allocatedModFile != null) {
          return allocatedModFile;
        }
        next = next.getNext();
      }
    }

    // no mod file found, allocate a new one
    return allocateNew(resource);
  }

  private ModificationFile allocate(
      TsFileResource target, TsFileResource shareCandidate, long levelNum) {
    ModificationFile allocatedModFile = null;
    ModificationFile prevModFile = shareCandidate.getModFile();
    if (prevModFile != null) {
      if (shouldAllocateNew(prevModFile, levelNum)) {
        allocatedModFile = allocateNew(target);
      } else {
        allocatedModFile = prevModFile;
        allocatedModFile.addReference(target);
      }
    }
    return allocatedModFile;
  }

  private boolean shouldAllocateNew(ModificationFile modificationFile, long levelNum) {
    int modFileCnt = allLevelModsFileMap.computeIfAbsent(levelNum, k -> new TreeMap<>()).size();
    if (modFileCnt >= levelModFileCntThreshold) {
      // too many mod files already, do not allocate new
      return false;
    }

    // if the mod file is large enough, allocate a new one
    long fileLength = modificationFile.getFile().length();
    return fileLength > singleModFileSizeThreshold;
  }

  /**
   * Force to allocate a new Mod File for the TsFile. This will NOT set any fields of the
   * TsFileResource.
   *
   * @param resource TsFile to allocate.
   * @return the newly allocated Mod File.
   */
  private ModificationFile allocateNew(TsFileResource resource) {
    TsFileID tsFileID = resource.getTsFileID();
    long levelNum = tsFileID.getInnerCompactionCount();
    long nextModNum = maxModNum(levelNum) + 1;
    File file =
        new File(
            resource.getTsFile().getParentFile(),
            ModificationFile.composeFileName(levelNum, nextModNum));
    TreeMap<Long, ModificationFile> levelModsFileMap =
        this.allLevelModsFileMap.computeIfAbsent(levelNum, k -> new TreeMap<>());
    synchronized (levelModsFileMap) {
      FileMetrics.getInstance().increaseModFileNum(1);
      // use the provided file as the initial reference to avoid a newly created Mod File being
      // cleaned
      return levelModsFileMap.computeIfAbsent(
          nextModNum, k -> new ModificationFile(file, resource));
    }
  }

  public void cleanModFile() {
    for (TreeMap<Long, ModificationFile> levelModFileMap : allLevelModsFileMap.values()) {
      List<Long> modFilesToRemove = new ArrayList<>();
      synchronized (levelModFileMap) {
        levelModFileMap.forEach(
            (modNum, modFile) -> {
              if (!modFile.hasReference()) {
                modFilesToRemove.add(modNum);
              }
            });
      }

      synchronized (levelModFileMap) {
        for (Long fileNum : modFilesToRemove) {
          ModificationFile remove = levelModFileMap.remove(fileNum);
          try {
            remove.close();
            FileMetrics.getInstance().decreaseModFileNum(1);
            FileMetrics.getInstance().decreaseModFileSize(remove.getSize());
            FileUtils.deleteFileOrDirectory(remove.getFile());
          } catch (Exception e) {
            LOGGER.warn("Failed to close mod file {}", remove.getFile(), e);
          }
        }
      }
    }
  }
}
