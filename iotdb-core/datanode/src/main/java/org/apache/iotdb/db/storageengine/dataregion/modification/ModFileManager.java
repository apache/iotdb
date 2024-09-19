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

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.iotdb.commons.utils.FileUtils;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileID;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A ModFileManager manages the ModificationFiles of a Time Partition.
 */
@SuppressWarnings({"resource", "SynchronizationOnLocalVariableOrMethodParameter"})
public class ModFileManager {

  private static final Logger LOGGER = LoggerFactory.getLogger(ModFileManager.class);
  // levelNum -> modFileNum -> modFile
  private final Map<Long, TreeMap<Long, ModificationFile>> allLevelModsFileMap = new ConcurrentHashMap<>();

  public void recoverModFile(String modFilePath, TsFileResource resource) {
    File file = new File(modFilePath);
    String name = file.getName();
    long[] levelNumAndModNum = ModificationFile.parseFileName(name);

    ModificationFile modificationFile = allLevelModsFileMap.computeIfAbsent(levelNumAndModNum[0],
        k -> new TreeMap<>()).computeIfAbsent(levelNumAndModNum[1], k -> new ModificationFile(file, resource));
    modificationFile.addReference(resource);
  }

  private long maxModNum(long levelNum) {
    TreeMap<Long, ModificationFile> levelModFileMap = allLevelModsFileMap.computeIfAbsent(
        levelNum, k -> new TreeMap<>());
    if (levelModFileMap.isEmpty()) {
      return -1;
    } else {
      return levelModFileMap.lastKey();
    }
  }

  public ModificationFile allocateNew(TsFileResource resource) {
    TsFileID tsFileID = resource.getTsFileID();
    long levelNum = tsFileID.getInnerCompactionCount();
    long nextModNum = maxModNum(levelNum) + 1;
    File file = new File(resource.getTsFile().getParentFile(), ModificationFile.composeFileName(levelNum, nextModNum));
    TreeMap<Long, ModificationFile> levelModsFileMap = this.allLevelModsFileMap.computeIfAbsent(
        levelNum,
        k -> new TreeMap<>());
    synchronized (levelModsFileMap) {
      return levelModsFileMap.computeIfAbsent(nextModNum, k -> new ModificationFile(file, resource));
    }
  }

  public void cleanModFile() {
    for (TreeMap<Long, ModificationFile> levelModFileMap : allLevelModsFileMap.values()) {
      List<Long> modFilesToRemove = new ArrayList<>();
      synchronized (levelModFileMap) {
        levelModFileMap.forEach((modNum, modFile) -> {
          if (!modFile.hasReference()) {
            modFilesToRemove.add(modNum);
          }
        });
      }

      synchronized (levelModFileMap) {
        for (Long l : modFilesToRemove) {
          ModificationFile remove = levelModFileMap.remove(l);
          try {
            remove.close();
            FileUtils.deleteFileOrDirectory(remove.getFile());
          } catch (Exception e) {
            LOGGER.warn("Failed to close mod file {}", remove.getFile(), e);
          }
        }
      }
    }
  }
}
