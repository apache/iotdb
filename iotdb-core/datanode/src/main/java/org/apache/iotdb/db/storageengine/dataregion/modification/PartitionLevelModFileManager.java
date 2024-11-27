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

import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResource;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

/** LevelModFileManager manages the shared mod files within one time partition. */
@SuppressWarnings("FieldCanBeLocal")
public class PartitionLevelModFileManager implements ModFileManagement {

  private final int levelModFileNumThreshold = 30;
  private final long singleModFileSizeThresholdByte = 16 * 1024L;
  // level -> mod file id -> mod file
  private final Map<Long, TreeMap<Long, ModificationFile>> levelModFileIdMap = new HashMap<>();
  private final Map<ModificationFile, Set<TsFileResource>> modFileReferences = new HashMap<>();

  @Override
  public synchronized ModificationFile recover(String modFilePath, TsFileResource tsFileResource)
      throws IOException {
    long[] levelAndModFileId = ModificationFile.parseFileName(new File(modFilePath).getName());
    long level = levelAndModFileId[0];
    long modFileId = levelAndModFileId[1];

    TreeMap<Long, ModificationFile> idModificationMap =
        levelModFileIdMap.computeIfAbsent(level, l -> new TreeMap<>());
    ModificationFile modificationFile =
        idModificationMap.computeIfAbsent(
            modFileId, id -> new ModificationFile(new File(modFilePath)));

    modFileReferences.computeIfAbsent(modificationFile, f -> new HashSet<>()).add(tsFileResource);

    return modificationFile;
  }

  @Override
  public ModificationFile allocateFor(TsFileResource tsFileResource) throws IOException {
    TsFileResource prev = tsFileResource.getPrev();
    TsFileResource next = tsFileResource.getNext();
    while (prev != null || next != null) {
      if (prev != null) {
        ModificationFile sharedModFile = prev.getSharedModFile();
        if (sharedModFile != null) {
          if (tryShare(sharedModFile, prev, tsFileResource)) {
            return sharedModFile;
          } else {
            // do not prove further if a TsFile with mod is already found
            prev = null;
          }
        } else {
          prev = prev.getPrev();
        }
      }

      if (next != null) {
        ModificationFile sharedModFile = next.getSharedModFile();
        if (sharedModFile != null) {
          if (tryShare(sharedModFile, next, tsFileResource)) {
            return sharedModFile;
          } else {
            // do not prove further if a TsFile with mod is already found
            next = null;
          }
        } else {
          next = next.getNext();
        }
      }
    }

    return allocateNew(tsFileResource);
  }

  private synchronized boolean tryShare(
      ModificationFile sharedModFile, TsFileResource modFileHolder, TsFileResource toAllocate)
      throws IOException {
    Set<TsFileResource> references = modFileReferences.get(sharedModFile);
    if (references.isEmpty()) {
      // the mod file is to be deleted, cannot share
      return false;
    }

    long level = modFileHolder.getTsFileID().compactionVersion;
    TreeMap<Long, ModificationFile> idModificationMap = levelModFileIdMap.get(level);
    if (idModificationMap.size() > levelModFileNumThreshold) {
      // too many mod files already, must share
      references.add(toAllocate);
      return true;
    }
    if (sharedModFile.getFileLength() < singleModFileSizeThresholdByte) {
      // mod file is not large enough, can share
      references.add(toAllocate);
      return true;
    }
    // mod file is already too large, do not share
    return false;
  }

  private synchronized ModificationFile allocateNew(TsFileResource tsFileResource) {
    long level = tsFileResource.getTsFileID().getInnerCompactionCount();
    TreeMap<Long, ModificationFile> idModificationMap =
        levelModFileIdMap.computeIfAbsent(level, l -> new TreeMap<>());
    long newId = idModificationMap.isEmpty() ? 1 : idModificationMap.lastEntry().getKey() + 1;
    ModificationFile newModFile =
        new ModificationFile(
            new File(
                tsFileResource.getTsFile().getParentFile(),
                ModificationFile.composeFileName(level, newId)));
    idModificationMap.put(newId, newModFile);

    Set<TsFileResource> references = new HashSet<>();
    references.add(tsFileResource);
    modFileReferences.put(newModFile, references);

    return newModFile;
  }

  @Override
  public synchronized void releaseFor(
      TsFileResource tsFileResource, ModificationFile modificationFile) throws IOException {
    Set<TsFileResource> references = modFileReferences.get(modificationFile);
    references.remove(tsFileResource);
    if (references.isEmpty()) {
      modFileReferences.remove(modificationFile);
      modificationFile.remove();
    }
  }

  @Override
  public synchronized void addReference(
      TsFileResource tsFileResource, ModificationFile modificationFile) {
    modFileReferences.computeIfAbsent(modificationFile, f -> new HashSet<>()).add(tsFileResource);
  }
}
