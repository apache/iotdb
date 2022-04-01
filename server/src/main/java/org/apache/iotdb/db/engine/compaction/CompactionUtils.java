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
package org.apache.iotdb.db.engine.compaction;

import org.apache.iotdb.commons.conf.IoTDBConstant;
import org.apache.iotdb.db.engine.modification.Modification;
import org.apache.iotdb.db.engine.modification.ModificationFile;
import org.apache.iotdb.db.engine.storagegroup.TsFileNameGenerator;
import org.apache.iotdb.db.engine.storagegroup.TsFileResource;
import org.apache.iotdb.tsfile.common.constant.TsFileConstant;
import org.apache.iotdb.tsfile.exception.write.WriteProcessException;
import org.apache.iotdb.tsfile.fileSystem.FSFactoryProducer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * This tool can be used to perform inner space or cross space compaction of aligned and non aligned
 * timeseries . Currently, we use {@link
 * org.apache.iotdb.db.engine.compaction.inner.utils.InnerSpaceCompactionUtils} to speed up if it is
 * an inner space compaction.
 */
public class CompactionUtils {
  private static final Logger logger =
      LoggerFactory.getLogger(IoTDBConstant.COMPACTION_LOGGER_NAME);

  /**
   * Update the targetResource. Move tmp target file to target file and serialize
   * xxx.tsfile.resource.
   */
  public static void moveTargetFile(
      List<TsFileResource> targetResources, boolean isInnerSpace, String fullStorageGroupName)
      throws IOException, WriteProcessException {
    String fileSuffix;
    if (isInnerSpace) {
      fileSuffix = IoTDBConstant.INNER_COMPACTION_TMP_FILE_SUFFIX;
    } else {
      fileSuffix = IoTDBConstant.CROSS_COMPACTION_TMP_FILE_SUFFIX;
    }
    for (TsFileResource targetResource : targetResources) {
      moveOneTargetFile(targetResource, fileSuffix, fullStorageGroupName);
    }
  }

  private static void moveOneTargetFile(
      TsFileResource targetResource, String tmpFileSuffix, String fullStorageGroupName)
      throws IOException {
    // move to target file and delete old tmp target file
    if (!targetResource.getTsFile().exists()) {
      logger.info(
          "{} [Compaction] Tmp target tsfile {} may be deleted after compaction.",
          fullStorageGroupName,
          targetResource.getTsFilePath());
      return;
    }
    File newFile =
        new File(
            targetResource.getTsFilePath().replace(tmpFileSuffix, TsFileConstant.TSFILE_SUFFIX));
    if (!newFile.exists()) {
      FSFactoryProducer.getFSFactory().moveFile(targetResource.getTsFile(), newFile);
    }

    // serialize xxx.tsfile.resource
    targetResource.setFile(newFile);
    targetResource.serialize();
    targetResource.close();
  }

  /**
   * Collect all the compaction modification files of source files, and combines them as the
   * modification file of target file.
   */
  public static void combineModsInCompaction(
      List<TsFileResource> seqResources,
      List<TsFileResource> unseqResources,
      List<TsFileResource> targetResources)
      throws IOException {
    // target file may less than source seq files, so we should find each target file with its
    // corresponding source seq file.
    Map<String, TsFileResource> seqFileInfoMap = new HashMap<>();
    for (TsFileResource tsFileResource : seqResources) {
      seqFileInfoMap.put(
          TsFileNameGenerator.increaseCrossCompactionCnt(tsFileResource.getTsFile()).getName(),
          tsFileResource);
    }
    // update each target mods file.
    for (TsFileResource tsFileResource : targetResources) {
      updateOneTargetMods(
          tsFileResource, seqFileInfoMap.get(tsFileResource.getTsFile().getName()), unseqResources);
    }
  }

  private static void updateOneTargetMods(
      TsFileResource targetFile, TsFileResource seqFile, List<TsFileResource> unseqFiles)
      throws IOException {
    // write mods in the seq file
    if (seqFile != null) {
      ModificationFile seqCompactionModificationFile = ModificationFile.getCompactionMods(seqFile);
      for (Modification modification : seqCompactionModificationFile.getModifications()) {
        targetFile.getModFile().write(modification);
      }
    }
    // write mods in all unseq files
    for (TsFileResource unseqFile : unseqFiles) {
      ModificationFile compactionUnseqModificationFile =
          ModificationFile.getCompactionMods(unseqFile);
      for (Modification modification : compactionUnseqModificationFile.getModifications()) {
        targetFile.getModFile().write(modification);
      }
    }
    targetFile.getModFile().close();
  }

  public static void deleteCompactionModsFile(
      List<TsFileResource> selectedSeqTsFileResourceList,
      List<TsFileResource> selectedUnSeqTsFileResourceList)
      throws IOException {
    for (TsFileResource seqFile : selectedSeqTsFileResourceList) {
      ModificationFile modificationFile = seqFile.getCompactionModFile();
      if (modificationFile.exists()) {
        modificationFile.remove();
      }
    }
    for (TsFileResource unseqFile : selectedUnSeqTsFileResourceList) {
      ModificationFile modificationFile = unseqFile.getCompactionModFile();
      if (modificationFile.exists()) {
        modificationFile.remove();
      }
    }
  }
}
