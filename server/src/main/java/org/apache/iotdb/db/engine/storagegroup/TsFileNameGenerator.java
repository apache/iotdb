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

package org.apache.iotdb.db.engine.storagegroup;

import org.apache.iotdb.db.conf.directories.DirectoryManager;
import org.apache.iotdb.db.exception.DiskSpaceInsufficientException;
import org.apache.iotdb.tsfile.fileSystem.FSFactoryProducer;
import org.apache.iotdb.tsfile.fileSystem.fsFactory.FSFactory;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

import static org.apache.iotdb.commons.conf.IoTDBConstant.CROSS_COMPACTION_TMP_FILE_VERSION_INTERVAL;

public class TsFileNameGenerator {

  private static final FSFactory fsFactory = FSFactoryProducer.getFSFactory();

  public static String generateNewTsFilePath(
      String tsFileDir,
      long time,
      long version,
      int innerSpaceCompactionCount,
      int crossSpaceCompactionCount) {
    return tsFileDir
        + File.separator
        + TsFileName.getTsFileName(
            time, version, innerSpaceCompactionCount, crossSpaceCompactionCount);
  }

  public static String generateNewTsFilePathWithMkdir(
      boolean sequence,
      String logicalStorageGroup,
      String virtualStorageGroup,
      long timePartitionId,
      long time,
      long version,
      int innerSpaceCompactionCount,
      int crossSpaceCompactionCount)
      throws DiskSpaceInsufficientException {
    String tsFileDir =
        generateTsFileDir(sequence, logicalStorageGroup, virtualStorageGroup, timePartitionId);
    fsFactory.getFile(tsFileDir).mkdirs();
    return tsFileDir
        + File.separator
        + TsFileName.getTsFileName(
            time, version, innerSpaceCompactionCount, crossSpaceCompactionCount);
  }

  public static String generateTsFileDir(
      boolean sequence,
      String logicalStorageGroup,
      String virtualStorageGroup,
      long timePartitionId)
      throws DiskSpaceInsufficientException {
    DirectoryManager directoryManager = DirectoryManager.getInstance();
    String baseDir =
        sequence
            ? directoryManager.getNextFolderForSequenceFile()
            : directoryManager.getNextFolderForUnSequenceFile();
    return baseDir
        + File.separator
        + logicalStorageGroup
        + File.separator
        + virtualStorageGroup
        + File.separator
        + timePartitionId;
  }

  public static TsFileResource increaseCrossCompactionCnt(TsFileResource tsFileResource) {
    File tsFile = tsFileResource.getTsFile();
    String path = tsFile.getParent();
    TsFileName tsFileName = TsFileName.parse(tsFileResource.getTsFile().getName());
    tsFileName.setCrossCompactionCnt(tsFileName.getCrossCompactionCnt() + 1);
    tsFileResource.setFile(new File(path, tsFileName.toFileName()));
    return tsFileResource;
  }

  public static File increaseCrossCompactionCnt(File tsFile) {
    String path = tsFile.getParent();
    TsFileName tsFileName = TsFileName.parse(tsFile.getName());
    tsFileName.setCrossCompactionCnt(tsFileName.getCrossCompactionCnt() + 1);
    return new File(path, tsFileName.toFileName());
  }

  public static File increaseFileVersion(File tsFile, long offset) {
    String path = tsFile.getParent();
    TsFileName tsFileName = TsFileName.parse(tsFile.getName());
    tsFileName.setVersion(tsFileName.getVersion() + offset);
    return new File(path, tsFileName.toFileName());
  }

  /**
   * Create tmp target file for cross space compaction, in which each sequence source file has its
   * own tmp target file.
   *
   * @param seqResources
   * @return tmp target file list, which is xxx.cross
   */
  public static List<TsFileResource> getCrossCompactionTargetFileResources(
      List<TsFileResource> seqResources) {
    List<TsFileResource> targetFileResources = new ArrayList<>();
    for (TsFileResource resource : seqResources) {
      TsFileName tsFileName = TsFileName.parse(resource.getTsFile().getName());
      tsFileName.setCrossCompactionCnt(tsFileName.getCrossCompactionCnt() + 1);
      tsFileName.setVersion(tsFileName.getVersion() + CROSS_COMPACTION_TMP_FILE_VERSION_INTERVAL);
      targetFileResources.add(
          new TsFileResource(
              new File(
                  resource.getTsFile().getParent(),
                  TsFileName.getCrossTsFileName(
                      tsFileName.getTime(),
                      tsFileName.getVersion(),
                      tsFileName.getInnerCompactionCnt(),
                      tsFileName.getCrossCompactionCnt()))));
    }
    return targetFileResources;
  }

  /**
   * Create tmp target file for inner space compaction, in which all source files has only one tmp
   * target file.
   *
   * @param tsFileResources
   * @param sequence
   * @return tmp target file, which is xxx.target
   */
  public static TsFileResource getInnerCompactionTargetFileResource(
      List<TsFileResource> tsFileResources, boolean sequence) {
    long minTime = Long.MAX_VALUE;
    long maxTime = Long.MIN_VALUE;
    long minVersion = Long.MAX_VALUE;
    long maxVersion = Long.MIN_VALUE;
    int maxInnerMergeCount = Integer.MIN_VALUE;
    int maxCrossMergeCount = Integer.MIN_VALUE;
    for (TsFileResource resource : tsFileResources) {
      TsFileName tsFileName = TsFileName.parse(resource.getTsFile().getName());
      minTime = Math.min(tsFileName.getTime(), minTime);
      maxTime = Math.max(tsFileName.getTime(), maxTime);
      minVersion = Math.min(tsFileName.getVersion(), minVersion);
      maxVersion = Math.max(tsFileName.getVersion(), maxVersion);
      maxInnerMergeCount = Math.max(tsFileName.getInnerCompactionCnt(), maxInnerMergeCount);
      maxCrossMergeCount = Math.max(tsFileName.getCrossCompactionCnt(), maxCrossMergeCount);
    }
    return sequence
        ? new TsFileResource(
            new File(
                tsFileResources.get(0).getTsFile().getParent(),
                TsFileName.getInnerTsFileName(
                    minTime, minVersion, (maxInnerMergeCount + 1), maxCrossMergeCount)))
        : new TsFileResource(
            new File(
                tsFileResources.get(0).getTsFile().getParent(),
                TsFileName.getInnerTsFileName(
                    maxTime, maxVersion, (maxInnerMergeCount + 1), maxCrossMergeCount)));
  }
}
