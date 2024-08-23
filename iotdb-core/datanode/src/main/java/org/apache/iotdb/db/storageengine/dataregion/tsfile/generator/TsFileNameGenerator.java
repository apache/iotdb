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

package org.apache.iotdb.db.storageengine.dataregion.tsfile.generator;

import org.apache.iotdb.commons.conf.IoTDBConstant;
import org.apache.iotdb.commons.utils.TestOnly;
import org.apache.iotdb.db.exception.DiskSpaceInsufficientException;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResource;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResourceStatus;
import org.apache.iotdb.db.storageengine.rescon.disk.TierManager;

import org.apache.tsfile.common.constant.TsFileConstant;
import org.apache.tsfile.fileSystem.FSFactoryProducer;
import org.apache.tsfile.fileSystem.fsFactory.FSFactory;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.apache.iotdb.commons.conf.IoTDBConstant.FILE_NAME_SEPARATOR;
import static org.apache.tsfile.common.constant.TsFileConstant.TSFILE_SUFFIX;

public class TsFileNameGenerator {

  private static FSFactory fsFactory = FSFactoryProducer.getFSFactory();

  public static String generateNewTsFilePath(
      String tsFileDir,
      long time,
      long version,
      int innerSpaceCompactionCount,
      int crossSpaceCompactionCount) {
    return tsFileDir
        + File.separator
        + generateNewTsFileName(
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
    return generateNewTsFilePathWithMkdir(
        sequence,
        logicalStorageGroup,
        virtualStorageGroup,
        timePartitionId,
        time,
        version,
        innerSpaceCompactionCount,
        crossSpaceCompactionCount,
        0,
        TsFileConstant.TSFILE_SUFFIX);
  }

  public static String generateNewTsFilePathWithMkdir(
      boolean sequence,
      String logicalStorageGroup,
      String virtualStorageGroup,
      long timePartitionId,
      long time,
      long version,
      int innerSpaceCompactionCount,
      int crossSpaceCompactionCount,
      int tierLevel,
      String customSuffix)
      throws DiskSpaceInsufficientException {
    String tsFileDir =
        generateTsFileDir(
            sequence, logicalStorageGroup, virtualStorageGroup, timePartitionId, tierLevel);
    fsFactory.getFile(tsFileDir).mkdirs();
    return tsFileDir
        + File.separator
        + generateNewTsFileName(
            time, version, innerSpaceCompactionCount, crossSpaceCompactionCount, customSuffix);
  }

  public static String generateTsFileDir(
      boolean sequence,
      String logicalStorageGroup,
      String virtualStorageGroup,
      long timePartitionId,
      int tierLevel)
      throws DiskSpaceInsufficientException {
    TierManager tierManager = TierManager.getInstance();
    String baseDir = tierManager.getNextFolderForTsFile(tierLevel, sequence);
    return baseDir
        + File.separator
        + logicalStorageGroup
        + File.separator
        + virtualStorageGroup
        + File.separator
        + timePartitionId;
  }

  public static String generateNewTsFileName(
      long time, long version, int innerSpaceCompactionCount, int crossSpaceCompactionCount) {
    return generateNewTsFileName(
        time, version, innerSpaceCompactionCount, crossSpaceCompactionCount, TSFILE_SUFFIX);
  }

  public static String generateNewTsFileName(
      long time,
      long version,
      int innerSpaceCompactionCount,
      int crossSpaceCompactionCount,
      String customSuffix) {
    return time
        + IoTDBConstant.FILE_NAME_SEPARATOR
        + version
        + IoTDBConstant.FILE_NAME_SEPARATOR
        + innerSpaceCompactionCount
        + IoTDBConstant.FILE_NAME_SEPARATOR
        + crossSpaceCompactionCount
        + customSuffix;
  }

  public static TsFileName getTsFileName(String fileName) throws IOException {
    Matcher matcher = TsFileName.FILE_NAME_MATCHER.matcher(fileName);
    if (matcher.find()) {
      try {
        TsFileName tsFileName =
            new TsFileName(
                Long.parseLong(matcher.group(1)),
                Long.parseLong(matcher.group(2)),
                Integer.parseInt(matcher.group(3)),
                Integer.parseInt(matcher.group(4)));
        return tsFileName;
      } catch (NumberFormatException e) {
        throw new IOException("tsfile file name format is incorrect:" + fileName);
      }
    } else {
      throw new IOException("tsfile file name format is incorrect:" + fileName);
    }
  }

  @TestOnly
  public static TsFileResource increaseCrossCompactionCnt(TsFileResource tsFileResource)
      throws IOException {
    File tsFile = tsFileResource.getTsFile();
    String path = tsFile.getParent();
    TsFileName tsFileName = getTsFileName(tsFileResource.getTsFile().getName());
    tsFileName.setCrossCompactionCnt(tsFileName.getCrossCompactionCnt() + 1);
    tsFileResource.setFile(
        new File(
            path,
            tsFileName.time
                + FILE_NAME_SEPARATOR
                + tsFileName.version
                + FILE_NAME_SEPARATOR
                + tsFileName.innerCompactionCnt
                + FILE_NAME_SEPARATOR
                + tsFileName.crossCompactionCnt
                + TSFILE_SUFFIX));
    return tsFileResource;
  }

  @TestOnly
  public static TsFileResource increaseInnerCompactionCnt(TsFileResource tsFileResource)
      throws IOException {
    File tsFile = tsFileResource.getTsFile();
    String path = tsFile.getParent();
    TsFileName tsFileName = getTsFileName(tsFileResource.getTsFile().getName());
    tsFileName.setInnerCompactionCnt(tsFileName.getInnerCompactionCnt() + 1);
    tsFileResource.setFile(
        new File(
            path,
            tsFileName.time
                + FILE_NAME_SEPARATOR
                + tsFileName.version
                + FILE_NAME_SEPARATOR
                + tsFileName.innerCompactionCnt
                + FILE_NAME_SEPARATOR
                + tsFileName.crossCompactionCnt
                + TSFILE_SUFFIX));
    return tsFileResource;
  }

  public static File increaseCrossCompactionCnt(File tsFile) throws IOException {
    String path = tsFile.getParent();
    TsFileName tsFileName = getTsFileName(tsFile.getName());
    tsFileName.setCrossCompactionCnt(tsFileName.getCrossCompactionCnt() + 1);
    return new File(
        path,
        tsFileName.time
            + FILE_NAME_SEPARATOR
            + tsFileName.version
            + FILE_NAME_SEPARATOR
            + tsFileName.innerCompactionCnt
            + FILE_NAME_SEPARATOR
            + tsFileName.crossCompactionCnt
            + TSFILE_SUFFIX);
  }

  /**
   * Create tmp target file for cross space compaction, in which each sequence source file has its
   * own tmp target file.
   *
   * @param seqResources
   * @return tmp target file list, which is xxx.cross
   * @throws IOException
   */
  public static List<TsFileResource> getCrossCompactionTargetFileResources(
      List<TsFileResource> seqResources) throws IOException, DiskSpaceInsufficientException {
    List<TsFileResource> targetFileResources = new ArrayList<>();
    for (TsFileResource resource : seqResources) {
      TsFileName tsFileName = getTsFileName(resource.getTsFile().getName());
      tsFileName.setCrossCompactionCnt(tsFileName.getCrossCompactionCnt() + 1);
      // set target resource to COMPACTING until the end of this task
      targetFileResources.add(
          new TsFileResource(
              new File(
                  generateNewTsFilePathWithMkdir(
                      resource.isSeq(),
                      resource.getDatabaseName(),
                      resource.getDataRegionId(),
                      resource.getTimePartition(),
                      tsFileName.time,
                      tsFileName.version,
                      tsFileName.innerCompactionCnt,
                      tsFileName.crossCompactionCnt,
                      resource.getTierLevel(),
                      IoTDBConstant.CROSS_COMPACTION_TMP_FILE_SUFFIX)),
              TsFileResourceStatus.COMPACTING));
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
   * @throws IOException
   */
  public static TsFileResource getInnerCompactionTargetFileResource(
      List<TsFileResource> tsFileResources, boolean sequence)
      throws IOException, DiskSpaceInsufficientException {
    long minTime = Long.MAX_VALUE;
    long maxTime = Long.MIN_VALUE;
    long minVersion = Long.MAX_VALUE;
    long maxVersion = Long.MIN_VALUE;
    long maxInnerMergeCount = Long.MIN_VALUE;
    long maxCrossMergeCount = Long.MIN_VALUE;
    int maxTierLevel = 0;
    for (TsFileResource resource : tsFileResources) {
      TsFileName tsFileName = getTsFileName(resource.getTsFile().getName());
      minTime = Math.min(tsFileName.time, minTime);
      maxTime = Math.max(tsFileName.time, maxTime);
      minVersion = Math.min(tsFileName.version, minVersion);
      maxVersion = Math.max(tsFileName.version, maxVersion);
      maxInnerMergeCount = Math.max(tsFileName.innerCompactionCnt, maxInnerMergeCount);
      maxCrossMergeCount = Math.max(tsFileName.crossCompactionCnt, maxCrossMergeCount);
      maxTierLevel = Math.max(resource.getTierLevel(), maxTierLevel);
    }
    // set target resource to COMPACTING until the end of this task
    TsFileResource resource =
        sequence
            ? new TsFileResource(
                new File(
                    generateNewTsFilePathWithMkdir(
                        sequence,
                        tsFileResources.get(0).getDatabaseName(),
                        tsFileResources.get(0).getDataRegionId(),
                        tsFileResources.get(0).getTimePartition(),
                        minTime,
                        minVersion,
                        (int) maxInnerMergeCount + 1,
                        (int) maxCrossMergeCount,
                        maxTierLevel,
                        IoTDBConstant.INNER_COMPACTION_TMP_FILE_SUFFIX)),
                TsFileResourceStatus.COMPACTING)
            : new TsFileResource(
                new File(
                    generateNewTsFilePathWithMkdir(
                        sequence,
                        tsFileResources.get(0).getDatabaseName(),
                        tsFileResources.get(0).getDataRegionId(),
                        tsFileResources.get(0).getTimePartition(),
                        maxTime,
                        maxVersion,
                        (int) maxInnerMergeCount + 1,
                        (int) maxCrossMergeCount,
                        maxTierLevel,
                        IoTDBConstant.INNER_COMPACTION_TMP_FILE_SUFFIX)),
                TsFileResourceStatus.COMPACTING);
    resource.setSeq(sequence);
    return resource;
  }

  public static List<TsFileResource> getNewInnerCompactionTargetFileResources(
      List<TsFileResource> tsFileResources, boolean sequence)
      throws IOException, DiskSpaceInsufficientException {
    long maxCrossMergeCount = Long.MIN_VALUE;
    int maxTierLevel = 0;
    for (TsFileResource resource : tsFileResources) {
      TsFileName tsFileName = getTsFileName(resource.getTsFile().getName());
      maxCrossMergeCount = Math.max(tsFileName.crossCompactionCnt, maxCrossMergeCount);
      maxTierLevel = Math.max(resource.getTierLevel(), maxTierLevel);
    }
    List<TsFileResource> targetResources = new ArrayList<>(tsFileResources.size());
    for (TsFileResource resource : tsFileResources) {
      TsFileName tsFileName = getTsFileName(resource.getTsFile().getName());
      TsFileResource targetResource =
          new TsFileResource(
              new File(
                  generateNewTsFilePathWithMkdir(
                      sequence,
                      tsFileResources.get(0).getDatabaseName(),
                      tsFileResources.get(0).getDataRegionId(),
                      tsFileResources.get(0).getTimePartition(),
                      tsFileName.time,
                      tsFileName.version,
                      tsFileName.innerCompactionCnt + 1,
                      (int) maxCrossMergeCount,
                      maxTierLevel,
                      IoTDBConstant.INNER_COMPACTION_TMP_FILE_SUFFIX)),
              TsFileResourceStatus.COMPACTING);
      targetResource.setSeq(sequence);
      targetResources.add(targetResource);
    }
    return targetResources;
  }

  public static TsFileResource getSettleCompactionTargetFileResources(
      List<TsFileResource> tsFileResources, boolean sequence) throws IOException {
    long minTime = Long.MAX_VALUE;
    long maxTime = Long.MIN_VALUE;
    long minVersion = Long.MAX_VALUE;
    long maxVersion = Long.MIN_VALUE;
    long maxInnerMergeCount = Long.MIN_VALUE;
    long maxCrossMergeCount = Long.MIN_VALUE;
    for (TsFileResource resource : tsFileResources) {
      TsFileName tsFileName = getTsFileName(resource.getTsFile().getName());
      minTime = Math.min(tsFileName.time, minTime);
      maxTime = Math.max(tsFileName.time, maxTime);
      minVersion = Math.min(tsFileName.version, minVersion);
      maxVersion = Math.max(tsFileName.version, maxVersion);
      maxInnerMergeCount = Math.max(tsFileName.innerCompactionCnt, maxInnerMergeCount);
      maxCrossMergeCount = Math.max(tsFileName.crossCompactionCnt, maxCrossMergeCount);
    }
    // set target resource to COMPACTING until the end of this task
    return sequence
        ? new TsFileResource(
            new File(
                tsFileResources.get(0).getTsFile().getParent(),
                minTime
                    + FILE_NAME_SEPARATOR
                    + minVersion
                    + FILE_NAME_SEPARATOR
                    + (maxInnerMergeCount + 1)
                    + FILE_NAME_SEPARATOR
                    + maxCrossMergeCount
                    + IoTDBConstant.SETTLE_SUFFIX),
            TsFileResourceStatus.COMPACTING)
        : new TsFileResource(
            new File(
                tsFileResources.get(0).getTsFile().getParent(),
                maxTime
                    + FILE_NAME_SEPARATOR
                    + maxVersion
                    + FILE_NAME_SEPARATOR
                    + (maxInnerMergeCount + 1)
                    + FILE_NAME_SEPARATOR
                    + maxCrossMergeCount
                    + IoTDBConstant.SETTLE_SUFFIX),
            TsFileResourceStatus.COMPACTING);
  }

  public static class TsFileName {
    private static final String FILE_NAME_PATTERN = "(\\d+)-(\\d+)-(\\d+)-(\\d+).tsfile$";
    private static final Pattern FILE_NAME_MATCHER = Pattern.compile(TsFileName.FILE_NAME_PATTERN);

    private long time;
    private long version;
    private int innerCompactionCnt;
    private int crossCompactionCnt;

    public TsFileName(long time, long version, int innerCompactionCnt, int crossCompactionCnt) {
      this.time = time;
      this.version = version;
      this.innerCompactionCnt = innerCompactionCnt;
      this.crossCompactionCnt = crossCompactionCnt;
    }

    public long getTime() {
      return time;
    }

    public long getVersion() {
      return version;
    }

    public int getInnerCompactionCnt() {
      return innerCompactionCnt;
    }

    public int getCrossCompactionCnt() {
      return crossCompactionCnt;
    }

    public void setTime(long time) {
      this.time = time;
    }

    public void setVersion(long version) {
      this.version = version;
    }

    public void setInnerCompactionCnt(int innerCompactionCnt) {
      this.innerCompactionCnt = innerCompactionCnt;
    }

    public void setCrossCompactionCnt(int crossCompactionCnt) {
      this.crossCompactionCnt = crossCompactionCnt;
    }
  }
}
