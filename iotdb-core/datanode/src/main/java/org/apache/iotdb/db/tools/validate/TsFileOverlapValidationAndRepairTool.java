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

package org.apache.iotdb.db.tools.validate;

import org.apache.iotdb.db.i18n.DataNodeMiscMessages;
import org.apache.iotdb.db.i18n.StorageEngineMessages;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.utils.log.CompactionLogger;
import org.apache.iotdb.db.storageengine.dataregion.modification.ModificationFile;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResource;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.generator.TsFileNameGenerator;

import org.apache.tsfile.common.constant.TsFileConstant;
import org.apache.tsfile.file.metadata.IDeviceID;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Scanner;
import java.util.Set;

public class TsFileOverlapValidationAndRepairTool {

  private static final Set<TsFileResource> toMoveFiles = new HashSet<>();
  private static final List<File> partitionDirsWhichHaveOverlapFiles = new ArrayList<>();
  private static int overlapTsFileNum = 0;
  private static int totalTsFileNum = 0;

  public static void main(String[] args) throws IOException {
    if (args.length == 0) {
      System.out.println(DataNodeMiscMessages.LOG_PLEASE_INPUT_SEQUENCE_DATA_DIR_PATH_6B8E0450);
      return;
    }
    List<String> sequenceDataDirs = getDataDirs(args);
    validateSequenceDataDirs(sequenceDataDirs);
    if (!confirmMoveOverlapFilesToUnsequenceSpace()) {
      return;
    }
    moveOverlapFilesToUnsequenceSpace(toMoveFiles);
  }

  private static List<String> getDataDirs(String[] args) {
    return Arrays.asList(args);
  }

  private static boolean confirmMoveOverlapFilesToUnsequenceSpace() {
    System.out.println(DataNodeMiscMessages.LOG_TIMEPARTITIONS_WHICH_HAVE_OVERLAP_FILES_B50DEB6C);
    for (File partitionDirsWhichHaveOverlapFile : partitionDirsWhichHaveOverlapFiles) {
      System.out.println(partitionDirsWhichHaveOverlapFile.getAbsolutePath());
    }
    System.out.println();

    System.out.printf(
        DataNodeMiscMessages.LOG_OVERLAP_TSFILE_NUM_IS_ARG_TOTAL_TSFILE_NUM_IS_ARG_892F7267,
        overlapTsFileNum,
        totalTsFileNum);
    if (overlapTsFileNum == 0) {
      return false;
    }
    System.out.println(DataNodeMiscMessages.LOG_REPAIR_OVERLAP_TSFILES_Y_N_D8587D19);
    Scanner scanner = new Scanner(System.in);
    String input = scanner.nextLine();
    return "y".equals(input);
  }

  private static void moveOverlapFilesToUnsequenceSpace(Set<TsFileResource> toMoveResources)
      throws IOException {
    for (TsFileResource resource : toMoveResources) {
      moveSeqResourceToUnsequenceDir(resource);
    }
  }

  private static void moveSeqResourceToUnsequenceDir(TsFileResource resource) throws IOException {
    if (!resource.tsFileExists()) {
      System.out.println(
          String.format(
              DataNodeMiscMessages.LOG_ARG_DOES_NOT_EXIST_WHEN_REPAIRING_763A5B2F,
              Objects.toString(resource.getTsFile().getAbsolutePath())));
      return;
    }
    String dirPath = resource.getTsFile().getParentFile().getAbsolutePath();
    String replaceStr = File.separator + "sequence" + File.separator;
    String replaceToStr = File.separator + "unsequence" + File.separator;
    int sequenceDirIndex = dirPath.indexOf(replaceStr);
    if (sequenceDirIndex == -1) {
      return;
    }
    String moveToDir =
        dirPath.substring(0, sequenceDirIndex)
            + replaceToStr
            + dirPath.substring(sequenceDirIndex + replaceStr.length());
    File targetDir = new File(moveToDir);
    if (!targetDir.exists()) {
      targetDir.mkdirs();
    }

    File tsfile = resource.getTsFile();
    File targetFile;
    TsFileNameGenerator.TsFileName tsFileName = TsFileNameGenerator.getTsFileName(tsfile.getName());
    do {
      String fileNameStr =
          String.format(
              "%d-%d-%d-%d" + TsFileConstant.TSFILE_SUFFIX,
              tsFileName.getTime(),
              0,
              tsFileName.getInnerCompactionCnt(),
              0);
      targetFile = new File(targetDir.getAbsolutePath() + File.separator + fileNameStr);
      if (!targetFile.exists()) {
        break;
      }
      if (tsFileName.getTime() == Long.MAX_VALUE) {
        throw new IOException(
            String.format(
                StorageEngineMessages
                    .EXCEPTION_CANNOT_REPAIR_ARG_BECAUSE_TARGET_FILE_ALREADY_EXISTS_AND_TIMESTAMP_IS_LONG_MAX_VALUE_F29F630A,
                tsfile.getAbsolutePath()));
      }
      tsFileName.setTime(tsFileName.getTime() + 1);
    } while (true);

    moveFile(tsfile, targetFile);
    moveFile(
        new File(tsfile.getAbsolutePath() + TsFileResource.RESOURCE_SUFFIX),
        new File(targetFile.getAbsolutePath() + TsFileResource.RESOURCE_SUFFIX));
    if (resource.exclusiveModFileExists()) {
      moveFile(
          ModificationFile.getExclusiveMods(tsfile), ModificationFile.getExclusiveMods(targetFile));
    }
  }

  private static void moveFile(File from, File to) {
    boolean success = from.renameTo(to);
    if (!success) {
      System.out.println(
          String.format(
              DataNodeMiscMessages.LOG_FAILED_TO_REPAIR_ARG_6522A1E2,
              Objects.toString(from.getAbsolutePath())));
    }
    System.out.println(
        String.format(
            DataNodeMiscMessages.LOG_REPAIR_FILE_ARG_BEFC61E6, Objects.toString(from.getName())));
  }

  private static void validateSequenceDataDirs(List<String> sequenceDataDirPaths)
      throws IOException {
    Map<String, List<File>> partitionMap = new HashMap<>();
    for (String sequenceDataDirPath : sequenceDataDirPaths) {
      File sequenceDataDir = new File(sequenceDataDirPath);
      if (!sequenceDataDir.exists()
          || sequenceDataDir.isFile()
          || !sequenceDataDir.getName().equals("sequence")) {
        System.out.println(
            String.format(
                DataNodeMiscMessages.LOG_ARG_IS_NOT_A_CORRECT_PATH_F2B497AA,
                Objects.toString(sequenceDataDir.getAbsolutePath())));
        continue;
      }
      for (File sg : Objects.requireNonNull(sequenceDataDir.listFiles())) {
        if (!sg.isDirectory()) {
          continue;
        }
        for (File dataRegionDir : Objects.requireNonNull(sg.listFiles())) {
          if (!dataRegionDir.isDirectory()) {
            continue;
          }
          for (File timePartitionDir : Objects.requireNonNull(dataRegionDir.listFiles())) {
            if (!timePartitionDir.isDirectory()) {
              continue;
            }
            String partitionKey =
                calculateTimePartitionKey(
                    sg.getName(), dataRegionDir.getName(), timePartitionDir.getName());
            List<File> partitionDirs =
                partitionMap.computeIfAbsent(partitionKey, v -> new ArrayList<>());
            partitionDirs.add(timePartitionDir);
          }
        }
      }
    }

    for (Map.Entry<String, List<File>> partition : partitionMap.entrySet()) {
      String partitionName = partition.getKey();
      List<TsFileResource> resources = loadSortedTsFileResources(partition.getValue());
      if (resources.isEmpty()) {
        continue;
      }
      int overlapTsFileNumInCurrentTimePartition = checkTimePartitionHasOverlap(resources);
      if (overlapTsFileNumInCurrentTimePartition == 0) {
        continue;
      }
      System.out.println(
          String.format(
              DataNodeMiscMessages.LOG_TIMEPARTITION_ARG_HAS_OVERLAP_FILE_DIR_IS_ARG_3F69AA70,
              Objects.toString(partitionName),
              Objects.toString(partition.getValue())));
      partitionDirsWhichHaveOverlapFiles.addAll(partition.getValue());
      overlapTsFileNum += overlapTsFileNumInCurrentTimePartition;
    }
  }

  private static String calculateTimePartitionKey(
      String storageGroup, String dataRegion, String timePartition) {
    return storageGroup + "-" + dataRegion + "-" + timePartition;
  }

  @SuppressWarnings("OptionalGetWithoutIsPresent")
  public static int checkTimePartitionHasOverlap(List<TsFileResource> resources) {
    int overlapTsFileNum = 0;
    Map<IDeviceID, Long> deviceEndTimeMap = new HashMap<>();
    Map<IDeviceID, TsFileResource> deviceLastExistTsFileMap = new HashMap<>();
    for (TsFileResource resource : resources) {
      Set<IDeviceID> devices = resource.getDevices();
      boolean fileHasOverlap = false;
      // check overlap
      for (IDeviceID device : devices) {
        // iterating the index, must present
        long deviceStartTimeInCurrentFile = resource.getStartTime(device).get();
        if (deviceStartTimeInCurrentFile > resource.getEndTime(device).get()) {
          continue;
        }
        if (!deviceEndTimeMap.containsKey(device)) {
          continue;
        }
        long deviceEndTimeInPreviousFile = deviceEndTimeMap.get(device);
        if (deviceStartTimeInCurrentFile <= deviceEndTimeInPreviousFile) {
          System.out.printf(
              DataNodeMiscMessages
                  .LOG_PREVIOUS_FILE_ARG_CURRENT_FILE_ARG_DEVICE_ARG_IN_PREVIOUS_FILE_END_TIME_IS_ARG_DEVICE_IN_CURRENT_FILE_START_TIME_IS_ARG_6AB6F44C,
              deviceLastExistTsFileMap.get(device).getTsFilePath(),
              resource.getTsFilePath(),
              device.toString(),
              deviceEndTimeInPreviousFile,
              deviceStartTimeInCurrentFile);
          fileHasOverlap = true;
          recordOverlapTsFile(resource);
          overlapTsFileNum++;
          break;
        }
      }
      // update end time map
      if (!fileHasOverlap) {
        for (IDeviceID device : devices) {
          deviceEndTimeMap.put(device, resource.getEndTime(device).get());
          deviceLastExistTsFileMap.put(device, resource);
        }
      }
    }
    return overlapTsFileNum;
  }

  private static void recordOverlapTsFile(TsFileResource overlapFile) {
    toMoveFiles.add(overlapFile);
  }

  private static List<TsFileResource> loadSortedTsFileResources(List<File> timePartitionDirs)
      throws IOException {
    List<TsFileResource> resources = new ArrayList<>();
    for (File timePartitionDir : timePartitionDirs) {
      for (File tsfile : Objects.requireNonNull(timePartitionDir.listFiles())) {
        String filePath = tsfile.getAbsolutePath();
        // has compaction log
        if (filePath.endsWith(CompactionLogger.INNER_COMPACTION_LOG_NAME_SUFFIX)
            || filePath.endsWith(CompactionLogger.CROSS_COMPACTION_LOG_NAME_SUFFIX)) {
          System.out.println(
              String.format(
                  DataNodeMiscMessages
                      .LOG_TIME_PARTITION_ARG_IS_SKIPPED_BECAUSE_A_COMPACTION_IS_NOT_FINISHED_4A775D07,
                  Objects.toString(timePartitionDir.getName())));
          return Collections.emptyList();
        }

        if (!filePath.endsWith(TsFileConstant.TSFILE_SUFFIX) || !tsfile.isFile()) {
          continue;
        }
        String resourcePath = tsfile.getAbsolutePath() + TsFileResource.RESOURCE_SUFFIX;

        if (!new File(resourcePath).exists()) {
          System.out.println(
              String.format(
                  DataNodeMiscMessages
                      .LOG_ARG_IS_SKIPPED_BECAUSE_RESOURCE_FILE_IS_NOT_EXIST_24044E7F,
                  Objects.toString(tsfile.getAbsolutePath())));
          continue;
        }

        TsFileResource resource = new TsFileResource(tsfile);
        resource.deserialize();
        resource.close();
        resources.add(resource);
      }
    }
    resources.sort(
        (f1, f2) -> {
          int timeDiff =
              Long.compareUnsigned(
                  Long.parseLong(f1.getTsFile().getName().split("-")[0]),
                  Long.parseLong(f2.getTsFile().getName().split("-")[0]));
          return timeDiff == 0
              ? Long.compareUnsigned(
                  Long.parseLong(f1.getTsFile().getName().split("-")[1]),
                  Long.parseLong(f2.getTsFile().getName().split("-")[1]))
              : timeDiff;
        });

    totalTsFileNum += resources.size();
    return resources;
  }
}
