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

import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.utils.log.CompactionLogger;
import org.apache.iotdb.db.storageengine.dataregion.modification.ModificationFile;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResource;
import org.apache.iotdb.tsfile.common.constant.TsFileConstant;

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

  private static final Set<File> toMoveFiles = new HashSet<>();
  private static final List<File> partitionDirsWhichHaveOverlapFiles = new ArrayList<>();
  private static int overlapTsFileNum = 0;
  private static int totalTsFileNum = 0;

  public static void main(String[] args) throws IOException {
    if (args.length == 0) {
      System.out.println("Please input sequence data dir path.");
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
    System.out.println("TimePartitions which have overlap files:");
    for (File partitionDirsWhichHaveOverlapFile : partitionDirsWhichHaveOverlapFiles) {
      System.out.println(partitionDirsWhichHaveOverlapFile.getAbsolutePath());
    }
    System.out.println();

    System.out.printf(
        "Overlap tsfile num is %d, total tsfile num is %d\n", overlapTsFileNum, totalTsFileNum);
    System.out.println("Corresponding file num is " + toMoveFiles.size());
    if (overlapTsFileNum == 0) {
      return false;
    }
    System.out.println("Repair overlap tsfiles (y/n)");
    Scanner scanner = new Scanner(System.in);
    String input = scanner.nextLine();
    return "y".equals(input);
  }

  private static void moveOverlapFilesToUnsequenceSpace(Set<File> toMoveFiles) {
    for (File f : toMoveFiles) {
      if (!f.exists()) {
        System.out.println(f.getAbsolutePath() + "is not exist in repairing");
        continue;
      }
      String filePath = f.getAbsolutePath();
      String replaceStr = File.separator + "sequence" + File.separator;
      String replaceToStr = File.separator + "unsequence" + File.separator;
      int sequenceDirIndex = filePath.indexOf(replaceStr);
      if (sequenceDirIndex == -1) {
        continue;
      }
      String moveToPath =
          filePath.substring(0, sequenceDirIndex)
              + replaceToStr
              + filePath.substring(sequenceDirIndex + replaceStr.length());
      File targetFile = new File(moveToPath);
      File targetParentFile = targetFile.getParentFile();
      if (targetParentFile.exists()) {
        targetParentFile.mkdirs();
      }
      boolean success = f.renameTo(targetFile);
      if (!success) {
        System.out.println("Failed to repair " + f.getAbsolutePath());
      }
      System.out.println("Repair file " + targetFile.getName());
    }
  }

  private static void validateSequenceDataDirs(List<String> sequenceDataDirPaths)
      throws IOException {
    Map<String, List<File>> partitionMap = new HashMap<>();
    for (String sequenceDataDirPath : sequenceDataDirPaths) {
      File sequenceDataDir = new File(sequenceDataDirPath);
      if (!sequenceDataDir.exists() || sequenceDataDir.isFile()) {
        System.out.println(sequenceDataDir.getAbsolutePath() + " is not a correct path");
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
          "TimePartition " + partitionName + " has overlap file, dir is " + partition.getValue());
      partitionDirsWhichHaveOverlapFiles.addAll(partition.getValue());
      overlapTsFileNum += overlapTsFileNumInCurrentTimePartition;
    }
  }

  private static String calculateTimePartitionKey(
      String storageGroup, String dataRegion, String timePartition) {
    return storageGroup + "-" + dataRegion + "-" + timePartition;
  }

  public static int checkTimePartitionHasOverlap(List<TsFileResource> resources) {
    int overlapTsFileNum = 0;
    Map<String, Long> deviceEndTimeMap = new HashMap<>();
    Map<String, TsFileResource> deviceLastExistTsFileMap = new HashMap<>();
    for (TsFileResource resource : resources) {
      Set<String> devices = resource.getDevices();
      boolean fileHasOverlap = false;
      // check overlap
      for (String device : devices) {
        long deviceStartTimeInCurrentFile = resource.getStartTime(device);
        if (deviceStartTimeInCurrentFile > resource.getEndTime(device)) {
          continue;
        }
        if (!deviceEndTimeMap.containsKey(device)) {
          continue;
        }
        long deviceEndTimeInPreviousFile = deviceEndTimeMap.get(device);
        if (deviceStartTimeInCurrentFile <= deviceEndTimeInPreviousFile) {
          System.out.printf(
              "previous file: %s, current file: %s, device %s in previous file end time is %d,"
                  + " device in current file start time is %d\n",
              deviceLastExistTsFileMap.get(device).getTsFilePath(),
              resource.getTsFilePath(),
              device,
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
        for (String device : devices) {
          long deviceEndTimeInCurrentFile = resource.getEndTime(device);
          if (!deviceLastExistTsFileMap.containsKey(device)) {
            deviceEndTimeMap.put(device, deviceEndTimeInCurrentFile);
            deviceLastExistTsFileMap.put(device, resource);
            continue;
          }
          deviceEndTimeMap.put(device, resource.getEndTime(device));
          deviceLastExistTsFileMap.put(device, resource);
        }
      }
    }
    return overlapTsFileNum;
  }

  private static void recordOverlapTsFile(TsFileResource overlapFile) {
    String filePath = overlapFile.getTsFilePath();
    toMoveFiles.add(overlapFile.getTsFile());
    toMoveFiles.add(new File(filePath + TsFileResource.RESOURCE_SUFFIX));
    ModificationFile modsFile = overlapFile.getModFile();
    if (modsFile.exists()) {
      toMoveFiles.add(new File(modsFile.getFilePath()));
    }
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
              "Time partition "
                  + timePartitionDir.getName()
                  + " is skipped because a compaction is not finished");
          return Collections.emptyList();
        }

        if (!filePath.endsWith(TsFileConstant.TSFILE_SUFFIX) || !tsfile.isFile()) {
          continue;
        }
        String resourcePath = tsfile.getAbsolutePath() + TsFileResource.RESOURCE_SUFFIX;

        if (!new File(resourcePath).exists()) {
          System.out.println(
              tsfile.getAbsolutePath() + " is skipped because resource file is not exist.");
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
