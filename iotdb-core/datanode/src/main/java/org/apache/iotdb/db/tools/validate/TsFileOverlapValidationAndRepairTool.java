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

import org.apache.iotdb.db.storageengine.dataregion.modification.ModificationFile;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResource;
import org.apache.iotdb.tsfile.common.constant.TsFileConstant;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Scanner;
import java.util.Set;

public class TsFileOverlapValidationAndRepairTool {

  private static final Set<File> toMoveFiles = new HashSet<>();
  private static int overlapTsFileNum = 0;
  private static int totalTsFileNum = 0;
  public static void main(String[] args) throws IOException {
    if (args.length == 0) {
      System.out.println("Please input sequence data dir path.");
      return;
    }
    String sequenceDataPath = args[0];
    File sequenceData = new File(sequenceDataPath);
    validateSequenceDataDir(sequenceData);
    if (!confirmMoveOverlapFilesToUnsequenceSpace()) {
      return;
    }
    moveOverlapFiles();
  }

  private static boolean confirmMoveOverlapFilesToUnsequenceSpace() {
    System.out.printf("Overlap tsfile num is %d, total tsfile num is %d\n", overlapTsFileNum, totalTsFileNum);
    System.out.println("Corresponding file num is " + toMoveFiles.size());
    if (overlapTsFileNum == 0) {
      return false;
    }
    System.out.println("Repair overlap tsfiles (y/n)");
    Scanner scanner = new Scanner(System.in);
    String input = scanner.nextLine();
    return "y".equals(input);
  }

  private static void moveOverlapFiles() {
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
      String moveToPath = filePath.substring(0, sequenceDirIndex) + replaceToStr + filePath.substring(sequenceDirIndex + replaceStr.length());
      File targetFile = new File(moveToPath);
      File targetParentFile = targetFile.getParentFile();
      if (targetParentFile.exists()) {
        targetParentFile.mkdirs();
      }
      boolean success = f.renameTo(new File(moveToPath));
      if (!success) {
        System.out.println("Failed to repair " + f.getAbsolutePath());
      }
    }
  }

  public static void validateSequenceDataDir(File sequenceDataDir) throws IOException {
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
          checkTimePartitionHasOverlap(timePartitionDir);
        }
      }
    }
  }

  private static void checkTimePartitionHasOverlap(File timePartitionDir) throws IOException {
    List<TsFileResource> resources = loadSortedTsFileResources(timePartitionDir);
    if (resources.isEmpty()) {
      return;
    }
    Map<String, Long> deviceEndTimeMap = new HashMap<>();
    Map<String, TsFileResource> deviceLastExistTsFileMap = new HashMap<>();
    for (TsFileResource resource : resources) {
      Set<String> devices = resource.getDevices();
      boolean fileHasOverlap = false;
      for (String device : devices) {
        long deviceStartTimeInCurrentFile = resource.getStartTime(device);
        long deviceEndTimeInCurrentFile = resource.getEndTime(device);
        if (!deviceLastExistTsFileMap.containsKey(device)) {
          deviceEndTimeMap.put(device, deviceEndTimeInCurrentFile);
          deviceLastExistTsFileMap.put(device, resource);
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
          if (!fileHasOverlap) {
            recordOverlapTsFile(resource);
            fileHasOverlap = true;
          }
        }
        if (deviceEndTimeInCurrentFile > deviceEndTimeInPreviousFile) {
          deviceEndTimeMap.put(device, deviceEndTimeInCurrentFile);
        }
        deviceLastExistTsFileMap.put(device, resource);
      }
    }
  }

  private static void recordOverlapTsFile(TsFileResource overlapFile) {
    String filePath = overlapFile.getTsFilePath();
    toMoveFiles.add(overlapFile.getTsFile());
    toMoveFiles.add(new File(filePath + TsFileResource.RESOURCE_SUFFIX));
    ModificationFile modsFile = overlapFile.getModFile();
    if (modsFile.exists()) {
      toMoveFiles.add(new File(modsFile.getFilePath()));
    }
    overlapTsFileNum++;
  }

  public static List<TsFileResource> loadSortedTsFileResources(File timePartitionDir) throws IOException {
    List<TsFileResource> resources = new ArrayList<>();
    for (File tsfile : Objects.requireNonNull(timePartitionDir.listFiles())) {
      if (!tsfile.getAbsolutePath().endsWith(TsFileConstant.TSFILE_SUFFIX) || !tsfile.isFile()) {
        continue;
      }
      String resourcePath = tsfile.getAbsolutePath() + TsFileResource.RESOURCE_SUFFIX;
      if (!new File(resourcePath).exists()) {
        System.out.println(tsfile.getAbsolutePath() + " is skipped because resource file is not exist.");
        continue;
      }
      TsFileResource resource = new TsFileResource();
      resource.setFile(tsfile);
      resource.deserialize();
      resource.close();
      resources.add(resource);
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
