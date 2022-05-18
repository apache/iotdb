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
package org.apache.iotdb.db.utils.repair;

import org.apache.iotdb.commons.conf.IoTDBConstant;
import org.apache.iotdb.db.engine.storagegroup.TsFileResource;
import org.apache.iotdb.tsfile.exception.write.WriteProcessException;
import org.apache.iotdb.tsfile.fileSystem.FSFactoryProducer;
import org.apache.iotdb.tsfile.fileSystem.fsFactory.FSFactory;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import static org.apache.iotdb.tsfile.common.constant.TsFileConstant.TSFILE_SUFFIX;

/**
 * Repair corrupted seq file which has overlap data with previous files. This tool will move the
 * corrupted seq files to the corresponding unseq data dir. For example: There are seq files 1 2 3,
 * in which there is a device in file 2 and file 3 that overlap with file 1, then file 2 and file 3
 * will be thrown into the corresponding unseq directory.
 */
public class TsFileRepairTool {
  private static final Logger logger = LoggerFactory.getLogger(TsFileRepairTool.class);
  private static final FSFactory fsFactory = FSFactoryProducer.getFSFactory();
  private static String baseDataDirPath;

  public static void main(String[] args) throws WriteProcessException, IOException {
    if (!checkArgs(args)) {
      System.exit(1);
    }
    System.out.println("Start repairing...");
    // get seq data dirs
    List<String> seqDataDirs =
        new ArrayList<>(
            Arrays.asList(
                Objects.requireNonNull(
                    new File(baseDataDirPath)
                        .list((dir, name) -> (!name.equals("system") && !name.equals("wal"))))));
    for (int i = 0; i < Objects.requireNonNull(seqDataDirs).size(); i++) {
      seqDataDirs.set(
          i,
          baseDataDirPath
              + File.separator
              + seqDataDirs.get(i)
              + File.separator
              + IoTDBConstant.SEQUENCE_FLODER_NAME);
    }

    for (String seqDataPath : seqDataDirs) {
      // get sg data dirs
      File seqDataDir = new File(seqDataPath);
      if (!checkIsDirectory(seqDataDir)) {
        continue;
      }
      File[] sgDirs = seqDataDir.listFiles();
      for (File sgDir : sgDirs) {
        if (!checkIsDirectory(sgDir)) {
          continue;
        }
        System.out.println("Repair files in storage group: " + sgDir.getAbsolutePath());
        // get vsg data dirs
        File[] vsgDirs = sgDir.listFiles();
        for (File vsgDir : vsgDirs) {
          if (!checkIsDirectory(vsgDir)) {
            continue;
          }
          // get time partition dir
          File[] timePartitionDirs = vsgDir.listFiles();
          for (File timePartitionDir : timePartitionDirs) {
            if (!checkIsDirectory(timePartitionDir)) {
              continue;
            }
            // get all seq files under the time partition dir
            File[] tsFiles =
                timePartitionDir.listFiles(file -> file.getName().endsWith(TSFILE_SUFFIX));
            moveBadSeqFilesToUnseqDir(tsFiles);
          }
        }
      }
    }
    System.out.println("Finish repairing successfully!");
  }

  public static boolean checkArgs(String[] args) {
    if (args.length != 1) {
      System.out.println("Num of param should be one, which is base data dir. Eg: xxx/iotdb/data");
      return false;
    } else {
      baseDataDirPath = args[0];
      if ((baseDataDirPath.endsWith("data") || baseDataDirPath.endsWith("data" + File.separator))
          && !baseDataDirPath.endsWith("data" + File.separator + "data")) {
        return true;
      }
      System.out.println("Please input correct base data dir. Eg: xxx/iotdb/data");
      return false;
    }
  }

  private static void moveBadSeqFilesToUnseqDir(File[] tsFiles)
      throws WriteProcessException, IOException {
    // deviceID -> endTime
    Map<String, Long> deviceEndTime = new HashMap<>();
    for (File tsFile : tsFiles) {
      TsFileResource resource = new TsFileResource(tsFile);
      resource.deserialize();
      boolean hasMoved = false;
      for (String deviceID : resource.getDevices()) {
        long startTime = resource.getStartTime(deviceID);
        if (startTime <= deviceEndTime.getOrDefault(deviceID, Long.MIN_VALUE)) {
          // find the corrupted seq file which device end time is less than previous seq files.
          // move the corrupted seq file to corresponding unseq dir.
          if (hasMoved) {
            continue;
          }
          logger.info(
              "Find the corrupted file {}, move it to unseq dir.", tsFile.getAbsolutePath());
          String targetDirPath = resource.getTsFile().getParent().replace("sequence", "unsequence");
          // corrupted files, including .tsfile, .resource and .mods file
          File[] filesToBeMoved =
              fsFactory.listFilesByPrefix(
                  resource.getTsFile().getParent(), resource.getTsFile().getName());
          moveFiles(filesToBeMoved, targetDirPath);
          hasMoved = true;
        } else {
          deviceEndTime.put(deviceID, resource.getEndTime(deviceID));
        }
      }
    }
  }

  private static void moveFiles(File[] files, String targetDirPath) throws WriteProcessException {
    File targetDir = new File(targetDirPath);
    if (!targetDir.exists()) {
      targetDir.mkdir();
    } else if (!targetDir.isDirectory()) {
      throw new WriteProcessException("target dir " + targetDirPath + " is not a directory");
    }
    for (File srcFile : files) {
      File desFile = new File(targetDirPath, srcFile.getName());
      fsFactory.moveFile(srcFile, desFile);
    }
  }

  private static boolean checkIsDirectory(File dir) {
    boolean res = true;
    if (!dir.isDirectory()) {
      logger.error("{} is not a directory or does not exist, skip it.", dir.getAbsolutePath());
      res = false;
    }
    return res;
  }
}
