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

package org.apache.iotdb.db.tools.settle;

import org.apache.iotdb.db.engine.settle.SettleLog;
import org.apache.iotdb.db.engine.settle.SettleLog.SettleCheckStatus;
import org.apache.iotdb.db.engine.storagegroup.TsFileResource;
import org.apache.iotdb.db.tools.TsFileRewriteTool;
import org.apache.iotdb.tsfile.fileSystem.FSFactoryProducer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.iotdb.tsfile.common.constant.TsFileConstant.TSFILE_SUFFIX;

/**
 * Offline Settle tool, which is used to settle TsFile and its corresponding mods file to a new
 * TsFile.
 */
public class TsFileAndModSettleTool {
  private static final Logger logger = LoggerFactory.getLogger(TsFileAndModSettleTool.class);
  // TsFilePath -> SettleCheckStatus
  public static Map<String, Integer> recoverSettleFileMap = new HashMap<>();

  public static void main(String[] args) {
    Map<String, TsFileResource> oldTsFileResources = new HashMap<>();
    // List<TsFileResource> oldTsFileResources = new ArrayList<>();
    findFilesToBeRecovered();
    for (Map.Entry<String, Integer> entry : recoverSettleFileMap.entrySet()) {
      String path = entry.getKey();
      TsFileResource resource = new TsFileResource(new File(path));
      resource.setClosed(true);
      oldTsFileResources.put(resource.getTsFile().getName(), resource);
    }
    List<File> tsFiles = checkArgs(args);
    if (tsFiles != null) {
      for (File file : tsFiles) {
        if (!oldTsFileResources.containsKey(file.getName())) {
          if (new File(file + TsFileResource.RESOURCE_SUFFIX).exists()) {
            TsFileResource resource = new TsFileResource(file);
            resource.setClosed(true);
            oldTsFileResources.put(file.getName(), resource);
          }
        }
      }
    }
    System.out.println(
        "Totally find "
            + oldTsFileResources.size()
            + " tsFiles to be settled, including "
            + recoverSettleFileMap.size()
            + " tsFiles to be recovered.");
    settleTsFilesAndMods(oldTsFileResources);
  }

  public static List<File> checkArgs(String[] args) {
    String filePath = "test.tsfile";
    List<File> files = new ArrayList<>();
    if (args.length == 0) {
      return null;
    } else {
      for (String arg : args) {
        if (arg.endsWith(TSFILE_SUFFIX)) { // it's a file
          File f = new File(arg);
          if (!f.exists()) {
            logger.warn("Cannot find TsFile : " + arg);
            continue;
          }
          files.add(f);
        } else { // it's a dir
          List<File> tmpFiles = getAllFilesInOneDirBySuffix(arg, TSFILE_SUFFIX);
          if (tmpFiles == null) {
            continue;
          }
          files.addAll(tmpFiles);
        }
      }
    }
    return files;
  }

  private static List<File> getAllFilesInOneDirBySuffix(String dirPath, String suffix) {
    File dir = new File(dirPath);
    if (!dir.isDirectory()) {
      logger.warn("It's not a directory path : " + dirPath);
      return null;
    }
    if (!dir.exists()) {
      logger.warn("Cannot find Directory : " + dirPath);
      return null;
    }
    List<File> tsFiles = new ArrayList<>();
    tsFiles.addAll(
        Arrays.asList(FSFactoryProducer.getFSFactory().listFilesBySuffix(dirPath, suffix)));
    List<File> tmpFiles = Arrays.asList(dir.listFiles());
    for (File f : tmpFiles) {
      if (f.isDirectory()) {
        tsFiles.addAll(getAllFilesInOneDirBySuffix(f.getAbsolutePath(), suffix));
      }
    }
    return tsFiles;
  }

  /**
   * This method is used to settle tsFiles and mods files, so that each old TsFile corresponds to
   * one or several new TsFiles. This method is only applicable to V3 TsFile.
   *
   * @return Each old TsFile corresponds to one or several new TsFileResources of the new TsFiles
   */
  public static Map<String, List<TsFileResource>> settleTsFilesAndMods(
      Map<String, TsFileResource> resourcesToBeSettled) {
    int successCount = 0;
    Map<String, List<TsFileResource>> newTsFileResources = new HashMap<>();
    SettleLog.createSettleLog();
    for (Map.Entry<String, TsFileResource> entry : resourcesToBeSettled.entrySet()) {
      TsFileResource resourceToBeSettled = entry.getValue();
      List<TsFileResource> settledTsFileResources = new ArrayList<>();
      try {
        TsFileAndModSettleTool tsFileAndModSettleTool = new TsFileAndModSettleTool();
        System.out.println("Start settling for tsFile : " + resourceToBeSettled.getTsFilePath());
        if (tsFileAndModSettleTool.isSettledFileGenerated(resourceToBeSettled)) {
          settledTsFileResources = tsFileAndModSettleTool.findSettledFile(resourceToBeSettled);
          newTsFileResources.put(resourceToBeSettled.getTsFile().getName(), settledTsFileResources);
        } else {
          // Write Settle Log, Status 1
          SettleLog.writeSettleLog(
              resourceToBeSettled.getTsFilePath()
                  + SettleLog.COMMA_SEPERATOR
                  + SettleCheckStatus.BEGIN_SETTLE_FILE);
          tsFileAndModSettleTool.settleOneTsFileAndMod(resourceToBeSettled, settledTsFileResources);
          // Write Settle Log, Status 2
          SettleLog.writeSettleLog(
              resourceToBeSettled.getTsFilePath()
                  + SettleLog.COMMA_SEPERATOR
                  + SettleCheckStatus.AFTER_SETTLE_FILE);
          newTsFileResources.put(resourceToBeSettled.getTsFile().getName(), settledTsFileResources);
        }

        TsFileRewriteTool.moveNewTsFile(resourceToBeSettled, settledTsFileResources);
        // Write Settle Log, Status 3
        SettleLog.writeSettleLog(
            resourceToBeSettled.getTsFilePath()
                + SettleLog.COMMA_SEPERATOR
                + SettleCheckStatus.SETTLE_SUCCESS);
        System.out.println(
            "Finish settling successfully for tsFile : " + resourceToBeSettled.getTsFilePath());
        successCount++;
      } catch (Exception e) {
        System.out.println(
            "Meet error while settling the tsFile : " + resourceToBeSettled.getTsFilePath());
        e.printStackTrace();
      }
    }
    if (resourcesToBeSettled.size() == successCount) {
      SettleLog.closeLogWriter();
      FSFactoryProducer.getFSFactory().getFile(SettleLog.getSettleLogPath()).delete();
      System.out.println("Finish settling all tsfiles Successfully!");
    } else {
      System.out.println(
          "Finish Settling, "
              + (resourcesToBeSettled.size() - successCount)
              + " tsfiles meet errors.");
    }
    return newTsFileResources;
  }

  public void settleOneTsFileAndMod(
      TsFileResource resourceToBeSettled, List<TsFileResource> settledResources) throws Exception {
    if (!resourceToBeSettled.isClosed()) {
      logger.warn(
          "The tsFile {} should be sealed when rewritting.", resourceToBeSettled.getTsFilePath());
      return;
    }
    // if no deletions to this tsfile, then return null.
    if (!resourceToBeSettled.getModFile().exists()) {
      return;
    }
    try (TsFileRewriteTool tsFileRewriteTool = new TsFileRewriteTool(resourceToBeSettled)) {
      tsFileRewriteTool.parseAndRewriteFile(settledResources);
    }
    if (settledResources.size() == 0) { // if all the data in this tsfile has been deleted
      resourceToBeSettled.delete();
    }
    return;
  }

  public static void findFilesToBeRecovered() {
    if (FSFactoryProducer.getFSFactory().getFile(SettleLog.getSettleLogPath()).exists()) {
      try (BufferedReader settleLogReader =
          new BufferedReader(
              new FileReader(
                  FSFactoryProducer.getFSFactory().getFile(SettleLog.getSettleLogPath())))) {
        String line = null;
        while ((line = settleLogReader.readLine()) != null && !line.equals("")) {
          String oldFilePath = line.split(SettleLog.COMMA_SEPERATOR)[0];
          Integer settleCheckStatus = Integer.parseInt(line.split(SettleLog.COMMA_SEPERATOR)[1]);
          if (settleCheckStatus == SettleCheckStatus.SETTLE_SUCCESS.getCheckStatus()) {
            recoverSettleFileMap.remove(oldFilePath);
            continue;
          }
          recoverSettleFileMap.put(oldFilePath, settleCheckStatus);
        }
      } catch (IOException e) {
        logger.error(
            "meet error when recover settle process, file path:{}",
            SettleLog.getSettleLogPath(),
            e);
      }
    }
  }

  /** this method is used to check whether the new file is settled when recovering old tsFile. */
  public boolean isSettledFileGenerated(TsFileResource oldTsFileResource) {
    String oldFilePath = oldTsFileResource.getTsFilePath();
    return TsFileAndModSettleTool.recoverSettleFileMap.containsKey(oldFilePath)
        && TsFileAndModSettleTool.recoverSettleFileMap.get(oldFilePath)
            == SettleCheckStatus.AFTER_SETTLE_FILE.getCheckStatus();
  }

  /** when the new file is settled , we need to find and deserialize it. */
  public List<TsFileResource> findSettledFile(TsFileResource resourceToBeSettled)
      throws IOException {
    List<TsFileResource> settledTsFileResources = new ArrayList<>();
    SettleLog.writeSettleLog(
        resourceToBeSettled.getTsFilePath()
            + SettleLog.COMMA_SEPERATOR
            + SettleCheckStatus.BEGIN_SETTLE_FILE);

    for (File tempPartitionDir : resourceToBeSettled.getTsFile().getParentFile().listFiles()) {
      if (tempPartitionDir.isDirectory()
          && FSFactoryProducer.getFSFactory()
              .getFile(
                  tempPartitionDir,
                  resourceToBeSettled.getTsFile().getName() + TsFileResource.RESOURCE_SUFFIX)
              .exists()) {
        TsFileResource settledTsFileResource =
            new TsFileResource(
                FSFactoryProducer.getFSFactory()
                    .getFile(tempPartitionDir, resourceToBeSettled.getTsFile().getName()));
        settledTsFileResource.deserialize();
        settledTsFileResources.add(settledTsFileResource);
      }
    }
    SettleLog.writeSettleLog(
        resourceToBeSettled.getTsFilePath()
            + SettleLog.COMMA_SEPERATOR
            + SettleCheckStatus.AFTER_SETTLE_FILE);
    return settledTsFileResources;
  }

  public static void clearRecoverSettleFileMap() {
    recoverSettleFileMap.clear();
  }
}
