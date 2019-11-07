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
package org.apache.iotdb.tsfile.tool.upgrade;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.commons.io.FileUtils;
import org.apache.iotdb.tsfile.fileSystem.FSFactoryProducer;

public class UpgradeTool {

  /**
   * upgrade all tsfiles in the specific dir
   *
   * @param dir tsfile dir which needs to be upgraded
   * @param upgradeDir tsfile dir after upgraded
   * @param threadNum num of threads that perform offline upgrade tasks
   */
  public static void upgradeTsfiles(String dir, String upgradeDir, int threadNum)
      throws IOException {
    //Traverse to find all tsfiles
    File file = FSFactoryProducer.getFSFactory().getFile(dir);
    Queue<File> tmp = new LinkedList<>();
    tmp.add(file);
    List<String> tsfiles = new ArrayList<>();
    if (file.exists()) {
      while (!tmp.isEmpty()) {
        File tmp_file = tmp.poll();
        File[] files = tmp_file.listFiles();
        for (File file2 : files) {
          if (file2.isDirectory()) {
            tmp.add(file2);
          } else {
            if (file2.getName().endsWith(".tsfile")) {
              tsfiles.add(file2.getAbsolutePath());
            }
            // copy all the resource files to the upgradeDir
            if (file2.getName().endsWith(".resource")) {
              File newFileName = FSFactoryProducer.getFSFactory()
                  .getFile(file2.getAbsoluteFile().toString().replace(dir, upgradeDir));
              if (!newFileName.getParentFile().exists()) {
                newFileName.getParentFile().mkdirs();
              }
              newFileName.createNewFile();
              FileUtils.copyFile(file2, newFileName);
            }
          }
        }
      }
    }
    // begin upgrade tsfiles
    System.out.println(String.format(
        "begin upgrade the data dir:%s, the total num of the tsfiles that need to be upgraded:%s",
        dir, tsfiles.size()));
    AtomicInteger dirUpgradeFileNum = new AtomicInteger(tsfiles.size());
    ExecutorService offlineUpgradeThreadPool = Executors.newFixedThreadPool(threadNum);
    //for every tsfile，do upgrade operation
    for (String tsfile : tsfiles) {
      offlineUpgradeThreadPool.submit(() -> {
        try {
          upgradeOneTsfile(tsfile, tsfile.replace(dir, upgradeDir));
          System.out.println(
              String.format("upgrade file success, file name:%s, remaining file num:%s", tsfile,
                  dirUpgradeFileNum.decrementAndGet()));
        } catch (Exception e) {
          System.out.println(String.format("meet error when upgrade file:%s", tsfile));
          e.printStackTrace();
        }
      });
    }
    offlineUpgradeThreadPool.shutdown();
  }

  /**
   * upgrade a single tsfile
   *
   * @param tsfileName old version tsfile's absolute path
   * @param updateFileName new version tsfile's absolute path
   */
  public static void upgradeOneTsfile(String tsfileName, String updateFileName) throws IOException {
    TsfileUpgradeToolV0_8_0 updater = new TsfileUpgradeToolV0_8_0(tsfileName);
    updater.upgradeFile(updateFileName);
  }

}