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
import org.apache.commons.io.FileUtils;
import org.apache.iotdb.tsfile.fileSystem.FSFactoryProducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class UpgradeTool {

  private static final Logger logger = LoggerFactory.getLogger(UpgradeTool.class);

  /**
   * upgrade all tsfiles in the specific dir
   *
   * @param dir tsfile dir which needs to be upgraded
   * @param upgradeDir tsfile dir after upgraded
   */
  public static void updateTsfiles(String dir, String upgradeDir) throws IOException {
    //Traverse to find all tsfiles
    File file = FSFactoryProducer.getFSFactory().getFile(dir);
    Queue<File> tmp = new LinkedList<>();
    tmp.add(file);
    List<String> tsfiles = new ArrayList<>();
    if (file.exists()) {
      while (!tmp.isEmpty()) {
        File tmp_file = ((LinkedList<File>) tmp).pollFirst();
        File[] files = tmp_file.listFiles();
        for (File file2 : files) {
          if (file2.isDirectory()) {
            tmp.add(file2);
          } else {
            if (file2.getName().endsWith(".tsfile")) {
              tsfiles.add(file2.getAbsolutePath());
            }
            if (file2.getName().endsWith(".resource")){
              File newFileName = FSFactoryProducer.getFSFactory().getFile(file2.getAbsoluteFile().toString().replace(dir, upgradeDir));
              if (!newFileName.getParentFile().exists()){
                newFileName.getParentFile().mkdirs();
              }
              newFileName.createNewFile();
              FileUtils.copyFile(file2, newFileName);
            }
          }
        }
      }
    }
    //for every tsfile，do upgrade operation
    for (String tsfile : tsfiles) {
      upgradeOneTsfile(tsfile, tsfile.replace(dir, upgradeDir));
    }
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

  public static void main(String[] args) throws IOException {
    List<String> oldVersionTsfileDirs = new ArrayList<>();
    List<String> newVersionTsfileDirs = new ArrayList<>();
    oldVersionTsfileDirs.add("/Users/tianyu/incubator-iotdb/data/data/sequence/root.group_10");
    newVersionTsfileDirs.add("/Users/tianyu/incubator-iotdb/data/data/sequence/root.group_8");
    for (int i = 0; i < oldVersionTsfileDirs.size(); i++) {
      updateTsfiles(oldVersionTsfileDirs.get(i), newVersionTsfileDirs.get(i));
    }
  }
}