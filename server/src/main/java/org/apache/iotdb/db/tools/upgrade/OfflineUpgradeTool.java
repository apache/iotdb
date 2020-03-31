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
package org.apache.iotdb.db.tools.upgrade;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import org.apache.iotdb.tsfile.fileSystem.FSFactoryProducer;
import org.apache.iotdb.tsfile.tool.upgrade.UpgradeTool;

public class OfflineUpgradeTool {

  private static List<String> oldVersionTsfileDirs = new ArrayList<>();
  private static List<String> newVersionTsfileDirs = new ArrayList<>();
  private static int upgradeThreadNum;

  private static void loadProps(String configPath) {
    InputStream inputStream = null;
    try {
      inputStream = new FileInputStream(FSFactoryProducer.getFSFactory().getFile(configPath));
    } catch (FileNotFoundException e) {
      System.out.println(String.format("Fail to find config file:%s", configPath));
      e.printStackTrace();
      System.exit(1);
    }
    Properties properties = new Properties();
    try {
      properties.load(inputStream);
      String oldVersionTsfileDirString = properties.getProperty("old_version_data_dirs");
      Collections.addAll(oldVersionTsfileDirs, oldVersionTsfileDirString.split(","));
      String newVersionTsfileDirString = properties.getProperty("new_version_data_dirs");
      Collections.addAll(newVersionTsfileDirs, newVersionTsfileDirString.split(","));
      upgradeThreadNum = Integer.parseInt(properties.getProperty("upgrade_thread_num"));
    } catch (IOException e) {
      System.out.println("Cannot load config file ");
      e.printStackTrace();
    }
  }

  public static void main(String[] args) throws IOException {
    loadProps(args[0]);
    for (int i = 0; i < oldVersionTsfileDirs.size(); i++) {
      UpgradeTool.upgradeTsfiles(oldVersionTsfileDirs.get(i), newVersionTsfileDirs.get(i),
          upgradeThreadNum);
    }
  }
}
