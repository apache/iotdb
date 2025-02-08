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

package org.apache.iotdb.db.utils;

import org.apache.iotdb.commons.conf.ConfigurationFileUtils;
import org.apache.iotdb.db.utils.constant.TestConstant;

import org.junit.After;
import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Files;
import java.util.Objects;
import java.util.Properties;

public class ConfigurationFileUtilsTest {

  private File dir =
      new File(TestConstant.BASE_OUTPUT_PATH + File.separator + "ConfigurationFileUtilsTest");

  @After
  public void tearDown() throws IOException {
    if (!dir.exists()) {
      return;
    }
    for (File file : Objects.requireNonNull(dir.listFiles())) {
      Files.delete(file.toPath());
    }
    Files.delete(dir.toPath());
  }

  @Test
  public void testMergeOldVersionFiles() throws IOException, InterruptedException {
    dir.mkdirs();
    File confignodeConfigFile = new File(dir + File.separator + "iotdb-confignode.properties");
    File datanodeConfigFile = new File(dir + File.separator + "iotdb-datanode.properties");
    File commonConfigFile = new File(dir + File.separator + "iotdb-common.properties");
    File systemConfigFile = new File(dir + File.separator + "iotdb-system.properties");
    generateFile(confignodeConfigFile, "a=1");
    generateFile(datanodeConfigFile, "b=2");
    generateFile(commonConfigFile, "c=3");
    ConfigurationFileUtils.checkAndMayUpdate(
        systemConfigFile.toURI().toURL(),
        confignodeConfigFile.toURI().toURL(),
        datanodeConfigFile.toURI().toURL(),
        commonConfigFile.toURI().toURL());
    Assert.assertTrue(systemConfigFile.exists());
    Properties properties = new Properties();
    try (FileReader fileReader = new FileReader(systemConfigFile)) {
      properties.load(fileReader);
    }
    Assert.assertEquals("1", properties.getProperty("a"));
    Assert.assertEquals("2", properties.getProperty("b"));
    Assert.assertEquals("3", properties.getProperty("c"));
  }

  private void generateFile(File file, String content) throws IOException {
    Files.write(file.toPath(), content.getBytes());
  }
}
