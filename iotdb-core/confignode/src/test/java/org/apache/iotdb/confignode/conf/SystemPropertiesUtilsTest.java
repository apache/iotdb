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

package org.apache.iotdb.confignode.conf;

import org.apache.iotdb.commons.utils.FileUtils;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.Properties;

public class SystemPropertiesUtilsTest {

  private final ConfigNodeConfig conf = ConfigNodeDescriptor.getInstance().getConf();

  private String originalSystemDir;
  private String originalConsensusDir;
  private File testDir;

  @Before
  public void setUp() throws IOException {
    originalSystemDir = conf.getSystemDir();
    originalConsensusDir = conf.getConsensusDir();
    testDir = Files.createTempDirectory("SystemPropertiesUtilsTest").toFile();

    conf.setSystemDir(new File(testDir, "system").getAbsolutePath());
    conf.setConsensusDir(new File(testDir, "consensus").getAbsolutePath());
    SystemPropertiesUtils.reinitializeStatics();
  }

  @After
  public void tearDown() {
    conf.setSystemDir(originalSystemDir);
    conf.setConsensusDir(originalConsensusDir);
    SystemPropertiesUtils.reinitializeStatics();
    FileUtils.deleteFileOrDirectory(testDir, true);
  }

  @Test
  public void testFirstStartState() {
    Assert.assertEquals(
        SystemPropertiesUtils.StartupState.FIRST_START, SystemPropertiesUtils.getStartupState());
    Assert.assertFalse(SystemPropertiesUtils.isRestarted());
  }

  @Test
  public void testEmptyDirsAreStillFirstStart() {
    Assert.assertTrue(new File(conf.getSystemDir()).mkdirs());
    Assert.assertTrue(new File(conf.getConsensusDir()).mkdirs());

    Assert.assertEquals(
        SystemPropertiesUtils.StartupState.FIRST_START, SystemPropertiesUtils.getStartupState());
    Assert.assertFalse(SystemPropertiesUtils.isRestarted());
  }

  @Test
  public void testRestartState() throws IOException {
    writeSystemProperties(createValidSystemProperties());
    createConsensusStateFile();

    Assert.assertEquals(
        SystemPropertiesUtils.StartupState.RESTART, SystemPropertiesUtils.getStartupState());
    Assert.assertTrue(SystemPropertiesUtils.isRestarted());
  }

  @Test
  public void testPartialStartConsensusOnlyState() throws IOException {
    createConsensusStateFile();

    Assert.assertEquals(
        SystemPropertiesUtils.StartupState.PARTIAL_START_CONSENSUS_ONLY,
        SystemPropertiesUtils.getStartupState());
    Assert.assertFalse(SystemPropertiesUtils.isRestarted());
  }

  @Test
  public void testPartialStartSystemOnlyState() throws IOException {
    writeSystemProperties(createValidSystemProperties());
    Assert.assertTrue(new File(conf.getConsensusDir()).mkdirs());

    Assert.assertEquals(
        SystemPropertiesUtils.StartupState.PARTIAL_START_SYSTEM_ONLY,
        SystemPropertiesUtils.getStartupState());
    Assert.assertFalse(SystemPropertiesUtils.isRestarted());
  }

  @Test
  public void testCorruptedOrInconsistentState() throws IOException {
    Properties properties = createValidSystemProperties();
    properties.remove("config_node_id");
    writeSystemProperties(properties);
    createConsensusStateFile();

    Assert.assertEquals(
        SystemPropertiesUtils.StartupState.CORRUPTED_OR_INCONSISTENT,
        SystemPropertiesUtils.getStartupState());
    Assert.assertFalse(SystemPropertiesUtils.isRestarted());
  }

  private Properties createValidSystemProperties() {
    Properties properties = new Properties();
    properties.setProperty("config_node_id", "0");
    properties.setProperty("is_seed_config_node", "true");
    properties.setProperty("cn_internal_address", "127.0.0.1");
    properties.setProperty("cn_internal_port", "10710");
    properties.setProperty("cn_consensus_port", "10720");
    return properties;
  }

  private void writeSystemProperties(Properties properties) throws IOException {
    File systemFile = new File(conf.getSystemDir(), ConfigNodeConstant.SYSTEM_FILE_NAME);
    Assert.assertTrue(systemFile.getParentFile().mkdirs());
    try (FileOutputStream fileOutputStream = new FileOutputStream(systemFile);
        Writer writer = new OutputStreamWriter(fileOutputStream, StandardCharsets.UTF_8)) {
      properties.store(writer, "");
    }
  }

  private void createConsensusStateFile() throws IOException {
    File stateFile =
        new File(
            conf.getConsensusDir()
                + File.separator
                + "47474747-4747-4747-4747-000000000000"
                + File.separator
                + "current",
            "raft-meta");
    Assert.assertTrue(stateFile.getParentFile().mkdirs());
    Assert.assertTrue(stateFile.createNewFile());
  }
}
