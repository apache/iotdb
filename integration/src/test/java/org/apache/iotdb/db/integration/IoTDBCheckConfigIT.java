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
package org.apache.iotdb.db.integration;

import org.apache.iotdb.commons.exception.ConfigurationException;
import org.apache.iotdb.commons.file.SystemFileFactory;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.conf.IoTDBStartCheck;
import org.apache.iotdb.db.utils.EnvironmentUtils;
import org.apache.iotdb.itbase.category.LocalStandaloneTest;
import org.apache.iotdb.tsfile.common.conf.TSFileConfig;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintStream;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.fail;

@Category({LocalStandaloneTest.class})
public class IoTDBCheckConfigIT {
  private File propertiesFile =
      SystemFileFactory.INSTANCE.getFile(
          IoTDBDescriptor.getInstance().getConfig().getSchemaDir()
              + File.separator
              + "system.properties");

  private Map<String, String> systemProperties = new HashMap<>();

  private Properties properties = new Properties();

  private PrintStream console = null;
  private ByteArrayOutputStream bytes = null;

  @Before
  public void setUp() {
    EnvironmentUtils.envSetUp();

    bytes = new ByteArrayOutputStream();
    console = System.out;
    System.setOut(new PrintStream(bytes));

    systemProperties.put("partition_interval", "9223372036854775807");
    systemProperties.put("timestamp_precision", "ms");
    systemProperties.put("tsfile_storage_fs", "LOCAL");
    systemProperties.put("enable_partition", "false");
    systemProperties.put("max_degree_of_index_node", "256");
    systemProperties.put("tag_attribute_total_size", "700");
    systemProperties.put("iotdb_version", "0.13.0");
    systemProperties.put("virtual_storage_group_num", "1");
  }

  @After
  public void tearDown() throws Exception {
    EnvironmentUtils.cleanEnv();
    systemProperties.clear();
    properties.clear();
    System.setOut(console);
  }

  @Test
  public void testSaveTimeEncoderToSystemProperties() throws Exception {
    IoTDBStartCheck.getInstance().checkSystemConfig();
    // read properties from system.properties
    try (FileInputStream inputStream = new FileInputStream(propertiesFile);
        InputStreamReader inputStreamReader =
            new InputStreamReader(inputStream, TSFileConfig.STRING_CHARSET)) {
      properties.load(inputStreamReader);
    }
    String timeEncoder = (String) properties.get("time_encoder");
    assertFalse(timeEncoder.isEmpty());
  }

  @Test
  public void testAlterTimeEncoderAfterStartService() throws Exception {
    EnvironmentUtils.shutdownDaemon();
    EnvironmentUtils.stopDaemon();
    systemProperties.put("time_encoder", "REGULAR");
    writeSystemFile();
    EnvironmentUtils.reactiveDaemon();
    try {
      IoTDBStartCheck.getInstance().checkSystemConfig();
    } catch (ConfigurationException t) {
      t.printStackTrace();
      assertEquals("time_encoder", t.getParameter());
      assertEquals("REGULAR", t.getCorrectValue());
      return;
    }
    fail("should detect configuration errors");
  }

  @Test
  public void testSameTimeEncoderAfterStartService() throws Exception {
    EnvironmentUtils.shutdownDaemon();
    EnvironmentUtils.stopDaemon();
    systemProperties.put("time_encoder", "TS_2DIFF");
    writeSystemFile();
    EnvironmentUtils.reactiveDaemon();
    try {
      IoTDBStartCheck.getInstance().checkSystemConfig();
    } catch (Throwable t) {
      fail(t.getMessage());
    }
  }

  private void writeSystemFile() throws IOException {
    // write properties to system.properties
    try (FileOutputStream outputStream = new FileOutputStream(propertiesFile)) {
      systemProperties.forEach((k, v) -> properties.setProperty(k, v));
      properties.store(outputStream, "System properties:");
    }
  }
}
