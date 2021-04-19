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

import org.apache.iotdb.db.conf.IoTDBConfigCheck;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.engine.fileSystem.SystemFileFactory;
import org.apache.iotdb.db.service.IoTDB;
import org.apache.iotdb.db.utils.EnvironmentUtils;
import org.apache.iotdb.tsfile.common.conf.TSFileConfig;
import org.apache.iotdb.tsfile.common.conf.TSFileDescriptor;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintStream;
import java.security.AccessControlException;
import java.security.Permission;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class IoTDBCheckConfigIT {
  private File propertiesFile =
      SystemFileFactory.INSTANCE.getFile(
          IoTDBDescriptor.getInstance().getConfig().getSchemaDir()
              + File.separator
              + "system.properties");

  private TSFileConfig tsFileConfig = TSFileDescriptor.getInstance().getConfig();

  private Map<String, String> systemProperties = new HashMap<>();

  private Properties properties = new Properties();

  private PrintStream console = null;
  private ByteArrayOutputStream bytes = null;

  @Before
  public void setUp() {
    EnvironmentUtils.closeStatMonitor();
    EnvironmentUtils.envSetUp();

    final SecurityManager securityManager =
        new SecurityManager() {
          public void checkPermission(Permission permission) {
            if (permission.getName().startsWith("exitVM")) {
              throw new AccessControlException("Wrong system config");
            }
          }
        };
    System.setSecurityManager(securityManager);
    bytes = new ByteArrayOutputStream();
    console = System.out;
    System.setOut(new PrintStream(bytes));

    systemProperties.put("partition_interval", "604800");
    systemProperties.put("timestamp_precision", "ms");
    systemProperties.put("tsfile_storage_fs", "LOCAL");
    systemProperties.put("enable_partition", "false");
    systemProperties.put("max_degree_of_index_node", "256");
    systemProperties.put("tag_attribute_total_size", "700");
    systemProperties.put("iotdb_version", "0.11.2");
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
    try {
      IoTDBConfigCheck.getInstance().checkConfig();
    } finally {
      System.setSecurityManager(null);
    }

    // read properties from system.properties
    try (FileInputStream inputStream = new FileInputStream(propertiesFile);
        InputStreamReader inputStreamReader =
            new InputStreamReader(inputStream, TSFileConfig.STRING_CHARSET)) {
      properties.load(inputStreamReader);
    }
    String timeEncoder = (String) properties.get("time_encoder");
    assertTrue(!timeEncoder.isEmpty());
  }

  @Test
  public void testAlterTimeEncoderAfterStartService() throws Exception {
    EnvironmentUtils.shutdownDaemon();
    EnvironmentUtils.stopDaemon();
    IoTDB.metaManager.clear();
    systemProperties.put("time_encoder", "REGULAR");
    writeSystemFile();
    EnvironmentUtils.reactiveDaemon();
    try {
      IoTDBConfigCheck.getInstance().checkConfig();
    } catch (Throwable t) {
      assertEquals("Wrong system config", t.getMessage());
    } finally {
      System.setSecurityManager(null);
    }
    assertTrue(bytes.toString().contains("Wrong time_encoder, please set as: REGULAR"));
  }

  @Test
  public void testSameTimeEncoderAfterStartService() throws Exception {
    EnvironmentUtils.shutdownDaemon();
    EnvironmentUtils.stopDaemon();
    IoTDB.metaManager.clear();
    systemProperties.put("time_encoder", "TS_2DIFF");
    writeSystemFile();
    EnvironmentUtils.reactiveDaemon();
    try {
      IoTDBConfigCheck.getInstance().checkConfig();
    } catch (Throwable t) {
      assertTrue(false);
    } finally {
      System.setSecurityManager(null);
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
