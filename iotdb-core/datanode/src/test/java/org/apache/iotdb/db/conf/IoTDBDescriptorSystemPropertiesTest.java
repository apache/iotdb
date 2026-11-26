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
package org.apache.iotdb.db.conf;

import org.apache.iotdb.commons.conf.CommonConfig;
import org.apache.iotdb.commons.conf.IoTDBConstant;
import org.apache.iotdb.commons.memory.MemoryConfig;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;

import static org.junit.Assert.assertEquals;

public class IoTDBDescriptorSystemPropertiesTest {

  @Rule public TemporaryFolder temporaryFolder = new TemporaryFolder();

  @Test
  public void testSystemPropertiesInitializeConfiguredRpcBufferMemory() throws Exception {
    String originalConf = System.getProperty(IoTDBConstant.IOTDB_CONF);
    File confDir = temporaryFolder.newFolder();
    Files.writeString(
        confDir.toPath().resolve(CommonConfig.SYSTEM_CONFIG_NAME),
        "datanode_memory_proportion=1:1:1:1:1:5\n",
        StandardCharsets.UTF_8);
    System.setProperty(IoTDBConstant.IOTDB_CONF, confDir.getAbsolutePath());

    try {
      // The descriptor and MemoryConfig statics are isolated by surefire's per-class fork.
      IoTDBDescriptor descriptor = new IoTDBDescriptor();
      descriptor.getMemoryConfig().activateAutoResizingBufferMemoryControl();

      assertEquals(
          Runtime.getRuntime().maxMemory() / 4,
          MemoryConfig.getInstance().getAutoResizingBufferMemoryTotalSizeInBytes());
    } finally {
      if (originalConf == null) {
        System.clearProperty(IoTDBConstant.IOTDB_CONF);
      } else {
        System.setProperty(IoTDBConstant.IOTDB_CONF, originalConf);
      }
    }
  }
}
