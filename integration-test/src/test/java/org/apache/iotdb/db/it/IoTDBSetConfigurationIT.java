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

package org.apache.iotdb.db.it;

import org.apache.iotdb.commons.conf.CommonConfig;
import org.apache.iotdb.it.env.EnvFactory;
import org.apache.iotdb.it.env.cluster.node.ConfigNodeWrapper;
import org.apache.iotdb.it.env.cluster.node.DataNodeWrapper;
import org.apache.iotdb.it.framework.IoTDBTestRunner;
import org.apache.iotdb.itbase.category.LocalStandaloneIT;

import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.File;
import java.nio.file.Files;
import java.sql.Connection;
import java.sql.Statement;

@RunWith(IoTDBTestRunner.class)
@Category({LocalStandaloneIT.class})
public class IoTDBSetConfigurationIT {
  @BeforeClass
  public static void setUp() throws Exception {
    EnvFactory.getEnv().initClusterEnvironment();
  }

  @AfterClass
  public static void tearDown() throws Exception {
    EnvFactory.getEnv().cleanClusterEnvironment();
  }

  @Test
  public void testSetConfiguration() throws Exception {
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      statement.execute("set configuration \"enable_seq_space_compaction\"=\"false\"");
      statement.execute("set configuration \"enable_unseq_space_compaction\"=\"false\" on 0");
      statement.execute("set configuration \"enable_cross_space_compaction\"=\"false\" on 1");
    } catch (Exception e) {
      Assert.fail(e.getMessage());
    }
    for (ConfigNodeWrapper configNodeWrapper : EnvFactory.getEnv().getConfigNodeWrapperList()) {
      String systemPropertiesPath =
          configNodeWrapper.getNodePath()
              + File.separator
              + "conf"
              + File.separator
              + CommonConfig.SYSTEM_CONFIG_NAME;
      File f = new File(systemPropertiesPath);
      String content = new String(Files.readAllBytes(f.toPath()));
      Assert.assertTrue(content.contains("enable_seq_space_compaction=false"));
      Assert.assertTrue(content.contains("enable_unseq_space_compaction=false"));
    }
    for (DataNodeWrapper dataNodeWrapper : EnvFactory.getEnv().getDataNodeWrapperList()) {
      String systemPropertiesPath =
          dataNodeWrapper.getNodePath()
              + File.separator
              + "conf"
              + File.separator
              + CommonConfig.SYSTEM_CONFIG_NAME;
      File f = new File(systemPropertiesPath);
      String content = new String(Files.readAllBytes(f.toPath()));
      Assert.assertTrue(content.contains("enable_seq_space_compaction=false"));
      Assert.assertTrue(content.contains("enable_cross_space_compaction=false"));
    }
  }
}
