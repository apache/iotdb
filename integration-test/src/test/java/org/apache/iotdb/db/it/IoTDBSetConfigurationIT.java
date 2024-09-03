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
import org.apache.iotdb.it.env.cluster.node.AbstractNodeWrapper;
import org.apache.iotdb.it.framework.IoTDBTestRunner;
import org.apache.iotdb.itbase.category.LocalStandaloneIT;

import org.awaitility.Awaitility;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Arrays;
import java.util.concurrent.TimeUnit;

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
  public void testSetConfiguration() {
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      statement.execute("set configuration \"enable_seq_space_compaction\"=\"false\"");
      statement.execute("set configuration \"enable_unseq_space_compaction\"=\"false\" on 0");
      statement.execute("set configuration \"enable_cross_space_compaction\"=\"false\" on 1");
    } catch (Exception e) {
      Assert.fail(e.getMessage());
    }
    Assert.assertTrue(
        EnvFactory.getEnv().getConfigNodeWrapperList().stream()
            .allMatch(
                nodeWrapper ->
                    checkConfigFileContains(
                        nodeWrapper,
                        "enable_seq_space_compaction=false",
                        "enable_unseq_space_compaction=false")));
    Assert.assertTrue(
        EnvFactory.getEnv().getDataNodeWrapperList().stream()
            .allMatch(
                nodeWrapper ->
                    checkConfigFileContains(
                        nodeWrapper,
                        "enable_seq_space_compaction=false",
                        "enable_cross_space_compaction=false")));
  }

  @Test
  public void testSetClusterName() throws Exception {
    // set cluster name on cn and dn
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      statement.execute("set configuration \"cluster_name\"=\"xx\"");
      ResultSet variables = statement.executeQuery("show variables");
      variables.next();
      Assert.assertEquals("xx", variables.getString(2));
    }
    Assert.assertTrue(
        EnvFactory.getEnv().getNodeWrapperList().stream()
            .allMatch(nodeWrapper -> checkConfigFileContains(nodeWrapper, "cluster_name=xx")));
    // restart successfully
    EnvFactory.getEnv().getDataNodeWrapper(0).stop();
    EnvFactory.getEnv().getDataNodeWrapper(0).start();
    // set cluster name on datanode
    Awaitility.await()
        .atMost(30, TimeUnit.SECONDS)
        .pollDelay(1, TimeUnit.SECONDS)
        .until(
            () -> {
              try (Connection connection = EnvFactory.getEnv().getConnection();
                  Statement statement = connection.createStatement()) {
                statement.execute("set configuration \"cluster_name\"=\"yy\" on 1");
              } catch (Exception e) {
                return false;
              }
              return true;
            });
    // cannot restart
    EnvFactory.getEnv().getDataNodeWrapper(0).stop();
    EnvFactory.getEnv().getDataNodeWrapper(0).start();
    Awaitility.await()
        .atMost(10, TimeUnit.SECONDS)
        .until(() -> !EnvFactory.getEnv().getDataNodeWrapper(0).isAlive());
    Assert.assertTrue(
        checkConfigFileContains(EnvFactory.getEnv().getDataNodeWrapper(0), "cluster_name=yy"));
  }

  private static boolean checkConfigFileContains(
      AbstractNodeWrapper nodeWrapper, String... contents) {
    try {
      String systemPropertiesPath =
          nodeWrapper.getNodePath()
              + File.separator
              + "conf"
              + File.separator
              + CommonConfig.SYSTEM_CONFIG_NAME;
      File f = new File(systemPropertiesPath);
      String fileContent = new String(Files.readAllBytes(f.toPath()));
      return Arrays.stream(contents).allMatch(fileContent::contains);
    } catch (IOException ignore) {
      return false;
    }
  }
}
