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
import org.apache.iotdb.commons.conf.ConfigurationFileUtils;
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
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

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
  public void testSetConfigurationWithUndefinedConfigKey() {
    String expectedExceptionMsg =
        "301: ignored config items: [a] because they are immutable or undefined.";
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      executeAndExpectException(
          statement, "set configuration \"a\"=\"false\"", expectedExceptionMsg);
      int configNodeNum = EnvFactory.getEnv().getConfigNodeWrapperList().size();
      int dataNodeNum = EnvFactory.getEnv().getDataNodeWrapperList().size();

      for (int i = 0; i < configNodeNum; i++) {
        executeAndExpectException(
            statement, "set configuration \"a\"=\"false\" on " + i, expectedExceptionMsg);
      }
      for (int i = 0; i < dataNodeNum; i++) {
        int dnId = configNodeNum + i;
        executeAndExpectException(
            statement, "set configuration \"a\"=\"false\" on " + dnId, expectedExceptionMsg);
      }
    } catch (Exception e) {
      Assert.fail(e.getMessage());
    }
  }

  private void executeAndExpectException(
      Statement statement, String sql, String expectedContentInExceptionMsg) {
    try {
      statement.execute(sql);
    } catch (Exception e) {
      Assert.assertTrue(e.getMessage().contains(expectedContentInExceptionMsg));
      return;
    }
    Assert.fail();
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
    AbstractNodeWrapper datanode = EnvFactory.getEnv().getDataNodeWrapper(0);
    Assert.assertTrue(
        checkConfigFileContains(EnvFactory.getEnv().getDataNodeWrapper(0), "cluster_name=yy"));

    // Modify the config file manually because the datanode can not restart
    Properties properties = new Properties();
    properties.put("cluster_name", "xx");
    ConfigurationFileUtils.updateConfiguration(getConfigFile(datanode), properties, null);
    EnvFactory.getEnv().getDataNodeWrapper(0).stop();
    EnvFactory.getEnv().getDataNodeWrapper(0).start();
    // wait the datanode restart successfully (won't do any meaningful modification)
    Awaitility.await()
        .atMost(30, TimeUnit.SECONDS)
        .pollDelay(1, TimeUnit.SECONDS)
        .until(
            () -> {
              try (Connection connection = EnvFactory.getEnv().getConnection();
                  Statement statement = connection.createStatement()) {
                statement.execute("set configuration \"cluster_name\"=\"xx\" on 1");
              } catch (Exception e) {
                return false;
              }
              return true;
            });
  }

  private static boolean checkConfigFileContains(
      AbstractNodeWrapper nodeWrapper, String... contents) {
    try {
      String fileContent = new String(Files.readAllBytes(getConfigFile(nodeWrapper).toPath()));
      return Arrays.stream(contents).allMatch(fileContent::contains);
    } catch (IOException ignore) {
      return false;
    }
  }

  private static File getConfigFile(AbstractNodeWrapper nodeWrapper) {
    String systemPropertiesPath =
        nodeWrapper.getNodePath()
            + File.separator
            + "conf"
            + File.separator
            + CommonConfig.SYSTEM_CONFIG_NAME;
    return new File(systemPropertiesPath);
  }

  @Test
  public void testSetDefaultSGLevel() throws SQLException {
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      // legal value
      statement.execute("set configuration \"default_storage_group_level\"=\"3\"");
      statement.execute("INSERT INTO root.a.b.c.d1(timestamp, s1) VALUES (1, 1)");
      ResultSet databases = statement.executeQuery("show databases");
      databases.next();
      Assert.assertEquals("root.a.b.c", databases.getString(1));
      assertFalse(databases.next());

      // path too short
      try {
        statement.execute("INSERT INTO root.fail(timestamp, s1) VALUES (1, 1)");
      } catch (SQLException e) {
        assertEquals(
            "509: An error occurred when executing getDeviceToDatabase():root.fail is not a legal path, because it is no longer than default sg level: 3",
            e.getMessage());
      }

      // illegal value
      try {
        statement.execute("set configuration \"default_storage_group_level\"=\"-1\"");
      } catch (SQLException e) {
        assertTrue(e.getMessage().contains("Illegal defaultStorageGroupLevel: -1, should >= 1"));
      }

      // Failed updates will not change the files.
      assertFalse(
          checkConfigFileContains(
              EnvFactory.getEnv().getDataNodeWrapper(0), "default_storage_group_level=-1"));
      assertTrue(
          checkConfigFileContains(
              EnvFactory.getEnv().getDataNodeWrapper(0), "default_storage_group_level=3"));
    }

    // can start with an illegal value
    EnvFactory.getEnv().cleanClusterEnvironment();
    EnvFactory.getEnv().getConfig().getCommonConfig().setDefaultStorageGroupLevel(-1);
    EnvFactory.getEnv().initClusterEnvironment();
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      statement.execute("INSERT INTO root.a.b.c.d1(timestamp, s1) VALUES (1, 1)");
      ResultSet databases = statement.executeQuery("show databases");
      databases.next();
      // the default value should take effect
      Assert.assertEquals("root.a", databases.getString(1));
      assertFalse(databases.next());

      // create timeseries with an illegal path
      try {
        statement.execute("CREATE TIMESERIES root.db1.s3 WITH datatype=INT32");
      } catch (SQLException e) {
        assertEquals(
                "509: An error occurred when executing getDeviceToDatabase():root.db1 is not a legal path, because it is no longer than default sg level: 3",
                e.getMessage());
      }
    }
  }
}
