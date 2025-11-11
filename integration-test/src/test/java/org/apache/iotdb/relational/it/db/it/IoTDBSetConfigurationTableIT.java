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

package org.apache.iotdb.relational.it.db.it;

import org.apache.iotdb.commons.conf.CommonConfig;
import org.apache.iotdb.isession.ITableSession;
import org.apache.iotdb.isession.SessionDataSet;
import org.apache.iotdb.it.env.EnvFactory;
import org.apache.iotdb.it.env.cluster.node.AbstractNodeWrapper;
import org.apache.iotdb.it.framework.IoTDBTestRunner;
import org.apache.iotdb.itbase.category.TableClusterIT;
import org.apache.iotdb.itbase.category.TableLocalStandaloneIT;
import org.apache.iotdb.itbase.env.BaseEnv;

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
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;

@RunWith(IoTDBTestRunner.class)
@Category({TableLocalStandaloneIT.class, TableClusterIT.class})
public class IoTDBSetConfigurationTableIT {
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
    try (Connection connection = EnvFactory.getEnv().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement statement = connection.createStatement()) {
      executeAndExpectException(statement, "set configuration \"a\"='false'", expectedExceptionMsg);
      int configNodeNum = EnvFactory.getEnv().getConfigNodeWrapperList().size();
      int dataNodeNum = EnvFactory.getEnv().getDataNodeWrapperList().size();

      for (int i = 0; i < configNodeNum; i++) {
        executeAndExpectException(
            statement, "set configuration a=\'false\' on " + i, expectedExceptionMsg);
      }
      for (int i = 0; i < dataNodeNum; i++) {
        int dnId = configNodeNum + i;
        executeAndExpectException(
            statement, "set configuration \"a\"='false' on " + dnId, expectedExceptionMsg);
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
    int configNodeNum = EnvFactory.getEnv().getConfigNodeWrapperList().size();
    int dataNodeNum = EnvFactory.getEnv().getDataNodeWrapperList().size();
    try (Connection connection = EnvFactory.getEnv().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement statement = connection.createStatement()) {
      statement.execute("set configuration \"enable_seq_space_compaction\"='false'");

      for (int i = 0; i < configNodeNum; i++) {
        statement.execute("set configuration enable_unseq_space_compaction=\'false\' on " + i);
      }
      for (int i = 0; i < dataNodeNum; i++) {
        int dnId = configNodeNum + i;
        statement.execute("set configuration \"enable_cross_space_compaction\"='false' on " + dnId);
        statement.execute(
            "set configuration inner_compaction_candidate_file_num='1',max_cross_compaction_candidate_file_num='1' on "
                + dnId);
      }
    } catch (Exception e) {
      Assert.fail(e.getMessage());
    }
    for (int i = 0; i < configNodeNum; i++) {
      Assert.assertTrue(
          checkConfigFileContains(
              i,
              EnvFactory.getEnv().getConfigNodeWrapperList().get(i),
              "enable_seq_space_compaction=false",
              "enable_unseq_space_compaction=false"));
    }
    for (int i = 0; i < dataNodeNum; i++) {
      int dnId = configNodeNum + i;
      Assert.assertTrue(
          checkConfigFileContains(
              dnId,
              EnvFactory.getEnv().getDataNodeWrapperList().get(i),
              "enable_seq_space_compaction=false",
              "enable_cross_space_compaction=false",
              "inner_compaction_candidate_file_num=1",
              "max_cross_compaction_candidate_file_num=1"));
    }
  }

  private static boolean checkConfigFileContains(
      int nodeId, AbstractNodeWrapper nodeWrapper, String... contents) {
    try {
      String systemPropertiesPath =
          nodeWrapper.getNodePath()
              + File.separator
              + "conf"
              + File.separator
              + CommonConfig.SYSTEM_CONFIG_NAME;
      File f = new File(systemPropertiesPath);
      String fileContent = new String(Files.readAllBytes(f.toPath()));
      Map<String, String> showConfigurationResults = new HashMap<>();
      for (String content : contents) {
        if (!fileContent.contains(content)) {
          return false;
        }
        String[] split = content.split("=");
        showConfigurationResults.put(split[0], split[1]);
      }
      return checkShowConfigurationContains(nodeId, showConfigurationResults);
    } catch (IOException ignore) {
      return false;
    }
  }

  private static boolean checkShowConfigurationContains(
      int nodeId, Map<String, String> expectedKeyValues) {
    try (ITableSession tableSessionConnection = EnvFactory.getEnv().getTableSessionConnection()) {
      SessionDataSet sessionDataSet =
          tableSessionConnection.executeQueryStatement("show configuration on " + nodeId);
      SessionDataSet.DataIterator iterator = sessionDataSet.iterator();
      while (iterator.next()) {
        String name = iterator.getString(1);
        String value = iterator.isNull(2) ? null : iterator.getString(2);
        String expectedValue = expectedKeyValues.remove(name);
        if (expectedValue != null && !expectedValue.equals(value)) {
          return false;
        }
      }
    } catch (Exception e) {
      return false;
    }
    return expectedKeyValues.isEmpty();
  }
}
