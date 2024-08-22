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
import java.util.Arrays;

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
  public void testSetConfiguration() {
    try (Connection connection = EnvFactory.getEnv().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement statement = connection.createStatement()) {
      statement.execute("set configuration \"enable_seq_space_compaction\"='false'");
      int configNodeNum = EnvFactory.getEnv().getConfigNodeWrapperList().size();
      int dataNodeNum = EnvFactory.getEnv().getDataNodeWrapperList().size();

      for (int i = 0; i < configNodeNum; i++) {
        statement.execute("set configuration enable_unseq_space_compaction=\'false\' on " + i);
      }
      for (int i = 0; i < dataNodeNum; i++) {
        int dnId = configNodeNum + i;
        statement.execute("set configuration \"enable_cross_space_compaction\"='false' on " + dnId);
        statement.execute(
            "set configuration max_inner_compaction_candidate_file_num='1',max_cross_compaction_candidate_file_num='1' on "
                + dnId);
      }
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
                        "enable_cross_space_compaction=false",
                        "max_inner_compaction_candidate_file_num=1",
                        "max_cross_compaction_candidate_file_num=1")));
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
