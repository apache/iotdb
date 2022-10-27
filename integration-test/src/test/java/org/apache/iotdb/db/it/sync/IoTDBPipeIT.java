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
package org.apache.iotdb.db.it.sync;

import org.apache.iotdb.db.mpp.common.header.ColumnHeaderConstant;
import org.apache.iotdb.it.env.EnvFactory;
import org.apache.iotdb.it.framework.IoTDBTestRunner;
import org.apache.iotdb.itbase.category.ClusterIT;
import org.apache.iotdb.itbase.category.LocalStandaloneIT;

import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;

import static org.apache.iotdb.db.it.utils.TestUtils.assertResultSetEqual;

@RunWith(IoTDBTestRunner.class)
@Category({LocalStandaloneIT.class, ClusterIT.class})
public class IoTDBPipeIT {
  private static String ip;
  private static int port;
  private static final String SHOW_PIPE_HEADER =
      ColumnHeaderConstant.COLUMN_PIPE_CREATE_TIME
          + ","
          + ColumnHeaderConstant.COLUMN_PIPE_NAME
          + ","
          + ColumnHeaderConstant.COLUMN_PIPE_ROLE
          + ","
          + ColumnHeaderConstant.COLUMN_PIPE_REMOTE
          + ","
          + ColumnHeaderConstant.COLUMN_PIPE_STATUS
          + ","
          + ColumnHeaderConstant.COLUMN_PIPE_MESSAGE
          + ",";

  @BeforeClass
  public static void setUp() throws Exception {
    EnvFactory.getEnv().initBeforeClass();
    if (EnvFactory.getEnv().getDataNodeWrapperList() != null
        && EnvFactory.getEnv().getDataNodeWrapperList().size() > 0) {
      ip = EnvFactory.getEnv().getDataNodeWrapperList().get(0).getIp();
      port = EnvFactory.getEnv().getDataNodeWrapperList().get(0).getPort();
    } else {
      ip = "127.0.0.1";
      port = 6667;
    }
  }

  @AfterClass
  public static void tearDown() throws Exception {
    EnvFactory.getEnv().cleanAfterClass();
  }

  @Test
  public void testOperatePipe() {
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {

      try {
        // check exception1: PIPESINK not exist
        statement.execute("CREATE PIPE p to demo;");
        Assert.fail();
      } catch (Exception e) {
        Assert.assertTrue(e.getMessage().contains("PIPESINK [demo] does not exist"));
      }

      statement.execute(
          String.format("CREATE PIPESINK demo AS IoTDB (ip='%s',port='%d');", ip, port));
      statement.execute("CREATE PIPE p1 to demo;");
      statement.execute("CREATE PIPE p2 to demo;");
      try {
        // check exception2: PIPE already exist
        statement.execute("CREATE PIPE p2 to demo;");
        Assert.fail();
      } catch (Exception e) {
        Assert.assertTrue(e.getMessage().contains("PIPE [p2] is STOP, please retry after drop it"));
      }
      // check pipe operation
      String createTime1 = getCreateTime(statement, "p1");
      String createTime2 = getCreateTime(statement, "p2");
      try (ResultSet resultSet = statement.executeQuery("SHOW PIPE")) {
        String[] expectedRetSet =
            new String[] {
              String.format("%s,p1,sender,demo,STOP,NORMAL,", createTime1),
              String.format("%s,p2,sender,demo,STOP,NORMAL,", createTime2)
            };
        assertResultSetEqual(resultSet, SHOW_PIPE_HEADER, expectedRetSet);
      }
      statement.execute("START PIPE p1;");
      try (ResultSet resultSet = statement.executeQuery("SHOW PIPE")) {
        String[] expectedRetSet =
            new String[] {
              // there is no data now, so no connection in receiver
              String.format("%s,p1,sender,demo,RUNNING,NORMAL,", createTime1),
              String.format("%s,p2,sender,demo,STOP,NORMAL,", createTime2)
            };
        assertResultSetEqual(resultSet, SHOW_PIPE_HEADER, expectedRetSet);
      }
      try {
        statement.execute("START PIPE p3;");
        Assert.fail();
      } catch (Exception e) {
        Assert.assertTrue(e.getMessage().contains("PIPE [p3] does not exist"));
      }
      try (ResultSet resultSet = statement.executeQuery("SHOW PIPE p1")) {
        String[] expectedRetSet =
            new String[] {String.format("%s,p1,sender,demo,RUNNING,NORMAL,", createTime1)};
        assertResultSetEqual(resultSet, SHOW_PIPE_HEADER, expectedRetSet);
      }
      statement.execute("STOP PIPE p1;");
      statement.execute("STOP PIPE p2;");
      try (ResultSet resultSet = statement.executeQuery("SHOW PIPE")) {
        String[] expectedRetSet =
            new String[] {
              String.format("%s,p1,sender,demo,STOP,NORMAL,", createTime1),
              String.format("%s,p2,sender,demo,STOP,NORMAL,", createTime2)
            };
        assertResultSetEqual(resultSet, SHOW_PIPE_HEADER, expectedRetSet);
      }
      try {
        statement.execute("STOP PIPE p3;");
        Assert.fail();
      } catch (Exception e) {
        Assert.assertTrue(e.getMessage().contains("PIPE [p3] does not exist"));
      }
      statement.execute("DROP PIPE p1;");
      statement.execute("DROP PIPE p2;");
      statement.execute("DROP PIPE p3;");
      try (ResultSet resultSet = statement.executeQuery("SHOW PIPE")) {
        String[] expectedRetSet = new String[] {};
        assertResultSetEqual(resultSet, SHOW_PIPE_HEADER, expectedRetSet);
      }
    } catch (Exception e) {
      e.printStackTrace();
      Assert.fail();
    }
  }

  private String getCreateTime(Statement statement, String pipeName) throws Exception {
    String createTime = "";
    try (ResultSet resultSet = statement.executeQuery("SHOW PIPE " + pipeName)) {
      Assert.assertTrue(resultSet.next());
      createTime = resultSet.getString(1);
    }
    Assert.assertNotEquals("", createTime);
    return createTime;
  }
}
