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

import org.apache.iotdb.db.mpp.common.header.ColumnHeader;
import org.apache.iotdb.db.mpp.common.header.ColumnHeaderConstant;
import org.apache.iotdb.it.env.EnvFactory;
import org.apache.iotdb.it.framework.IoTDBTestRunner;
import org.apache.iotdb.itbase.category.ClusterIT;
import org.apache.iotdb.itbase.category.LocalStandaloneIT;

import org.apache.commons.lang3.StringUtils;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.apache.iotdb.db.it.utils.TestUtils.assertResultSetEqual;
import static org.awaitility.Awaitility.await;

@RunWith(IoTDBTestRunner.class)
@Category({LocalStandaloneIT.class, ClusterIT.class})
public class IoTDBPipeIT {
  private static String ip;
  private static int port;
  private static final String SHOW_PIPE_HEADER =
      StringUtils.join(
              ColumnHeaderConstant.showPipeColumnHeaders.stream()
                  .map(ColumnHeader::getColumnName)
                  .toArray(),
              ",")
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
      statement.execute(
          "CREATE PIPE p1 to demo FROM (select ** from root where time>=1648569600000) WITH SyncDelOp=false;");
      statement.execute("CREATE PIPE p2 to demo WITH SyncDelOp=true;");
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
              String.format(
                  "%s,p1,sender,demo,STOP,SyncDelOp=false,DataStartTimestamp=1648569600000,NORMAL,",
                  createTime1),
              String.format(
                  "%s,p2,sender,demo,STOP,SyncDelOp=true,DataStartTimestamp=0,NORMAL,", createTime2)
            };
        assertResultSetEqual(resultSet, SHOW_PIPE_HEADER, expectedRetSet);
      }
      statement.execute("START PIPE p1;");
      await()
          .atMost(5, SECONDS)
          .until(
              () -> {
                try (ResultSet resultSet = statement.executeQuery("SHOW PIPE")) {
                  int cnt = 0;
                  while (resultSet.next()) {
                    cnt++;
                  }
                  return cnt == 2;
                }
              });
      try (ResultSet resultSet = statement.executeQuery("SHOW PIPE")) {
        String[] expectedRetSet =
            new String[] {
              // there is no data now, so no connection in receiver
              String.format(
                  "%s,p1,sender,demo,RUNNING,SyncDelOp=false,DataStartTimestamp=1648569600000,NORMAL,",
                  createTime1),
              String.format(
                  "%s,p2,sender,demo,STOP,SyncDelOp=true,DataStartTimestamp=0,NORMAL,", createTime2)
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
            new String[] {
              String.format(
                  "%s,p1,sender,demo,RUNNING,SyncDelOp=false,DataStartTimestamp=1648569600000,NORMAL,",
                  createTime1)
            };
        assertResultSetEqual(resultSet, SHOW_PIPE_HEADER, expectedRetSet);
      }
      statement.execute("STOP PIPE p1;");
      statement.execute("STOP PIPE p2;");
      try (ResultSet resultSet = statement.executeQuery("SHOW PIPE")) {
        String[] expectedRetSet =
            new String[] {
              String.format(
                  "%s,p1,sender,demo,STOP,SyncDelOp=false,DataStartTimestamp=1648569600000,NORMAL,",
                  createTime1),
              String.format(
                  "%s,p2,sender,demo,STOP,SyncDelOp=true,DataStartTimestamp=0,NORMAL,", createTime2)
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
      try {
        statement.execute("DROP PIPE p3;");
        Assert.fail();
      } catch (Exception e) {
        Assert.assertTrue(e.getMessage().contains("PIPE [p3] does not exist"));
      }
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
