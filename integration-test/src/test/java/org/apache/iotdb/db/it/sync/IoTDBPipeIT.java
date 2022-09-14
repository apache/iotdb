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
import org.apache.iotdb.itbase.category.LocalStandaloneIT;

import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;

import static org.apache.iotdb.db.it.utils.TestUtils.assertResultSetEqual;

// TODO: this test only support for new standalone now
@Ignore
@RunWith(IoTDBTestRunner.class)
@Category({LocalStandaloneIT.class})
public class IoTDBPipeIT {

  @BeforeClass
  public static void setUp() throws Exception {
    EnvFactory.getEnv().initBeforeClass();
  }

  @AfterClass
  public static void tearDown() throws Exception {
    EnvFactory.getEnv().cleanAfterClass();
  }

  @Test
  public void testOperatePipe() {
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      String ip = EnvFactory.getEnv().getDataNodeWrapperList().get(0).getIp();
      int port = EnvFactory.getEnv().getDataNodeWrapperList().get(0).getPort();
      statement.execute(
          String.format("CREATE PIPESINK demo AS IoTDB (ip='%s',port='%d');", ip, port));
      statement.execute("CREATE PIPE p to demo;");
      String expectedHeader =
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

      String createTime = getCreateTime("p");
      try (ResultSet resultSet = statement.executeQuery("SHOW PIPE")) {
        String[] expectedRetSet =
            new String[] {String.format("%s,p,sender,demo,STOP,,", createTime)};
        assertResultSetEqual(resultSet, expectedHeader, expectedRetSet);
      }
      statement.execute("START PIPE p;");
      Thread.sleep(1000); // wait 1000 ms to start thread
      try (ResultSet resultSet = statement.executeQuery("SHOW PIPE")) {
        String[] expectedRetSet =
            new String[] {
              String.format("%s,p,sender,demo,RUNNING,,", createTime),
              String.format("%s,p,receiver,0.0.0.0,RUNNING,,", createTime),
            };
        assertResultSetEqual(resultSet, expectedHeader, expectedRetSet);
      }
      statement.execute("STOP PIPE p;");
      try (ResultSet resultSet = statement.executeQuery("SHOW PIPE")) {
        String[] expectedRetSet =
            new String[] {String.format("%s,p,sender,demo,STOP,,", createTime)};
        assertResultSetEqual(resultSet, expectedHeader, expectedRetSet);
      }
      statement.execute("DROP PIPE p;");
      try (ResultSet resultSet = statement.executeQuery("SHOW PIPE")) {
        String[] expectedRetSet =
            new String[] {String.format("%s,p,sender,demo,DROP,,", createTime)};
        assertResultSetEqual(resultSet, expectedHeader, expectedRetSet);
      }
    } catch (Exception e) {
      e.printStackTrace();
      Assert.fail();
    }
  }

  private String getCreateTime(String pipeName) throws Exception {
    String createTime = "";
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      try (ResultSet resultSet = statement.executeQuery("SHOW PIPE " + pipeName)) {
        Assert.assertTrue(resultSet.next());
        createTime = resultSet.getString(1);
      }
    }
    Assert.assertNotEquals("", createTime);
    return createTime;
  }
}
