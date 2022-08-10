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

import org.apache.iotdb.it.env.ConfigFactory;
import org.apache.iotdb.it.env.EnvFactory;
import org.apache.iotdb.it.framework.IoTDBTestRunner;
import org.apache.iotdb.itbase.category.ClusterIT;
import org.apache.iotdb.itbase.category.LocalStandaloneIT;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;

import static org.junit.Assert.assertEquals;

@RunWith(IoTDBTestRunner.class)
@Category({LocalStandaloneIT.class, ClusterIT.class})
public class IoTDBDeletePartitionIT {

  private static int partitionInterval = 1;

  @Before
  public void setUp() throws Exception {
    ConfigFactory.getConfig().setEnablePartition(true);
    ConfigFactory.getConfig().setPartitionInterval(partitionInterval);
    EnvFactory.getEnv().initBeforeClass();
  }

  @After
  public void tearDown() throws Exception {
    ConfigFactory.getConfig().setEnablePartition(false);
    ConfigFactory.getConfig().setPartitionInterval(-1);
    EnvFactory.getEnv().cleanAfterClass();
  }

  @Test
  public void testRemoveOnePartitionAndInsertData() {
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      statement.execute("set storage group to root.test");
      statement.execute("insert into root.test.wf02.wt02(timestamp,status) values(1,true)");
      statement.execute("select * from root.test.wf02.wt02");
      statement.execute("DELETE PARTITION root.test 0");
      statement.execute("select * from root.test.wf02.wt02");
      statement.execute("insert into root.test.wf02.wt02(timestamp,status) values(1,true)");
      try (ResultSet resultSet = statement.executeQuery("select * from root.test.wf02.wt02")) {
        assertEquals(true, resultSet.next());
      }
      statement.execute("flush");
      try (ResultSet resultSet = statement.executeQuery("select * from root.test.wf02.wt02")) {
        assertEquals(true, resultSet.next());
      }
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  @Test
  public void testFlushAndRemoveOnePartitionAndInsertData() {
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      statement.execute("set storage group to root.test");
      statement.execute("insert into root.test.wf02.wt02(timestamp,status) values(1,true)");
      statement.execute("flush");
      statement.execute("DELETE PARTITION root.test 0");
      statement.execute("select * from root.test.wf02.wt02");
      statement.execute("insert into root.test.wf02.wt02(timestamp,status) values(1,true)");
      try (ResultSet resultSet = statement.executeQuery("select * from root.test.wf02.wt02")) {
        assertEquals(true, resultSet.next());
      }
      statement.execute("flush");
      try (ResultSet resultSet = statement.executeQuery("select * from root.test.wf02.wt02")) {
        assertEquals(true, resultSet.next());
      }
    } catch (Exception e) {
      e.printStackTrace();
    }
  }
}
