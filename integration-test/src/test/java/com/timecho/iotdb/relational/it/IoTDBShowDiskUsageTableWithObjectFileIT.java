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

package com.timecho.iotdb.relational.it;

import org.apache.iotdb.isession.ITableSession;
import org.apache.iotdb.isession.SessionDataSet;
import org.apache.iotdb.it.env.EnvFactory;
import org.apache.iotdb.it.framework.IoTDBTestRunner;
import org.apache.iotdb.itbase.category.TableClusterIT;
import org.apache.iotdb.itbase.category.TableLocalStandaloneIT;
import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.StatementExecutionException;

import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

@RunWith(IoTDBTestRunner.class)
@Category({TableLocalStandaloneIT.class, TableClusterIT.class})
public class IoTDBShowDiskUsageTableWithObjectFileIT {

  @BeforeClass
  public static void setUp() throws Exception {
    EnvFactory.getEnv().initClusterEnvironment();
    try (ITableSession session = EnvFactory.getEnv().getTableSessionConnection()) {
      session.executeNonQueryStatement("create database test");
      session.executeNonQueryStatement("use test");
      session.executeNonQueryStatement(
          "create table t1(tag1 string tag, s0 int32 field, s1 int32 field, s2 object field)");
      for (int i = 0; i < 20; i++) {
        session.executeNonQueryStatement(
            "insert into t1(time,tag1,s0,s1) values (" + i + ", 'd1', 1, 1)");
      }
      session.executeNonQueryStatement("flush");
    }
  }

  @AfterClass
  public static void tearDown() throws Exception {
    EnvFactory.getEnv().cleanClusterEnvironment();
  }

  @Test
  public void test() throws IoTDBConnectionException, StatementExecutionException {
    try (ITableSession session = EnvFactory.getEnv().getTableSessionConnection()) {
      session.executeNonQueryStatement("use test");
      long tableSize = getTableSize(session, "t1", 0);
      session.executeNonQueryStatement(
          "insert into t1(time, tag1, s2) values (100, 'd1', to_object(true, 0, X'aabbccdd'))");
      session.executeNonQueryStatement(
          "insert into t1(time, tag1, s2) values (-100, 'd1', to_object(true, 0, X'aabbccdd'))");
      Assert.assertEquals(tableSize + 4, getTableSize(session, "t1", 0));
      Assert.assertEquals(4, getTableSize(session, "t1", -1));
      session.executeNonQueryStatement(
          "insert into t1(time, tag1, s2) values (100, 'd1', to_object(true, 0, X'aabbccddeeff'))");
      Assert.assertEquals(tableSize + 6, getTableSize(session, "t1", 0));
      session.executeNonQueryStatement("delete from t1 where time = 100");
      Assert.assertEquals(tableSize, getTableSize(session, "t1", 0));
      Assert.assertEquals(4, getTableSize(session, "t1", -1));
    }
  }

  private long getTableSize(ITableSession session, String tableName, long timePartition)
      throws StatementExecutionException, IoTDBConnectionException {
    SessionDataSet sessionDataSet =
        session.executeQueryStatement(
            "select sum(size_in_bytes) from information_schema.table_disk_usage where database = 'test' and table_name = '"
                + tableName
                + "' and time_partition="
                + timePartition);
    Assert.assertTrue(sessionDataSet.hasNext());
    return (long) sessionDataSet.next().getField(0).getDoubleV();
  }
}
