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
package org.apache.iotdb.relational.it.db.it.aligned;

import org.apache.iotdb.it.env.EnvFactory;
import org.apache.iotdb.it.framework.IoTDBTestRunner;
import org.apache.iotdb.itbase.category.ClusterIT;
import org.apache.iotdb.itbase.category.LocalStandaloneIT;
import org.apache.iotdb.itbase.env.BaseEnv;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

import static org.junit.Assert.fail;

@RunWith(IoTDBTestRunner.class)
@Category({LocalStandaloneIT.class, ClusterIT.class})
public class IoTDBInsertAlignedValues4IT {

  @Before
  public void setUp() throws Exception {
    EnvFactory.getEnv()
        .getConfig()
        .getCommonConfig()
        .setAutoCreateSchemaEnabled(true)
        .setPrimitiveArraySize(2);
    EnvFactory.getEnv().initClusterEnvironment();
  }

  @After
  public void tearDown() throws Exception {
    EnvFactory.getEnv().cleanClusterEnvironment();
  }

  @Test
  public void testExtendTextColumn() {

    try (Connection connection = EnvFactory.getEnv().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement statement = connection.createStatement()) {
      statement.execute("create database test");
      statement.execute("use \"test\"");
      statement.execute(
          "create table sg (id1 string id, s1 string measurement, s2 string measurement)");
      statement.execute("insert into sg(id1,time,s1,s2) values('d1',1,'test','test')");
      statement.execute("insert into sg(id1,time,s1,s2) values('d1',2,'test','test')");
      statement.execute("insert into sg(id1,time,s1,s2) values('d1',3,'test','test')");
      statement.execute("insert into sg(id1,time,s1,s2) values('d1',4,'test','test')");
      statement.execute("insert into sg(id1,time,s1,s3) values('d1',5,'test','test')");
      statement.execute("insert into sg(id1,time,s1,s2) values('d1',6,'test','test')");
      statement.execute("flush");
      statement.execute("insert into sg(id1,time,s1,s3) values('d1',7,'test','test')");
      fail();
    } catch (SQLException ignored) {
    }
  }
}
