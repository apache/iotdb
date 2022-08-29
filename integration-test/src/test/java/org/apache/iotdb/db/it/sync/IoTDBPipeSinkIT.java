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
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;

import static org.apache.iotdb.db.it.utils.TestUtils.assertResultSetEqual;

@RunWith(IoTDBTestRunner.class)
@Category({LocalStandaloneIT.class})
public class IoTDBPipeSinkIT {

  @BeforeClass
  public static void setUp() throws Exception {
    EnvFactory.getEnv().initBeforeClass();
  }

  @AfterClass
  public static void tearDown() throws Exception {
    EnvFactory.getEnv().cleanAfterClass();
  }

  @Test
  public void testShowPipeSinkType() {
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      String expectedHeader = ColumnHeaderConstant.COLUMN_PIPESINK_TYPE + ",";
      String[] expectedRetSet = new String[] {"IoTDB,", "ExternalPipe,"};
      try (ResultSet resultSet = statement.executeQuery("SHOW PIPESINKTYPE")) {
        assertResultSetEqual(resultSet, expectedHeader, expectedRetSet);
      }
    } catch (Exception e) {
      e.printStackTrace();
      Assert.fail();
    }
  }

  @Test
  public void testOperatePipeSink() {
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      statement.execute("CREATE PIPESINK demo1 AS IoTDB (ip='192.168.0.1',port='6677');");
      statement.execute("CREATE PIPESINK demo2 AS IoTDB (ip='192.168.0.2',port='6678');");
      statement.execute("CREATE PIPESINK demo3 AS IoTDB;");
      statement.execute("DROP PIPESINK demo2;");
      String expectedHeader =
          ColumnHeaderConstant.COLUMN_PIPESINK_NAME
              + ","
              + ColumnHeaderConstant.COLUMN_PIPESINK_TYPE
              + ","
              + ColumnHeaderConstant.COLUMN_PIPESINK_ATTRIBUTES
              + ",";
      try (ResultSet resultSet = statement.executeQuery("SHOW PIPESINK")) {
        String[] expectedRetSet =
            new String[] {
              "demo3,IoTDB,ip='127.0.0.1',port=6667,", "demo1,IoTDB,ip='192.168.0.1',port=6677,"
            };
        assertResultSetEqual(resultSet, expectedHeader, expectedRetSet);
      }
      try (ResultSet resultSet = statement.executeQuery("SHOW PIPESINK demo3")) {
        String[] expectedRetSet = new String[] {"demo3,IoTDB,ip='127.0.0.1',port=6667,"};
        assertResultSetEqual(resultSet, expectedHeader, expectedRetSet);
      }
    } catch (Exception e) {
      e.printStackTrace();
      Assert.fail();
    }
  }
}
