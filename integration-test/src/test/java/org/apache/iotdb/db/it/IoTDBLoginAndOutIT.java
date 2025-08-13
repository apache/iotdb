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

import org.apache.iotdb.it.env.EnvFactory;
import org.apache.iotdb.it.framework.IoTDBTestRunner;
import org.apache.iotdb.itbase.category.ClusterIT;
import org.apache.iotdb.itbase.category.LocalStandaloneIT;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;

import static org.junit.Assert.assertEquals;

@RunWith(IoTDBTestRunner.class)
@Category({LocalStandaloneIT.class, ClusterIT.class})
public class IoTDBLoginAndOutIT {

  @BeforeClass
  public static void setUp() throws Exception {
    // use small page
    EnvFactory.getEnv().initClusterEnvironment();
  }

  @AfterClass
  public static void tearDown() throws Exception {
    EnvFactory.getEnv().cleanClusterEnvironment();
  }

  @Test
  public void testRepeatedlyLoginAndOut() throws Exception {
    int attempts = 100;
    for (int i = 0; i < attempts; i++) {
      try (Connection ignored = EnvFactory.getEnv().getConnection()) {
        // do nothing
      }
    }
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      int rowCount = 0;
      ResultSet resultSet = statement.executeQuery("SHOW QUERIES");
      int columnCount = resultSet.getMetaData().getColumnCount();
      for (int i = 0; i < columnCount; i++) {
        System.out.printf("%s, ", resultSet.getMetaData().getColumnName(i + 1));
      }
      System.out.println();
      while (resultSet.next()) {
        for (int i = 0; i < columnCount; i++) {
          System.out.printf("%s, ", resultSet.getString(i + 1));
        }
        System.out.println();
        rowCount++;
      }
      assertEquals(1, rowCount);
    }
  }
}
