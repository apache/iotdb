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
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Locale;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

@RunWith(IoTDBTestRunner.class)
@Category({LocalStandaloneIT.class, ClusterIT.class})
public class IoTDBFileTimeIndexIT {

  private static final String[] sqls =
      new String[] {
        "insert into root.db.d1(time,s1) values(2,2)",
        "insert into root.db.d1(time,s1) values(3,3)",
        "flush",
        "insert into root.db.d2(time,s1) values(5,5)",
        "flush",
        "insert into root.db.d1(time,s1) values(4,4)",
        "flush",
        "insert into root.db.d2(time,s1) values(1,1)",
        "insert into root.db.d1(time,s1) values(3,30)",
        "insert into root.db.d1(time,s1) values(4,40)",
        "flush",
        "insert into root.db.d2(time,s1) values(2,2)",
        "insert into root.db.d1(time,s1) values(4,400)",
        "flush",
      };

  @BeforeClass
  public static void setUp() throws Exception {
    Locale.setDefault(Locale.ENGLISH);

    EnvFactory.getEnv()
        .getConfig()
        .getCommonConfig()
        .setDataRegionGroupExtensionPolicy("CUSTOM")
        .setDefaultDataRegionGroupNumPerDatabase(1)
        .setEnableSeqSpaceCompaction(false)
        .setEnableUnseqSpaceCompaction(false)
        .setEnableCrossSpaceCompaction(false)
        .setQueryMemoryProportion("1:100:200:50:200:200:0:250");
    // Adjust memstable threshold size to make it flush automatically
    EnvFactory.getEnv().initClusterEnvironment();
    prepareData();
  }

  private static void prepareData() {
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {

      for (String sql : sqls) {
        statement.addBatch(sql);
      }
      statement.executeBatch();
    } catch (Exception e) {
      fail(e.getMessage());
    }
  }

  @AfterClass
  public static void tearDown() throws Exception {
    EnvFactory.getEnv().cleanClusterEnvironment();
  }

  @Test
  public void testQuery() throws SQLException {
    long[] time = {2L, 3L, 4L};
    double[] value = {2.0f, 30.0f, 400.0f};

    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement();
        ResultSet resultSet = statement.executeQuery("select s1 from root.db.d1")) {
      int cnt = 0;
      while (resultSet.next()) {
        assertEquals(time[cnt], resultSet.getLong(1));
        assertEquals(value[cnt], resultSet.getDouble(2), 0.00001);
        cnt++;
      }
      assertEquals(time.length, cnt);
    }
  }
}
