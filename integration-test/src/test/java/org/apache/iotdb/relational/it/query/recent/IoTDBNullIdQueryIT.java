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

package org.apache.iotdb.relational.it.query.recent;

import org.apache.iotdb.it.env.EnvFactory;
import org.apache.iotdb.it.framework.IoTDBTestRunner;
import org.apache.iotdb.itbase.category.TableClusterIT;
import org.apache.iotdb.itbase.category.TableLocalStandaloneIT;
import org.apache.iotdb.itbase.env.BaseEnv;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;

import static org.apache.iotdb.db.it.utils.TestUtils.defaultFormatDataTime;
import static org.apache.iotdb.db.it.utils.TestUtils.prepareTableData;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

@RunWith(IoTDBTestRunner.class)
@Category({TableLocalStandaloneIT.class, TableClusterIT.class})
public class IoTDBNullIdQueryIT {
  private static final String DATABASE_NAME = "test";
  private static final String[] createSqls =
      new String[] {
        "CREATE DATABASE " + DATABASE_NAME,
        "USE " + DATABASE_NAME,
        "CREATE TABLE testNullId(id1 STRING ID, id2 STRING ID, s1 INT32 MEASUREMENT, s2 BOOLEAN MEASUREMENT, s3 DOUBLE MEASUREMENT)",
        "INSERT INTO testNullId(time,id1,id2,s1,s2,s3) " + "values(1, null, null, 0, false, 11.1)",
      };

  @BeforeClass
  public static void setUp() throws Exception {
    EnvFactory.getEnv().initClusterEnvironment();
    prepareTableData(createSqls);
  }

  @AfterClass
  public static void tearDown() throws Exception {
    EnvFactory.getEnv().cleanClusterEnvironment();
  }

  @Test
  public void nullFilterTest() throws Exception {
    String result = defaultFormatDataTime(1) + ",0,false,11.1";
    try (final Connection connectionIsNull =
            EnvFactory.getEnv().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        final Statement statement = connectionIsNull.createStatement()) {
      statement.execute("USE " + DATABASE_NAME);

      ResultSet resultSet = statement.executeQuery("select * from testNullId where id1 is null");
      assertTrue(resultSet.next());
      String ans =
          resultSet.getString("time")
              + ","
              + resultSet.getString("s1")
              + ","
              + resultSet.getString("s2")
              + ","
              + resultSet.getString("s3");
      assertEquals(result, ans);
      assertFalse(resultSet.next());

      resultSet = statement.executeQuery("select * from testNullId where id1 is not null");
      assertFalse(resultSet.next());

      resultSet = statement.executeQuery("select * from testNullId where id1 like '%'");
      assertFalse(resultSet.next());

      resultSet =
          statement.executeQuery("select * from testNullId where id1 is null and id2 is null");
      assertTrue(resultSet.next());
      ans =
          resultSet.getString("time")
              + ","
              + resultSet.getString("s1")
              + ","
              + resultSet.getString("s2")
              + ","
              + resultSet.getString("s3");
      assertEquals(result, ans);
      assertFalse(resultSet.next());

      // The second time we read from cache
      resultSet =
          statement.executeQuery("select * from testNullId where id1 is null and id2 is null");
      assertTrue(resultSet.next());
      ans =
          resultSet.getString("time")
              + ","
              + resultSet.getString("s1")
              + ","
              + resultSet.getString("s2")
              + ","
              + resultSet.getString("s3");
      assertEquals(result, ans);
      assertFalse(resultSet.next());

      // Test deduplication
      resultSet =
          statement.executeQuery("select * from testNullId where id1 is null or id2 is null");
      assertTrue(resultSet.next());
      ans =
          resultSet.getString("time")
              + ","
              + resultSet.getString("s1")
              + ","
              + resultSet.getString("s2")
              + ","
              + resultSet.getString("s3");
      assertEquals(result, ans);
      assertFalse(resultSet.next());

      // Test constant select item
      resultSet = statement.executeQuery("select *, 1 from testNullId");
      result = defaultFormatDataTime(1) + ",null,null,0,false,11.1,1";
      assertTrue(resultSet.next());
      ans =
          resultSet.getString("time")
              + ","
              + resultSet.getString("id1")
              + ","
              + resultSet.getString("id2")
              + ","
              + resultSet.getString("s1")
              + ","
              + resultSet.getString("s2")
              + ","
              + resultSet.getString("s3")
              + ","
              + resultSet.getString("_col6");

      assertEquals(result, ans);
      assertFalse(resultSet.next());

      // Test boolean between
      resultSet =
          statement.executeQuery("select * from testNullId where s2 between false and true");
      result = defaultFormatDataTime(1) + ",null,null,0,false,11.1";
      assertTrue(resultSet.next());
      ans =
          resultSet.getString("time")
              + ","
              + resultSet.getString("id1")
              + ","
              + resultSet.getString("id2")
              + ","
              + resultSet.getString("s1")
              + ","
              + resultSet.getString("s2")
              + ","
              + resultSet.getString("s3");

      assertEquals(result, ans);
      assertFalse(resultSet.next());

      // Test boolean not between
      resultSet =
          statement.executeQuery("select * from testNullId where s2 not between false and true");
      assertFalse(resultSet.next());
    }
  }
}
