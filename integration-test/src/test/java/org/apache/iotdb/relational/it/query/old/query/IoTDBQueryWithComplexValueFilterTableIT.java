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
package org.apache.iotdb.relational.it.query.old.query;

import org.apache.iotdb.it.env.EnvFactory;
import org.apache.iotdb.it.framework.IoTDBTestRunner;
import org.apache.iotdb.itbase.category.TableClusterIT;
import org.apache.iotdb.itbase.category.TableLocalStandaloneIT;
import org.apache.iotdb.itbase.env.BaseEnv;

import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;

import static org.apache.iotdb.db.it.utils.TestUtils.prepareTableData;
import static org.junit.Assert.fail;

@RunWith(IoTDBTestRunner.class)
@Category({TableLocalStandaloneIT.class, TableClusterIT.class})
public class IoTDBQueryWithComplexValueFilterTableIT {
  private static final String DATABASE_NAME = "test";
  private static final String[] SQLs =
      new String[] {
        "CREATE DATABASE " + DATABASE_NAME,
        "USE " + DATABASE_NAME,
        "CREATE TABLE sg1(device STRING ID, s1 INT32 MEASUREMENT, s2 DOUBLE MEASUREMENT, s3 STRING MEASUREMENT, s4 DATE MEASUREMENT, s5 TIMESTAMP MEASUREMENT)",
        "insert into sg1(time,device,s1,s2,s3,s4,s5) values(0,'d1',0,0,'0','2024-01-01',0)",
        "insert into sg1(time,device,s1,s2,s3,s4,s5) values(1,'d1',1,1,'1','2024-01-02',1)",
        "insert into sg1(time,device,s1,s2) values(2,'d1',2,2)",
        "insert into sg1(time,device,s1,s2) values(3,'d1',3,3)",
        "insert into sg1(time,device,s1,s2) values(4,'d1',4,4)",
        "insert into sg1(time,device,s1,s2) values(5,'d1',5,5)",
        "insert into sg1(time,device,s1,s2) values(6,'d1',6,6)",
        "insert into sg1(time,device,s1,s2) values(7,'d1',7,7)",
        "insert into sg1(time,device,s1,s2) values(8,'d1',8,8)",
        "insert into sg1(time,device,s1,s2) values(9,'d1',9,9)"
      };

  @BeforeClass
  public static void setUp() throws Exception {
    EnvFactory.getEnv().initClusterEnvironment();
    prepareTableData(SQLs);
  }

  @AfterClass
  public static void tearDown() throws Exception {
    EnvFactory.getEnv().cleanClusterEnvironment();
  }

  @Test
  public void testRawQuery1() {
    try (Connection connection = EnvFactory.getEnv().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement statement = connection.createStatement()) {
      statement.execute("USE " + DATABASE_NAME);
      try (ResultSet resultSet =
          statement.executeQuery(
              "select s1 from sg1 where (time > 4 and s1 <= 6) or (s2 > 3 and time <= 5)")) {
        int cnt = 0;
        while (resultSet.next()) {
          cnt++;
        }
        Assert.assertEquals(3, cnt);
      }
    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  @Test
  public void testRawQuery2() {
    try (Connection connection = EnvFactory.getEnv().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement statement = connection.createStatement()) {
      statement.execute("USE " + DATABASE_NAME);
      try (ResultSet resultSet =
          statement.executeQuery(
              "select s1 from sg1 where (time > 4 and s1 <= 6) and (s2 > 3 and time <= 5)")) {
        int cnt = 0;
        while (resultSet.next()) {
          cnt++;
        }
        Assert.assertEquals(1, cnt);
      }
    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  @Test
  public void testRawQuery3() {
    try (Connection connection = EnvFactory.getEnv().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement statement = connection.createStatement()) {
      statement.execute("USE " + DATABASE_NAME);

      try (ResultSet resultSet = statement.executeQuery("select s1 from sg1 where s3 = '1'")) {
        int cnt = 0;
        while (resultSet.next()) {
          cnt++;
        }
        Assert.assertEquals(1, cnt);
      }

      try (ResultSet resultSet =
          statement.executeQuery("select s1 from sg1 where s4 = CAST('2024-01-01' AS DATE)")) {
        int cnt = 0;
        while (resultSet.next()) {
          cnt++;
        }
        Assert.assertEquals(1, cnt);
      }

      try (ResultSet resultSet = statement.executeQuery("select s1 from sg1 where s5 = 1")) {
        int cnt = 0;
        while (resultSet.next()) {
          cnt++;
        }
        Assert.assertEquals(1, cnt);
      }

      try (ResultSet resultSet = statement.executeQuery("select s1 from sg1 where s3 != '1'")) {
        int cnt = 0;
        while (resultSet.next()) {
          cnt++;
        }
        Assert.assertEquals(1, cnt);
      }

      try (ResultSet resultSet =
          statement.executeQuery("select s1 from sg1 where s4 != CAST('2024-01-01' AS DATE)")) {
        int cnt = 0;
        while (resultSet.next()) {
          cnt++;
        }
        Assert.assertEquals(1, cnt);
      }

      try (ResultSet resultSet = statement.executeQuery("select s1 from sg1 where s5 != 1")) {
        int cnt = 0;
        while (resultSet.next()) {
          cnt++;
        }
        Assert.assertEquals(1, cnt);
      }
    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  @Test
  public void testRawQuery4() {
    try (Connection connection = EnvFactory.getEnv().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement statement = connection.createStatement()) {
      statement.execute("USE " + DATABASE_NAME);
      try (ResultSet resultSet = statement.executeQuery("select s1 from sg1 where s3 > '0'")) {
        int cnt = 0;
        while (resultSet.next()) {
          cnt++;
        }
        Assert.assertEquals(1, cnt);
      }

      try (ResultSet resultSet =
          statement.executeQuery("select s1 from sg1 where s4 > CAST('2024-01-01' AS DATE)")) {
        int cnt = 0;
        while (resultSet.next()) {
          cnt++;
        }
        Assert.assertEquals(1, cnt);
      }

      try (ResultSet resultSet = statement.executeQuery("select s1 from sg1 where s5 > 0")) {
        int cnt = 0;
        while (resultSet.next()) {
          cnt++;
        }
        Assert.assertEquals(1, cnt);
      }

      try (ResultSet resultSet = statement.executeQuery("select s1 from sg1 where s3 < '1'")) {
        int cnt = 0;
        while (resultSet.next()) {
          cnt++;
        }
        Assert.assertEquals(1, cnt);
      }

      try (ResultSet resultSet =
          statement.executeQuery("select s1 from sg1 where s4 < CAST('2024-01-02' AS DATE)")) {
        int cnt = 0;
        while (resultSet.next()) {
          cnt++;
        }
        Assert.assertEquals(1, cnt);
      }

      try (ResultSet resultSet = statement.executeQuery("select s1 from sg1 where s5 < 1")) {
        int cnt = 0;
        while (resultSet.next()) {
          cnt++;
        }
        Assert.assertEquals(1, cnt);
      }

      try (ResultSet resultSet = statement.executeQuery("select s1 from sg1 where s3 >= '1'")) {
        int cnt = 0;
        while (resultSet.next()) {
          cnt++;
        }
        Assert.assertEquals(1, cnt);
      }

      try (ResultSet resultSet =
          statement.executeQuery("select s1 from sg1 where s4 >= CAST('2024-01-01' AS DATE)")) {
        int cnt = 0;
        while (resultSet.next()) {
          cnt++;
        }
        Assert.assertEquals(2, cnt);
      }

      try (ResultSet resultSet = statement.executeQuery("select s1 from sg1 where s5 >= 1")) {
        int cnt = 0;
        while (resultSet.next()) {
          cnt++;
        }
        Assert.assertEquals(1, cnt);
      }

      try (ResultSet resultSet = statement.executeQuery("select s1 from sg1 where s3 <= '1'")) {
        int cnt = 0;
        while (resultSet.next()) {
          cnt++;
        }
        Assert.assertEquals(2, cnt);
      }

      try (ResultSet resultSet =
          statement.executeQuery("select s1 from sg1 where s4 <= CAST('2024-01-01' AS DATE)")) {
        int cnt = 0;
        while (resultSet.next()) {
          cnt++;
        }
        Assert.assertEquals(1, cnt);
      }

      try (ResultSet resultSet = statement.executeQuery("select s1 from sg1 where s5 <= 1")) {
        int cnt = 0;
        while (resultSet.next()) {
          cnt++;
        }
        Assert.assertEquals(2, cnt);
      }
    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  @Test
  public void testRawQuery5() {
    try (Connection connection = EnvFactory.getEnv().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement statement = connection.createStatement()) {
      statement.execute("USE " + DATABASE_NAME);
      try (ResultSet resultSet = statement.executeQuery("select s1 from sg1 where s3 = '1'")) {
        int cnt = 0;
        while (resultSet.next()) {
          cnt++;
        }
        Assert.assertEquals(1, cnt);
      }

      try (ResultSet resultSet = statement.executeQuery("select s1 from sg1 where s3 = '1'")) {
        int cnt = 0;
        while (resultSet.next()) {
          cnt++;
        }
        Assert.assertEquals(1, cnt);
      }

      try (ResultSet resultSet =
          statement.executeQuery("select s1 from sg1 where s4 = CAST('2024-01-01' AS DATE)")) {
        int cnt = 0;
        while (resultSet.next()) {
          cnt++;
        }
        Assert.assertEquals(1, cnt);
      }

      try (ResultSet resultSet = statement.executeQuery("select s1 from sg1 where s5 = 1")) {
        int cnt = 0;
        while (resultSet.next()) {
          cnt++;
        }
        Assert.assertEquals(1, cnt);
      }
    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }
}
