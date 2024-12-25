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
package org.apache.iotdb.relational.it.query.old.alignbydevice;

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
import java.sql.SQLException;
import java.sql.Statement;

import static org.apache.iotdb.db.it.utils.TestUtils.tableAssertTestFail;
import static org.apache.iotdb.db.it.utils.TestUtils.tableResultSetEqualTest;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

@RunWith(IoTDBTestRunner.class)
@Category({TableLocalStandaloneIT.class, TableClusterIT.class})
public class IoTDBAlignByDeviceWithTemplateTableIT {

  private static final String DATABASE_NAME = "db";
  private static final String[] sqls =
      new String[] {
        "CREATE database " + DATABASE_NAME,
        "use " + DATABASE_NAME,
        "create table table1(device_id STRING ID, s1 FLOAT MEASUREMENT, s2 BOOLEAN MEASUREMENT, s3 INT32 MEASUREMENT)",
        "INSERT INTO table1(Time, device_id, s1, s2, s3) VALUES (1,'d1', 1.1, false, 1), (2, 'd1', 2.2, false, 2), (1,'d2', 11.1, false, 11), (2,'d2', 22.2, false, 22), (1,'d3', 111.1, true, null), (4,'d3', 444.4, true, 44), (1,'d4', 1111.1, true, 1111), (5,'d4', 5555.5, false, 5555)",
      };

  @BeforeClass
  public static void setUp() throws Exception {
    EnvFactory.getEnv().initClusterEnvironment();
    insertData();
  }

  @AfterClass
  public static void tearDown() throws Exception {
    EnvFactory.getEnv().cleanClusterEnvironment();
  }

  @Test
  public void selectWildcardNoFilterTest() {
    // 1. order by device_id
    String[] expectedHeader = new String[] {"time", "device_id", "s3", "s1", "s2"};
    String[] retArray =
        new String[] {
          "1970-01-01T00:00:00.001Z,d1,1,1.1,false,",
          "1970-01-01T00:00:00.002Z,d1,2,2.2,false,",
          "1970-01-01T00:00:00.001Z,d2,11,11.1,false,",
          "1970-01-01T00:00:00.002Z,d2,22,22.2,false,",
          "1970-01-01T00:00:00.001Z,d3,null,111.1,true,",
          "1970-01-01T00:00:00.004Z,d3,44,444.4,true,",
          "1970-01-01T00:00:00.001Z,d4,1111,1111.1,true,",
          "1970-01-01T00:00:00.005Z,d4,5555,5555.5,false,",
        };
    tableResultSetEqualTest(
        "SELECT time, device_id, s3, s1, s2 FROM table1 order by device_id",
        expectedHeader,
        retArray,
        DATABASE_NAME);

    expectedHeader = new String[] {"time", "device_id", "s1", "s2", "s3", "s1"};
    retArray =
        new String[] {
          "1970-01-01T00:00:00.001Z,d1,1.1,false,1,1.1,",
          "1970-01-01T00:00:00.002Z,d1,2.2,false,2,2.2,",
          "1970-01-01T00:00:00.001Z,d2,11.1,false,11,11.1,",
          "1970-01-01T00:00:00.002Z,d2,22.2,false,22,22.2,",
          "1970-01-01T00:00:00.001Z,d3,111.1,true,null,111.1,",
          "1970-01-01T00:00:00.004Z,d3,444.4,true,44,444.4,",
          "1970-01-01T00:00:00.001Z,d4,1111.1,true,1111,1111.1,",
          "1970-01-01T00:00:00.005Z,d4,5555.5,false,5555,5555.5,",
        };
    tableResultSetEqualTest(
        "SELECT *, s1 FROM table1 order by device_id", expectedHeader, retArray, DATABASE_NAME);

    expectedHeader = new String[] {"time", "device_id", "s1", "s2", "s3"};
    retArray =
        new String[] {
          "1970-01-01T00:00:00.001Z,d1,1.1,false,1,",
          "1970-01-01T00:00:00.002Z,d1,2.2,false,2,",
          "1970-01-01T00:00:00.001Z,d2,11.1,false,11,",
          "1970-01-01T00:00:00.002Z,d2,22.2,false,22,",
        };
    tableResultSetEqualTest(
        "SELECT * FROM table1 where device_id = 'd1' or device_id = 'd2' or device_id = 'd6' order by device_id",
        expectedHeader,
        retArray,
        DATABASE_NAME);

    // 2. order by device_id + limit/offset
    expectedHeader = new String[] {"time", "device_id", "s1", "s2", "s3"};
    retArray =
        new String[] {
          "1970-01-01T00:00:00.002Z,d1,2.2,false,2,", "1970-01-01T00:00:00.001Z,d2,11.1,false,11,",
        };
    tableResultSetEqualTest(
        "SELECT * FROM table1 order by device_id, time OFFSET 1 LIMIT 2",
        expectedHeader,
        retArray,
        DATABASE_NAME);

    // 3. order by time
    retArray =
        new String[] {
          "1970-01-01T00:00:00.005Z,d4,5555.5,false,5555,",
          "1970-01-01T00:00:00.004Z,d3,444.4,true,44,",
          "1970-01-01T00:00:00.002Z,d1,2.2,false,2,",
          "1970-01-01T00:00:00.002Z,d2,22.2,false,22,",
          "1970-01-01T00:00:00.001Z,d1,1.1,false,1,",
          "1970-01-01T00:00:00.001Z,d2,11.1,false,11,",
          "1970-01-01T00:00:00.001Z,d3,111.1,true,null,",
          "1970-01-01T00:00:00.001Z,d4,1111.1,true,1111,",
        };
    tableResultSetEqualTest(
        "SELECT * FROM table1 ORDER BY time DESC, device_id ASC",
        expectedHeader,
        retArray,
        DATABASE_NAME);

    // 4. order by time + limit/offset
    retArray =
        new String[] {
          "1970-01-01T00:00:00.005Z,d4,5555.5,false,5555,",
          "1970-01-01T00:00:00.004Z,d3,444.4,true,44,",
          "1970-01-01T00:00:00.002Z,d1,2.2,false,2,",
          "1970-01-01T00:00:00.002Z,d2,22.2,false,22,",
        };
    tableResultSetEqualTest(
        "SELECT * FROM table1 ORDER BY time DESC, device_id LIMIT 4",
        expectedHeader,
        retArray,
        DATABASE_NAME);
  }

  @Test
  public void selectMeasurementNoFilterTest() {
    // 1. order by device_id
    String[] expectedHeader = new String[] {"time", "device_id", "s3", "s1"};
    String[] retArray =
        new String[] {
          "1970-01-01T00:00:00.001Z,d1,1,1.1,",
          "1970-01-01T00:00:00.002Z,d1,2,2.2,",
          "1970-01-01T00:00:00.001Z,d2,11,11.1,",
          "1970-01-01T00:00:00.002Z,d2,22,22.2,",
          "1970-01-01T00:00:00.001Z,d3,null,111.1,",
          "1970-01-01T00:00:00.004Z,d3,44,444.4,",
          "1970-01-01T00:00:00.001Z,d4,1111,1111.1,",
          "1970-01-01T00:00:00.005Z,d4,5555,5555.5,",
        };
    tableResultSetEqualTest(
        "SELECT time, device_id, s3, s1 FROM table1 order by device_id",
        expectedHeader,
        retArray,
        DATABASE_NAME);

    try (Connection connection = EnvFactory.getEnv().getConnection(BaseEnv.TABLE_SQL_DIALECT)) {
      try (Statement statement = connection.createStatement()) {
        statement.execute("use " + DATABASE_NAME);
        try (ResultSet resultSet =
            statement.executeQuery("SELECT s3,s1,s_null FROM table1 order by device_id")) {
          fail("should throw exception to indicate that s_null doesn't exist");
        } catch (SQLException e) {
          assertEquals("701: Column 's_null' cannot be resolved", e.getMessage());
        }
      }
    } catch (SQLException e) {
      e.printStackTrace();
      fail(e.getMessage());
    }

    // 2. order by device + limit/offset
    retArray =
        new String[] {
          "1970-01-01T00:00:00.002Z,d1,2,2.2,", "1970-01-01T00:00:00.001Z,d2,11,11.1,",
        };
    tableResultSetEqualTest(
        "SELECT time, device_id, s3,s1 FROM table1 order by device_id,time OFFSET 1 LIMIT 2",
        expectedHeader,
        retArray,
        DATABASE_NAME);

    // 3. order by time
    retArray =
        new String[] {
          "1970-01-01T00:00:00.005Z,d4,5555,5555.5,",
          "1970-01-01T00:00:00.004Z,d3,44,444.4,",
          "1970-01-01T00:00:00.002Z,d1,2,2.2,",
          "1970-01-01T00:00:00.002Z,d2,22,22.2,",
          "1970-01-01T00:00:00.001Z,d1,1,1.1,",
          "1970-01-01T00:00:00.001Z,d2,11,11.1,",
          "1970-01-01T00:00:00.001Z,d3,null,111.1,",
          "1970-01-01T00:00:00.001Z,d4,1111,1111.1,",
        };
    tableResultSetEqualTest(
        "SELECT time, device_id, s3,s1 FROM table1 ORDER BY TIME DESC, device_id",
        expectedHeader,
        retArray,
        DATABASE_NAME);

    // 4. order by time + limit/offset
    retArray =
        new String[] {
          "1970-01-01T00:00:00.005Z,d4,5555,5555.5,",
          "1970-01-01T00:00:00.004Z,d3,44,444.4,",
          "1970-01-01T00:00:00.002Z,d1,2,2.2,",
          "1970-01-01T00:00:00.002Z,d2,22,22.2,",
        };
    tableResultSetEqualTest(
        "SELECT time, device_id, s3,s1 FROM table1 ORDER BY time DESC, device_id LIMIT 4",
        expectedHeader,
        retArray,
        DATABASE_NAME);
  }

  @Test
  public void selectWildcardWithFilterOrderByTimeTest() {
    // 1. order by time + time filter
    String[] expectedHeader = new String[] {"time", "device_id", "s1", "s2", "s3"};
    String[] retArray =
        new String[] {
          "1970-01-01T00:00:00.004Z,d3,444.4,true,44,",
          "1970-01-01T00:00:00.002Z,d1,2.2,false,2,",
          "1970-01-01T00:00:00.002Z,d2,22.2,false,22,",
          "1970-01-01T00:00:00.001Z,d1,1.1,false,1,",
        };
    tableResultSetEqualTest(
        "SELECT * FROM table1 WHERE time < 5 ORDER BY TIME desc, device_id LIMIT 4",
        expectedHeader,
        retArray,
        DATABASE_NAME);

    // 2. order by time + time filter + value filter
    retArray =
        new String[] {
          "1970-01-01T00:00:00.004Z,d3,444.4,true,44,",
          "1970-01-01T00:00:00.002Z,d2,22.2,false,22,",
        };
    tableResultSetEqualTest(
        "SELECT * FROM table1 where time > 1 and time < 5 and s3>=11 and s3<=1111 and s1 != 11.1 "
            + "ORDER BY time DESC, device_id",
        expectedHeader,
        retArray,
        DATABASE_NAME);

    // 3. order by time + value filter: s_null > 1
    try (Connection connection = EnvFactory.getEnv().getConnection(BaseEnv.TABLE_SQL_DIALECT)) {
      try (Statement statement = connection.createStatement()) {
        statement.execute("use " + DATABASE_NAME);
        try (ResultSet resultSet =
            statement.executeQuery("SELECT * FROM table1 WHERE s_null > 1 order by device_id")) {
          fail("should throw exception to indicate that s_null doesn't exist");
        } catch (SQLException e) {
          assertEquals("701: Column 's_null' cannot be resolved", e.getMessage());
        }
      }
    } catch (SQLException e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  @Test
  public void selectWildcardWithFilterOrderByDeviceTest() {
    // 1. order by device + time filter
    String[] expectedHeader = new String[] {"time", "device_id", "s1", "s2", "s3"};

    String[] retArray =
        new String[] {
          "1970-01-01T00:00:00.001Z,d4,1111.1,true,1111,",
          "1970-01-01T00:00:00.001Z,d3,111.1,true,null,",
          "1970-01-01T00:00:00.004Z,d3,444.4,true,44,",
          "1970-01-01T00:00:00.001Z,d2,11.1,false,11,",
        };
    tableResultSetEqualTest(
        "SELECT * FROM table1 WHERE time < 5 ORDER BY device_id DESC, time LIMIT 4",
        expectedHeader,
        retArray,
        DATABASE_NAME);

    // 2. order by device + time filter + value filter
    retArray =
        new String[] {
          "1970-01-01T00:00:00.004Z,d3,444.4,true,44,",
          "1970-01-01T00:00:00.002Z,d2,22.2,false,22,",
        };
    tableResultSetEqualTest(
        "SELECT * FROM table1 where time > 1 and time < 5 and s3>=11 and s3<=1111 and s1 != 11.1 "
            + "ORDER BY device_id DESC",
        expectedHeader,
        retArray,
        DATABASE_NAME);
  }

  @Test
  public void selectMeasurementWithFilterOrderByTimeTest() {
    // 1. order by time + time filter
    String[] expectedHeader = new String[] {"time", "device_id", "s3", "s2"};
    String[] retArray =
        new String[] {
          "1970-01-01T00:00:00.004Z,d3,44,true,",
          "1970-01-01T00:00:00.002Z,d1,2,false,",
          "1970-01-01T00:00:00.002Z,d2,22,false,",
          "1970-01-01T00:00:00.001Z,d1,1,false,",
        };
    tableResultSetEqualTest(
        "SELECT time, device_id, s3, s2 FROM table1 WHERE time < 5 ORDER BY time DESC, device_id LIMIT 4",
        expectedHeader,
        retArray,
        DATABASE_NAME);

    // 2. order by time + time filter + value filter
    retArray =
        new String[] {
          "1970-01-01T00:00:00.004Z,d3,44,true,", "1970-01-01T00:00:00.002Z,d2,22,false,",
        };
    tableResultSetEqualTest(
        "SELECT time, device_id, s3,s2 FROM table1 where time > 1 and time < 5 and s3>=11 and s3<=1111 and s1 != 11.1 "
            + "ORDER BY time DESC, device_id",
        expectedHeader,
        retArray,
        DATABASE_NAME);
  }

  @Test
  public void selectMeasurementWithFilterOrderByDeviceTest() {
    // 1. order by device + time filter
    String[] expectedHeader = new String[] {"time", "device_id", "s3", "s2"};
    String[] retArray =
        new String[] {
          "1970-01-01T00:00:00.001Z,d4,1111,true,",
          "1970-01-01T00:00:00.001Z,d3,null,true,",
          "1970-01-01T00:00:00.004Z,d3,44,true,",
          "1970-01-01T00:00:00.001Z,d2,11,false,",
        };
    tableResultSetEqualTest(
        "SELECT time, device_id, s3,s2 FROM table1 WHERE time < 5 ORDER BY device_id DESC, time LIMIT 4",
        expectedHeader,
        retArray,
        DATABASE_NAME);

    // 2. order by device + time filter + value filter
    retArray =
        new String[] {
          "1970-01-01T00:00:00.004Z,d3,44,true,", "1970-01-01T00:00:00.002Z,d2,22,false,",
        };
    tableResultSetEqualTest(
        "SELECT time, device_id, s3,s2 FROM table1 where time > 1 and time < 5 and s3>=11 and s3<=1111 and s1 != 11.1 "
            + "ORDER BY device_id DESC, time asc",
        expectedHeader,
        retArray,
        DATABASE_NAME);
  }

  @Test
  public void aliasTest() {
    String[] expectedHeader = new String[] {"aa", "bb", "s3", "s2", "time", "device_id"};

    String[] retArray =
        new String[] {
          "1.1,false,1,false,1970-01-01T00:00:00.001Z,d1,",
          "2.2,false,2,false,1970-01-01T00:00:00.002Z,d1,",
          "11.1,false,11,false,1970-01-01T00:00:00.001Z,d2,",
          "22.2,false,22,false,1970-01-01T00:00:00.002Z,d2,",
          "111.1,true,null,true,1970-01-01T00:00:00.001Z,d3,",
          "444.4,true,44,true,1970-01-01T00:00:00.004Z,d3,",
          "1111.1,true,1111,true,1970-01-01T00:00:00.001Z,d4,",
          "5555.5,false,5555,false,1970-01-01T00:00:00.005Z,d4,",
        };
    tableResultSetEqualTest(
        "SELECT s1 as aa, s2 as bb, s3, s2, time, device_id FROM table1 order by device_id",
        expectedHeader,
        retArray,
        DATABASE_NAME);

    expectedHeader = new String[] {"time", "device_id", "a", "b"};

    retArray =
        new String[] {
          "1970-01-01T00:00:00.001Z,d1,1.1,1.1,",
          "1970-01-01T00:00:00.002Z,d1,2.2,2.2,",
          "1970-01-01T00:00:00.001Z,d2,11.1,11.1,",
          "1970-01-01T00:00:00.002Z,d2,22.2,22.2,",
          "1970-01-01T00:00:00.001Z,d3,111.1,111.1,",
          "1970-01-01T00:00:00.004Z,d3,444.4,444.4,",
          "1970-01-01T00:00:00.001Z,d4,1111.1,1111.1,",
          "1970-01-01T00:00:00.005Z,d4,5555.5,5555.5,",
        };
    tableResultSetEqualTest(
        "SELECT time, device_id, s1 as a, s1 as b  FROM table1 order by device_id",
        expectedHeader,
        retArray,
        DATABASE_NAME);
  }

  @Test
  public void orderByExpressionTest() {
    // 1. order by basic measurement
    String[] expectedHeader = new String[] {"time", "device_id", "s3", "s1", "s2"};
    String[] retArray =
        new String[] {
          "1970-01-01T00:00:00.005Z,d4,5555,5555.5,false,",
          "1970-01-01T00:00:00.002Z,d2,22,22.2,false,",
          "1970-01-01T00:00:00.001Z,d2,11,11.1,false,",
          "1970-01-01T00:00:00.002Z,d1,2,2.2,false,",
          "1970-01-01T00:00:00.001Z,d1,1,1.1,false,",
          "1970-01-01T00:00:00.001Z,d4,1111,1111.1,true,",
          "1970-01-01T00:00:00.004Z,d3,44,444.4,true,",
          "1970-01-01T00:00:00.001Z,d3,null,111.1,true,",
        };
    tableResultSetEqualTest(
        "SELECT time, device_id, s3, s1, s2 FROM table1 order by s2 asc, s1 desc, device_id",
        expectedHeader,
        retArray,
        DATABASE_NAME);

    // 2. select measurement is different with order by measurement
    expectedHeader = new String[] {"time", "device_id", "s3"};
    retArray =
        new String[] {
          "1970-01-01T00:00:00.005Z,d4,5555,",
          "1970-01-01T00:00:00.002Z,d2,22,",
          "1970-01-01T00:00:00.001Z,d2,11,",
          "1970-01-01T00:00:00.002Z,d1,2,",
          "1970-01-01T00:00:00.001Z,d1,1,",
          "1970-01-01T00:00:00.001Z,d4,1111,",
          "1970-01-01T00:00:00.004Z,d3,44,",
          "1970-01-01T00:00:00.001Z,d3,null,",
        };
    tableResultSetEqualTest(
        "SELECT time, device_id, s3 FROM table1 order by s2 asc, s1 desc",
        expectedHeader,
        retArray,
        DATABASE_NAME);

    // 3. order by expression
    retArray =
        new String[] {
          "1970-01-01T00:00:00.005Z,d4,5555,",
          "1970-01-01T00:00:00.001Z,d4,1111,",
          "1970-01-01T00:00:00.004Z,d3,44,",
          "1970-01-01T00:00:00.002Z,d2,22,",
          "1970-01-01T00:00:00.001Z,d2,11,",
          "1970-01-01T00:00:00.002Z,d1,2,",
          "1970-01-01T00:00:00.001Z,d1,1,",
          "1970-01-01T00:00:00.001Z,d3,null,",
        };
    tableResultSetEqualTest(
        "SELECT time, device_id, s3 FROM table1 order by s1+s3 desc",
        expectedHeader,
        retArray,
        DATABASE_NAME);
  }

  @Test
  public void templateInvalidTest() {

    tableAssertTestFail(
        "select s1 from table1 where s1",
        "701: WHERE clause must evaluate to a boolean: actual type FLOAT",
        DATABASE_NAME);
  }

  @Test
  public void emptyResultTest() {
    String[] expectedHeader = new String[] {"time", "device_id", "s1", "s2", "s3"};

    String[] retArray = new String[] {};
    tableResultSetEqualTest(
        "SELECT * FROM table1 where time>=now()-1d and time<=now() " + "ORDER BY time DESC",
        expectedHeader,
        retArray,
        DATABASE_NAME);
  }

  protected static void insertData() {
    try (Connection connection = EnvFactory.getEnv().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement statement = connection.createStatement()) {

      for (String sql : sqls) {
        statement.execute(sql);
      }
    } catch (Exception e) {
      e.printStackTrace();
    }
  }
}
