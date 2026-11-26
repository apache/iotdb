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
package org.apache.iotdb.db.it.alignbydevice;

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
import java.sql.Statement;

import static org.apache.iotdb.db.it.utils.TestUtils.assertTestFail;
import static org.apache.iotdb.db.it.utils.TestUtils.resultSetEqualTest;

@RunWith(IoTDBTestRunner.class)
@Category({LocalStandaloneIT.class, ClusterIT.class})
public class IoTDBAlignByDeviceWithTemplateIT {
  private static final String[] sqls =
      new String[] {
        // non-aligned template
        "CREATE database root.sg1;",
        "CREATE schema template t1 (s1 FLOAT encoding=RLE, s2 BOOLEAN encoding=PLAIN compression=SNAPPY, s3 INT32);",
        "SET SCHEMA TEMPLATE t1 to root.sg1;",
        "INSERT INTO root.sg1.d1(timestamp,s1,s2,s3) values(1,1.1,false,1), (2,2.2,false,2);",
        "INSERT INTO root.sg1.d2(timestamp,s1,s2,s3) values(1,11.1,false,11), (2,22.2,false,22);",
        "INSERT INTO root.sg1.d3(timestamp,s1,s2,s3) values(1,111.1,true,null), (4,444.4,true,44);",
        "INSERT INTO root.sg1.d4(timestamp,s1,s2,s3) values(1,1111.1,true,1111), (5,5555.5,false,5555);",

        // aligned template
        "CREATE database root.sg2;",
        "CREATE schema template t2 aligned (s1 FLOAT encoding=RLE, s2 BOOLEAN encoding=PLAIN compression=SNAPPY, s3 INT32);",
        "SET SCHEMA TEMPLATE t2 to root.sg2;",
        "INSERT INTO root.sg2.d1(timestamp,s1,s2,s3) values(1,1.1,false,1), (2,2.2,false,2);",
        "INSERT INTO root.sg2.d2(timestamp,s1,s2,s3) values(1,11.1,false,11), (2,22.2,false,22);",
        "INSERT INTO root.sg2.d3(timestamp,s1,s2,s3) values(1,111.1,true,null), (4,444.4,true,44);",
        "INSERT INTO root.sg2.d4(timestamp,s1,s2,s3) values(1,1111.1,true,1111), (5,5555.5,false,5555);",
      };

  String[] expectedHeader;
  String[] retArray;

  @BeforeClass
  public static void setUp() {
    EnvFactory.getEnv().initClusterEnvironment();
    insertData();
  }

  @AfterClass
  public static void tearDown() {
    EnvFactory.getEnv().cleanClusterEnvironment();
  }

  @Test
  public void singleDeviceTest() {
    expectedHeader = new String[] {"Time,Device,s3,s1,s2"};
    retArray =
        new String[] {
          "1,root.sg1.d1,1,1.1,false,",
        };
    resultSetEqualTest(
        "SELECT * FROM root.sg1.d1 order by time desc offset 1 limit 1 ALIGN BY DEVICE;",
        expectedHeader,
        retArray);
    retArray =
        new String[] {
          "1,root.sg2.d1,1,1.1,false,",
        };
    resultSetEqualTest(
        "SELECT * FROM root.sg2.d1 order by time desc offset 1 limit 1 ALIGN BY DEVICE;",
        expectedHeader,
        retArray);
  }

  @Test
  public void selectWildcardNoFilterTest() {
    // 1. order by device
    expectedHeader = new String[] {"Time,Device,s3,s1,s2"};
    retArray =
        new String[] {
          "1,root.sg1.d1,1,1.1,false,",
          "2,root.sg1.d1,2,2.2,false,",
          "1,root.sg1.d2,11,11.1,false,",
          "2,root.sg1.d2,22,22.2,false,",
          "1,root.sg1.d3,null,111.1,true,",
          "4,root.sg1.d3,44,444.4,true,",
          "1,root.sg1.d4,1111,1111.1,true,",
          "5,root.sg1.d4,5555,5555.5,false,",
        };
    resultSetEqualTest("SELECT * FROM root.sg1.** ALIGN BY DEVICE;", expectedHeader, retArray);
    retArray =
        new String[] {
          "1,root.sg2.d1,1,1.1,false,",
          "2,root.sg2.d1,2,2.2,false,",
          "1,root.sg2.d2,11,11.1,false,",
          "2,root.sg2.d2,22,22.2,false,",
          "1,root.sg2.d3,null,111.1,true,",
          "4,root.sg2.d3,44,444.4,true,",
          "1,root.sg2.d4,1111,1111.1,true,",
          "5,root.sg2.d4,5555,5555.5,false,",
        };
    resultSetEqualTest("SELECT * FROM root.sg2.** ALIGN BY DEVICE;", expectedHeader, retArray);

    expectedHeader = new String[] {"Time,Device,s3,s1,s2,s1"};
    retArray =
        new String[] {
          "1,root.sg1.d1,1,1.1,false,1.1,",
          "2,root.sg1.d1,2,2.2,false,2.2,",
          "1,root.sg1.d2,11,11.1,false,11.1,",
          "2,root.sg1.d2,22,22.2,false,22.2,",
          "1,root.sg1.d3,null,111.1,true,111.1,",
          "4,root.sg1.d3,44,444.4,true,444.4,",
          "1,root.sg1.d4,1111,1111.1,true,1111.1,",
          "5,root.sg1.d4,5555,5555.5,false,5555.5,",
        };
    resultSetEqualTest("SELECT *, s1 FROM root.sg1.** ALIGN BY DEVICE;", expectedHeader, retArray);
    retArray =
        new String[] {
          "1,root.sg2.d1,1,1.1,false,1.1,",
          "2,root.sg2.d1,2,2.2,false,2.2,",
          "1,root.sg2.d2,11,11.1,false,11.1,",
          "2,root.sg2.d2,22,22.2,false,22.2,",
          "1,root.sg2.d3,null,111.1,true,111.1,",
          "4,root.sg2.d3,44,444.4,true,444.4,",
          "1,root.sg2.d4,1111,1111.1,true,1111.1,",
          "5,root.sg2.d4,5555,5555.5,false,5555.5,",
        };
    resultSetEqualTest("SELECT *, s1 FROM root.sg2.** ALIGN BY DEVICE;", expectedHeader, retArray);

    expectedHeader = new String[] {"Time,Device,s3,s1,s2"};
    retArray =
        new String[] {
          "1,root.sg1.d1,1,1.1,false,",
          "2,root.sg1.d1,2,2.2,false,",
          "1,root.sg1.d2,11,11.1,false,",
          "2,root.sg1.d2,22,22.2,false,",
        };
    resultSetEqualTest(
        "SELECT * FROM root.sg1.d1,root.sg1.d2,root.sg1.d6 ALIGN BY DEVICE;",
        expectedHeader,
        retArray);
    retArray =
        new String[] {
          "1,root.sg2.d1,1,1.1,false,",
          "2,root.sg2.d1,2,2.2,false,",
          "1,root.sg2.d2,11,11.1,false,",
          "2,root.sg2.d2,22,22.2,false,",
        };
    resultSetEqualTest(
        "SELECT * FROM root.sg2.d1,root.sg2.d2,root.sg2.d6 ALIGN BY DEVICE;",
        expectedHeader,
        retArray);

    // 2. order by device + limit/offset
    expectedHeader = new String[] {"Time,Device,s3,s1,s2"};
    retArray =
        new String[] {
          "2,root.sg1.d1,2,2.2,false,", "1,root.sg1.d2,11,11.1,false,",
        };
    resultSetEqualTest(
        "SELECT * FROM root.sg1.** OFFSET 1 LIMIT 2 ALIGN BY DEVICE;", expectedHeader, retArray);

    retArray =
        new String[] {
          "2,root.sg2.d1,2,2.2,false,", "1,root.sg2.d2,11,11.1,false,",
        };
    resultSetEqualTest(
        "SELECT * FROM root.sg2.** OFFSET 1 LIMIT 2 ALIGN BY DEVICE;", expectedHeader, retArray);

    // 3. order by time
    retArray =
        new String[] {
          "5,root.sg1.d4,5555,5555.5,false,",
          "4,root.sg1.d3,44,444.4,true,",
          "2,root.sg1.d1,2,2.2,false,",
          "2,root.sg1.d2,22,22.2,false,",
          "1,root.sg1.d1,1,1.1,false,",
          "1,root.sg1.d2,11,11.1,false,",
          "1,root.sg1.d3,null,111.1,true,",
          "1,root.sg1.d4,1111,1111.1,true,",
        };
    resultSetEqualTest(
        "SELECT * FROM root.sg1.** ORDER BY TIME DESC ALIGN BY DEVICE;", expectedHeader, retArray);

    retArray =
        new String[] {
          "5,root.sg2.d4,5555,5555.5,false,",
          "4,root.sg2.d3,44,444.4,true,",
          "2,root.sg2.d1,2,2.2,false,",
          "2,root.sg2.d2,22,22.2,false,",
          "1,root.sg2.d1,1,1.1,false,",
          "1,root.sg2.d2,11,11.1,false,",
          "1,root.sg2.d3,null,111.1,true,",
          "1,root.sg2.d4,1111,1111.1,true,",
        };
    resultSetEqualTest(
        "SELECT * FROM root.sg2.** ORDER BY TIME DESC ALIGN BY DEVICE;", expectedHeader, retArray);

    // 4. order by time + limit/offset
    retArray =
        new String[] {
          "5,root.sg1.d4,5555,5555.5,false,", "4,root.sg1.d3,44,444.4,true,",
          "2,root.sg1.d1,2,2.2,false,", "2,root.sg1.d2,22,22.2,false,",
        };
    resultSetEqualTest(
        "SELECT * FROM root.sg1.** ORDER BY TIME DESC LIMIT 4 ALIGN BY DEVICE;",
        expectedHeader,
        retArray);

    retArray =
        new String[] {
          "5,root.sg2.d4,5555,5555.5,false,", "4,root.sg2.d3,44,444.4,true,",
          "2,root.sg2.d1,2,2.2,false,", "2,root.sg2.d2,22,22.2,false,",
        };
    resultSetEqualTest(
        "SELECT * FROM root.sg2.** ORDER BY TIME DESC LIMIT 4 ALIGN BY DEVICE;",
        expectedHeader,
        retArray);
  }

  @Test
  public void selectMeasurementNoFilterTest() {
    // 1. order by device
    String[] expectedHeader = new String[] {"Time,Device,s3,s1"};
    String[] retArray =
        new String[] {
          "1,root.sg1.d1,1,1.1,",
          "2,root.sg1.d1,2,2.2,",
          "1,root.sg1.d2,11,11.1,",
          "2,root.sg1.d2,22,22.2,",
          "1,root.sg1.d3,null,111.1,",
          "4,root.sg1.d3,44,444.4,",
          "1,root.sg1.d4,1111,1111.1,",
          "5,root.sg1.d4,5555,5555.5,",
        };
    resultSetEqualTest("SELECT s3,s1 FROM root.sg1.** ALIGN BY DEVICE;", expectedHeader, retArray);
    resultSetEqualTest(
        "SELECT s3,s1,s_null FROM root.sg1.** ALIGN BY DEVICE;", expectedHeader, retArray);

    retArray =
        new String[] {
          "1,root.sg2.d1,1,1.1,",
          "2,root.sg2.d1,2,2.2,",
          "1,root.sg2.d2,11,11.1,",
          "2,root.sg2.d2,22,22.2,",
          "1,root.sg2.d3,null,111.1,",
          "4,root.sg2.d3,44,444.4,",
          "1,root.sg2.d4,1111,1111.1,",
          "5,root.sg2.d4,5555,5555.5,",
        };
    resultSetEqualTest("SELECT s3,s1 FROM root.sg2.** ALIGN BY DEVICE;", expectedHeader, retArray);
    resultSetEqualTest(
        "SELECT s3,s1,s_null FROM root.sg2.** ALIGN BY DEVICE;", expectedHeader, retArray);

    // 2. order by device + limit/offset
    retArray =
        new String[] {
          "2,root.sg1.d1,2,2.2,", "1,root.sg1.d2,11,11.1,",
        };
    resultSetEqualTest(
        "SELECT s3,s1 FROM root.sg1.** OFFSET 1 LIMIT 2 ALIGN BY DEVICE;",
        expectedHeader,
        retArray);

    retArray =
        new String[] {
          "2,root.sg2.d1,2,2.2,", "1,root.sg2.d2,11,11.1,",
        };
    resultSetEqualTest(
        "SELECT s3,s1 FROM root.sg2.** OFFSET 1 LIMIT 2 ALIGN BY DEVICE;",
        expectedHeader,
        retArray);

    // 3. order by time
    retArray =
        new String[] {
          "5,root.sg1.d4,5555,5555.5,",
          "4,root.sg1.d3,44,444.4,",
          "2,root.sg1.d1,2,2.2,",
          "2,root.sg1.d2,22,22.2,",
          "1,root.sg1.d1,1,1.1,",
          "1,root.sg1.d2,11,11.1,",
          "1,root.sg1.d3,null,111.1,",
          "1,root.sg1.d4,1111,1111.1,",
        };
    resultSetEqualTest(
        "SELECT s3,s1 FROM root.sg1.** ORDER BY TIME DESC ALIGN BY DEVICE;",
        expectedHeader,
        retArray);

    retArray =
        new String[] {
          "5,root.sg2.d4,5555,5555.5,",
          "4,root.sg2.d3,44,444.4,",
          "2,root.sg2.d1,2,2.2,",
          "2,root.sg2.d2,22,22.2,",
          "1,root.sg2.d1,1,1.1,",
          "1,root.sg2.d2,11,11.1,",
          "1,root.sg2.d3,null,111.1,",
          "1,root.sg2.d4,1111,1111.1,",
        };
    resultSetEqualTest(
        "SELECT s3,s1 FROM root.sg2.** ORDER BY TIME DESC ALIGN BY DEVICE;",
        expectedHeader,
        retArray);

    // 4. order by time + limit/offset
    retArray =
        new String[] {
          "5,root.sg1.d4,5555,5555.5,", "4,root.sg1.d3,44,444.4,",
          "2,root.sg1.d1,2,2.2,", "2,root.sg1.d2,22,22.2,",
        };
    resultSetEqualTest(
        "SELECT s3,s1 FROM root.sg1.** ORDER BY TIME DESC LIMIT 4 ALIGN BY DEVICE;",
        expectedHeader,
        retArray);

    retArray =
        new String[] {
          "5,root.sg2.d4,5555,5555.5,", "4,root.sg2.d3,44,444.4,",
          "2,root.sg2.d1,2,2.2,", "2,root.sg2.d2,22,22.2,",
        };
    resultSetEqualTest(
        "SELECT s3,s1 FROM root.sg2.** ORDER BY TIME DESC LIMIT 4 ALIGN BY DEVICE;",
        expectedHeader,
        retArray);
  }

  @Test
  public void selectWildcardWithFilterOrderByTimeTest() {
    // 1. order by time + time filter
    String[] expectedHeader = new String[] {"Time,Device,s3,s1,s2"};
    String[] retArray =
        new String[] {
          "4,root.sg1.d3,44,444.4,true,",
          "2,root.sg1.d1,2,2.2,false,",
          "2,root.sg1.d2,22,22.2,false,",
          "1,root.sg1.d1,1,1.1,false,",
        };
    resultSetEqualTest(
        "SELECT * FROM root.sg1.** WHERE time < 5 ORDER BY TIME DESC LIMIT 4 ALIGN BY DEVICE;",
        expectedHeader,
        retArray);

    retArray =
        new String[] {
          "4,root.sg2.d3,44,444.4,true,",
          "2,root.sg2.d1,2,2.2,false,",
          "2,root.sg2.d2,22,22.2,false,",
          "1,root.sg2.d1,1,1.1,false,",
        };
    resultSetEqualTest(
        "SELECT * FROM root.sg2.** WHERE time < 5 ORDER BY TIME DESC LIMIT 4 ALIGN BY DEVICE;",
        expectedHeader,
        retArray);

    // 2. order by time + time filter + value filter
    retArray =
        new String[] {
          "4,root.sg1.d3,44,444.4,true,", "2,root.sg1.d2,22,22.2,false,",
        };
    resultSetEqualTest(
        "SELECT * FROM root.sg1.** where time > 1 and time < 5 and s3>=11 and s3<=1111 and s1 != 11.1 "
            + "ORDER BY TIME DESC ALIGN BY DEVICE;",
        expectedHeader,
        retArray);

    retArray =
        new String[] {
          "4,root.sg2.d3,44,444.4,true,", "2,root.sg2.d2,22,22.2,false,",
        };
    resultSetEqualTest(
        "SELECT * FROM root.sg2.** where time > 1 and time < 5 and s3>=11 and s3<=1111 and s1 != 11.1 "
            + "ORDER BY TIME DESC ALIGN BY DEVICE;",
        expectedHeader,
        retArray);

    // 3. order by time + value filter: s_null > 1
    retArray = new String[] {};
    resultSetEqualTest(
        "SELECT * FROM root.sg1.** WHERE s_null > 1 ORDER BY TIME ALIGN BY DEVICE;",
        expectedHeader,
        retArray);
    resultSetEqualTest(
        "SELECT * FROM root.sg2.** WHERE s_null > 1 ORDER BY TIME ALIGN BY DEVICE;",
        expectedHeader,
        retArray);

    retArray =
        new String[] {
          "4,root.sg1.d3,44,444.4,true,", "2,root.sg1.d2,22,22.2,false,",
        };
    resultSetEqualTest(
        "SELECT * FROM root.sg1.** WHERE s_null > 1 or "
            + "(time > 1 and time < 5 and s3>=11 and s3<=1111 and s1 != 11.1) ORDER BY TIME DESC ALIGN BY DEVICE;",
        expectedHeader,
        retArray);
    retArray =
        new String[] {
          "4,root.sg2.d3,44,444.4,true,", "2,root.sg2.d2,22,22.2,false,",
        };
    resultSetEqualTest(
        "SELECT * FROM root.sg2.** WHERE s_null > 1 or "
            + "(time > 1 and time < 5 and s3>=11 and s3<=1111 and s1 != 11.1) ORDER BY TIME DESC ALIGN BY DEVICE;",
        expectedHeader,
        retArray);
  }

  @Test
  public void selectWildcardWithFilterOrderByDeviceTest() {
    // 1. order by device + time filter
    String[] expectedHeader = new String[] {"Time,Device,s3,s1,s2"};
    String[] retArray =
        new String[] {
          "1,root.sg1.d4,1111,1111.1,true,",
          "1,root.sg1.d3,null,111.1,true,",
          "4,root.sg1.d3,44,444.4,true,",
          "1,root.sg1.d2,11,11.1,false,",
        };
    resultSetEqualTest(
        "SELECT * FROM root.sg1.** WHERE time < 5 ORDER BY DEVICE DESC LIMIT 4 ALIGN BY DEVICE;",
        expectedHeader,
        retArray);

    retArray =
        new String[] {
          "1,root.sg2.d4,1111,1111.1,true,",
          "1,root.sg2.d3,null,111.1,true,",
          "4,root.sg2.d3,44,444.4,true,",
          "1,root.sg2.d2,11,11.1,false,",
        };
    resultSetEqualTest(
        "SELECT * FROM root.sg2.** WHERE time < 5 ORDER BY DEVICE DESC LIMIT 4 ALIGN BY DEVICE;",
        expectedHeader,
        retArray);

    // 2. order by device + time filter + value filter
    retArray =
        new String[] {
          "4,root.sg1.d3,44,444.4,true,", "2,root.sg1.d2,22,22.2,false,",
        };
    resultSetEqualTest(
        "SELECT * FROM root.sg1.** where time > 1 and time < 5 and s3>=11 and s3<=1111 and s1 != 11.1 "
            + "ORDER BY DEVICE DESC ALIGN BY DEVICE;",
        expectedHeader,
        retArray);

    retArray =
        new String[] {
          "2,root.sg2.d2,22,22.2,false,", "4,root.sg2.d3,44,444.4,true,",
        };
    resultSetEqualTest(
        "SELECT * FROM root.sg2.** where time > 1 and time < 5 and s3>=11 and s3<=1111 and s1 != 11.1 "
            + "ORDER BY DEVICE ALIGN BY DEVICE;",
        expectedHeader,
        retArray);

    // 3. order by device + value filter: s_null > 1
    retArray = new String[] {};
    resultSetEqualTest(
        "SELECT * FROM root.sg1.** WHERE s_null > 1 ORDER BY DEVICE ALIGN BY DEVICE;",
        expectedHeader,
        retArray);
    resultSetEqualTest(
        "SELECT * FROM root.sg2.** WHERE s_null > 1 ORDER BY DEVICE ALIGN BY DEVICE;",
        expectedHeader,
        retArray);

    retArray =
        new String[] {
          "4,root.sg1.d3,44,444.4,true,", "2,root.sg1.d2,22,22.2,false,",
        };
    resultSetEqualTest(
        "SELECT * FROM root.sg1.** WHERE s_null > 1 or "
            + "(time > 1 and time < 5 and s3>=11 and s3<=1111 and s1 != 11.1) ORDER BY DEVICE DESC ALIGN BY DEVICE;",
        expectedHeader,
        retArray);
    retArray =
        new String[] {
          "4,root.sg2.d3,44,444.4,true,", "2,root.sg2.d2,22,22.2,false,",
        };
    resultSetEqualTest(
        "SELECT * FROM root.sg2.** WHERE s_null > 1 or "
            + "(time > 1 and time < 5 and s3>=11 and s3<=1111 and s1 != 11.1) ORDER BY DEVICE DESC ALIGN BY DEVICE;",
        expectedHeader,
        retArray);
  }

  @Test
  public void selectMeasurementWithFilterOrderByTimeTest() {
    // 1. order by time + time filter
    String[] expectedHeader = new String[] {"Time,Device,s3,s2"};
    String[] retArray =
        new String[] {
          "4,root.sg1.d3,44,true,",
          "2,root.sg1.d1,2,false,",
          "2,root.sg1.d2,22,false,",
          "1,root.sg1.d1,1,false,",
        };
    resultSetEqualTest(
        "SELECT s3,s2 FROM root.sg1.** WHERE time < 5 ORDER BY TIME DESC LIMIT 4 ALIGN BY DEVICE;",
        expectedHeader,
        retArray);

    retArray =
        new String[] {
          "4,root.sg2.d3,44,true,",
          "2,root.sg2.d1,2,false,",
          "2,root.sg2.d2,22,false,",
          "1,root.sg2.d1,1,false,",
        };
    resultSetEqualTest(
        "SELECT s3,s2 FROM root.sg2.** WHERE time < 5 ORDER BY TIME DESC LIMIT 4 ALIGN BY DEVICE;",
        expectedHeader,
        retArray);

    // 2. order by time + time filter + value filter
    retArray =
        new String[] {
          "4,root.sg1.d3,44,true,", "2,root.sg1.d2,22,false,",
        };
    resultSetEqualTest(
        "SELECT s3,s2 FROM root.sg1.** where time > 1 and time < 5 and s3>=11 and s3<=1111 and s1 != 11.1 "
            + "ORDER BY TIME DESC ALIGN BY DEVICE;",
        expectedHeader,
        retArray);

    retArray =
        new String[] {
          "4,root.sg2.d3,44,true,", "2,root.sg2.d2,22,false,",
        };
    resultSetEqualTest(
        "SELECT s3,s2 FROM root.sg2.** where time > 1 and time < 5 and s3>=11 and s3<=1111 and s1 != 11.1 "
            + "ORDER BY TIME DESC ALIGN BY DEVICE;",
        expectedHeader,
        retArray);

    // 3. order by time + value filter: s_null > 1
    retArray = new String[] {};
    resultSetEqualTest(
        "SELECT s3,s2 FROM root.sg1.** WHERE s_null > 1 ORDER BY TIME ALIGN BY DEVICE;",
        expectedHeader,
        retArray);
    resultSetEqualTest(
        "SELECT s3,s2 FROM root.sg2.** WHERE s_null > 1 ORDER BY TIME ALIGN BY DEVICE;",
        expectedHeader,
        retArray);

    retArray =
        new String[] {
          "2,root.sg1.d2,22,false,",
        };
    resultSetEqualTest(
        "SELECT s3,s2 FROM root.sg1.** where s_null > 1 or (time > 1 and time < 5 and s3>=11 and s3<=1111 and s1 != 11.1) "
            + "ORDER BY TIME DESC OFFSET 1 LIMIT 1 ALIGN BY DEVICE;",
        expectedHeader,
        retArray);

    retArray =
        new String[] {
          "2,root.sg2.d2,22,false,",
        };
    resultSetEqualTest(
        "SELECT s3,s2 FROM root.sg2.** where s_null > 1 or (time > 1 and time < 5 and s3>=11 and s3<=1111 and s1 != 11.1) "
            + "ORDER BY TIME DESC OFFSET 1 LIMIT 1 ALIGN BY DEVICE;",
        expectedHeader,
        retArray);
  }

  @Test
  public void selectMeasurementWithFilterOrderByDeviceTest() {
    // 1. order by device + time filter
    String[] expectedHeader = new String[] {"Time,Device,s3,s2"};
    String[] retArray =
        new String[] {
          "1,root.sg1.d4,1111,true,",
          "1,root.sg1.d3,null,true,",
          "4,root.sg1.d3,44,true,",
          "1,root.sg1.d2,11,false,",
        };
    resultSetEqualTest(
        "SELECT s3,s2 FROM root.sg1.** WHERE time < 5 ORDER BY DEVICE DESC LIMIT 4 ALIGN BY DEVICE;",
        expectedHeader,
        retArray);

    retArray =
        new String[] {
          "1,root.sg2.d4,1111,true,",
          "1,root.sg2.d3,null,true,",
          "4,root.sg2.d3,44,true,",
          "1,root.sg2.d2,11,false,",
        };
    resultSetEqualTest(
        "SELECT s3,s2 FROM root.sg2.** WHERE time < 5 ORDER BY DEVICE DESC LIMIT 4 ALIGN BY DEVICE;",
        expectedHeader,
        retArray);

    // 2. order by device + time filter + value filter
    retArray =
        new String[] {
          "4,root.sg1.d3,44,true,", "2,root.sg1.d2,22,false,",
        };
    resultSetEqualTest(
        "SELECT s3,s2 FROM root.sg1.** where time > 1 and time < 5 and s3>=11 and s3<=1111 and s1 != 11.1 "
            + "ORDER BY DEVICE DESC ALIGN BY DEVICE;",
        expectedHeader,
        retArray);

    retArray =
        new String[] {
          "4,root.sg2.d3,44,true,", "2,root.sg2.d2,22,false,",
        };
    resultSetEqualTest(
        "SELECT s3,s2 FROM root.sg2.** where time > 1 and time < 5 and s3>=11 and s3<=1111 and s1 != 11.1 "
            + "ORDER BY DEVICE DESC ALIGN BY DEVICE;",
        expectedHeader,
        retArray);

    // 3. order by device + value filter: s_null > 1
    retArray = new String[] {};
    resultSetEqualTest(
        "SELECT s3,s2 FROM root.sg1.** WHERE s_null > 1 ORDER BY DEVICE ALIGN BY DEVICE;",
        expectedHeader,
        retArray);
    resultSetEqualTest(
        "SELECT s3,s2 FROM root.sg2.** WHERE s_null > 1 ORDER BY DEVICE ALIGN BY DEVICE;",
        expectedHeader,
        retArray);

    retArray =
        new String[] {
          "2,root.sg1.d2,22,false,",
        };
    resultSetEqualTest(
        "SELECT s3,s2 FROM root.sg1.** where s_null > 1 or (time > 1 and time < 5 and s3>=11 and s3<=1111 and s1 != 11.1) "
            + "ORDER BY DEVICE DESC OFFSET 1 LIMIT 1 ALIGN BY DEVICE;",
        expectedHeader,
        retArray);

    retArray =
        new String[] {
          "2,root.sg2.d2,22,false,",
        };
    resultSetEqualTest(
        "SELECT s3,s2 FROM root.sg2.** where s_null > 1 or (time > 1 and time < 5 and s3>=11 and s3<=1111 and s1 != 11.1) "
            + "ORDER BY DEVICE DESC OFFSET 1 LIMIT 1 ALIGN BY DEVICE;",
        expectedHeader,
        retArray);
  }

  @Test
  public void aliasTest() {
    String[] expectedHeader = new String[] {"Time,Device,aa,bb,s3,s2"};
    String[] retArray =
        new String[] {
          "1,root.sg1.d1,1.1,false,1,false,",
          "2,root.sg1.d1,2.2,false,2,false,",
          "1,root.sg1.d2,11.1,false,11,false,",
          "2,root.sg1.d2,22.2,false,22,false,",
          "1,root.sg1.d3,111.1,true,null,true,",
          "4,root.sg1.d3,444.4,true,44,true,",
          "1,root.sg1.d4,1111.1,true,1111,true,",
          "5,root.sg1.d4,5555.5,false,5555,false,",
        };
    resultSetEqualTest(
        "SELECT s1 as aa, s2 as bb, s3, s2 FROM root.sg1.** ALIGN BY DEVICE;",
        expectedHeader,
        retArray);

    expectedHeader = new String[] {"Time,Device,aa,bb,s3,s2"};
    retArray =
        new String[] {
          "1,root.sg2.d1,1.1,false,1,false,",
          "2,root.sg2.d1,2.2,false,2,false,",
          "1,root.sg2.d2,11.1,false,11,false,",
          "2,root.sg2.d2,22.2,false,22,false,",
          "1,root.sg2.d3,111.1,true,null,true,",
          "4,root.sg2.d3,444.4,true,44,true,",
          "1,root.sg2.d4,1111.1,true,1111,true,",
          "5,root.sg2.d4,5555.5,false,5555,false,",
        };
    resultSetEqualTest(
        "SELECT s1 as aa, s2 as bb, s3, s2 FROM root.sg2.** ALIGN BY DEVICE;",
        expectedHeader,
        retArray);

    expectedHeader = new String[] {"Time,Device,a,b"};
    retArray =
        new String[] {
          "1,root.sg1.d1,1.1,1.1,",
          "2,root.sg1.d1,2.2,2.2,",
          "1,root.sg1.d2,11.1,11.1,",
          "2,root.sg1.d2,22.2,22.2,",
          "1,root.sg1.d3,111.1,111.1,",
          "4,root.sg1.d3,444.4,444.4,",
          "1,root.sg1.d4,1111.1,1111.1,",
          "5,root.sg1.d4,5555.5,5555.5,",
        };
    resultSetEqualTest(
        "SELECT s1 as a, s1 as b  FROM root.sg1.** ALIGN BY DEVICE;", expectedHeader, retArray);

    expectedHeader = new String[] {"Time,Device,a,b"};
    retArray =
        new String[] {
          "1,root.sg2.d1,1.1,1.1,",
          "2,root.sg2.d1,2.2,2.2,",
          "1,root.sg2.d2,11.1,11.1,",
          "2,root.sg2.d2,22.2,22.2,",
          "1,root.sg2.d3,111.1,111.1,",
          "4,root.sg2.d3,444.4,444.4,",
          "1,root.sg2.d4,1111.1,1111.1,",
          "5,root.sg2.d4,5555.5,5555.5,",
        };
    resultSetEqualTest(
        "SELECT s1 as a, s1 as b  FROM root.sg2.** ALIGN BY DEVICE;", expectedHeader, retArray);
  }

  @Test
  public void orderByExpressionTest() {
    // order by expression is not supported temporarily
    // 1. order by basic measurement
    String[] expectedHeader = new String[] {"Time,Device,s3,s1,s2"};
    String[] retArray =
        new String[] {
          "5,root.sg1.d4,5555,5555.5,false,",
          "2,root.sg1.d2,22,22.2,false,",
          "1,root.sg1.d2,11,11.1,false,",
          "2,root.sg1.d1,2,2.2,false,",
          "1,root.sg1.d1,1,1.1,false,",
          "1,root.sg1.d4,1111,1111.1,true,",
          "4,root.sg1.d3,44,444.4,true,",
          "1,root.sg1.d3,null,111.1,true,",
        };
    resultSetEqualTest(
        "SELECT * FROM root.sg1.** order by s2 asc, s1 desc ALIGN BY DEVICE;",
        expectedHeader,
        retArray);

    retArray =
        new String[] {
          "5,root.sg2.d4,5555,5555.5,false,",
          "2,root.sg2.d2,22,22.2,false,",
          "1,root.sg2.d2,11,11.1,false,",
          "2,root.sg2.d1,2,2.2,false,",
          "1,root.sg2.d1,1,1.1,false,",
          "1,root.sg2.d4,1111,1111.1,true,",
          "4,root.sg2.d3,44,444.4,true,",
          "1,root.sg2.d3,null,111.1,true,",
        };
    resultSetEqualTest(
        "SELECT * FROM root.sg2.** order by s2 asc, s1 desc ALIGN BY DEVICE;",
        expectedHeader,
        retArray);

    // 2. select measurement is different with order by measurement
    expectedHeader = new String[] {"Time,Device,s3"};
    retArray =
        new String[] {
          "5,root.sg1.d4,5555,",
          "2,root.sg1.d2,22,",
          "1,root.sg1.d2,11,",
          "2,root.sg1.d1,2,",
          "1,root.sg1.d1,1,",
          "1,root.sg1.d4,1111,",
          "4,root.sg1.d3,44,",
          "1,root.sg1.d3,null,",
        };
    resultSetEqualTest(
        "SELECT s3 FROM root.sg1.** order by s2 asc, s1 desc ALIGN BY DEVICE;",
        expectedHeader,
        retArray);

    retArray =
        new String[] {
          "5,root.sg2.d4,5555,",
          "2,root.sg2.d2,22,",
          "1,root.sg2.d2,11,",
          "2,root.sg2.d1,2,",
          "1,root.sg2.d1,1,",
          "1,root.sg2.d4,1111,",
          "4,root.sg2.d3,44,",
          "1,root.sg2.d3,null,",
        };
    resultSetEqualTest(
        "SELECT s3 FROM root.sg2.** order by s2 asc, s1 desc ALIGN BY DEVICE;",
        expectedHeader,
        retArray);

    // 3. order by expression
    retArray =
        new String[] {
          "5,root.sg1.d4,5555,",
          "1,root.sg1.d4,1111,",
          "4,root.sg1.d3,44,",
          "2,root.sg1.d2,22,",
          "1,root.sg1.d2,11,",
          "2,root.sg1.d1,2,",
          "1,root.sg1.d1,1,",
          "1,root.sg1.d3,null,",
        };
    resultSetEqualTest(
        "SELECT s3 FROM root.sg1.** order by s1+s3 desc ALIGN BY DEVICE;",
        expectedHeader,
        retArray);
    retArray =
        new String[] {
          "5,root.sg2.d4,5555,",
          "1,root.sg2.d4,1111,",
          "4,root.sg2.d3,44,",
          "2,root.sg2.d2,22,",
          "1,root.sg2.d2,11,",
          "2,root.sg2.d1,2,",
          "1,root.sg2.d1,1,",
          "1,root.sg2.d3,null,",
        };
    resultSetEqualTest(
        "SELECT s3 FROM root.sg2.** order by s1+s3 desc ALIGN BY DEVICE;",
        expectedHeader,
        retArray);
  }

  @Test
  public void templateInvalidTest() {
    // 1. non align by device query
    String[] expectedHeader = new String[] {"Time,root.sg1.d4.s3,root.sg1.d4.s1,root.sg1.d4.s2"};
    String[] retArray =
        new String[] {
          "1,1111,1111.1,true,", "5,5555,5555.5,false,",
        };
    resultSetEqualTest("SELECT * FROM root.sg1.** slimit 3;", expectedHeader, retArray);

    expectedHeader = new String[] {"Time,root.sg2.d4.s3,root.sg2.d4.s1,root.sg2.d4.s2"};
    retArray =
        new String[] {
          "1,1111,1111.1,true,", "5,5555,5555.5,false,",
        };
    resultSetEqualTest("SELECT * FROM root.sg2.** slimit 3;", expectedHeader, retArray);

    // 2. aggregation
    expectedHeader = new String[] {"Device,count(s1 + 1)"};
    retArray =
        new String[] {
          "root.sg1.d1,2,", "root.sg1.d2,2,", "root.sg1.d3,2,", "root.sg1.d4,2,",
        };
    resultSetEqualTest(
        "select count(s1+1) from root.sg1.** align by device;", expectedHeader, retArray);
    expectedHeader = new String[] {"Device,count(s1 + 1)"};
    retArray =
        new String[] {
          "root.sg2.d1,2,", "root.sg2.d2,2,", "root.sg2.d3,2,", "root.sg2.d4,2,",
        };
    resultSetEqualTest(
        "select count(s1+1) from root.sg2.** align by device;", expectedHeader, retArray);

    assertTestFail(
        "select s1 from root.sg1.** where s1 align by device;",
        "The output type of the expression in WHERE clause should be BOOLEAN, actual data type: FLOAT.");

    assertTestFail(
        "select s1 from root.sg2.** where s1 align by device;",
        "The output type of the expression in WHERE clause should be BOOLEAN, actual data type: FLOAT.");
  }

  @Test
  public void sLimitOffsetTest() {
    // 1. original
    String[] expectedHeader = new String[] {"Time,Device,s1"};
    String[] retArray =
        new String[] {
          "1,root.sg1.d1,1.1,",
          "2,root.sg1.d1,2.2,",
          "1,root.sg1.d2,11.1,",
          "2,root.sg1.d2,22.2,",
          "1,root.sg1.d3,111.1,",
          "4,root.sg1.d3,444.4,",
          "1,root.sg1.d4,1111.1,",
          "5,root.sg1.d4,5555.5,",
        };
    resultSetEqualTest(
        "SELECT * FROM root.sg1.** slimit 1 soffset 1 ALIGN BY DEVICE;", expectedHeader, retArray);
    retArray =
        new String[] {
          "1,root.sg2.d1,1.1,",
          "2,root.sg2.d1,2.2,",
          "1,root.sg2.d2,11.1,",
          "2,root.sg2.d2,22.2,",
          "1,root.sg2.d3,111.1,",
          "4,root.sg2.d3,444.4,",
          "1,root.sg2.d4,1111.1,",
          "5,root.sg2.d4,5555.5,",
        };
    resultSetEqualTest(
        "SELECT * FROM root.sg2.** slimit 1 soffset 1 ALIGN BY DEVICE;", expectedHeader, retArray);

    expectedHeader = new String[] {"Time,Device,s1,s2"};
    retArray =
        new String[] {
          "1,root.sg1.d1,1.1,false,",
          "2,root.sg1.d1,2.2,false,",
          "1,root.sg1.d2,11.1,false,",
          "2,root.sg1.d2,22.2,false,",
          "1,root.sg1.d3,111.1,true,",
          "4,root.sg1.d3,444.4,true,",
          "1,root.sg1.d4,1111.1,true,",
          "5,root.sg1.d4,5555.5,false,",
        };
    resultSetEqualTest(
        "SELECT *, s1 FROM root.sg1.** slimit 2 soffset 1 ALIGN BY DEVICE;",
        expectedHeader,
        retArray);
    retArray =
        new String[] {
          "1,root.sg2.d1,1.1,false,",
          "2,root.sg2.d1,2.2,false,",
          "1,root.sg2.d2,11.1,false,",
          "2,root.sg2.d2,22.2,false,",
          "1,root.sg2.d3,111.1,true,",
          "4,root.sg2.d3,444.4,true,",
          "1,root.sg2.d4,1111.1,true,",
          "5,root.sg2.d4,5555.5,false,",
        };
    resultSetEqualTest(
        "SELECT *, s1 FROM root.sg2.** slimit 2 soffset 1 ALIGN BY DEVICE;",
        expectedHeader,
        retArray);

    expectedHeader = new String[] {"Time,Device,s1"};
    retArray =
        new String[] {
          "1,root.sg1.d1,1.1,", "2,root.sg1.d1,2.2,", "1,root.sg1.d2,11.1,", "2,root.sg1.d2,22.2,",
        };
    resultSetEqualTest(
        "SELECT * FROM root.sg1.d1,root.sg1.d2,root.sg1.d6 soffset 1 slimit 1 ALIGN BY DEVICE;",
        expectedHeader,
        retArray);
    retArray =
        new String[] {
          "1,root.sg2.d1,1.1,", "2,root.sg2.d1,2.2,", "1,root.sg2.d2,11.1,", "2,root.sg2.d2,22.2,",
        };
    resultSetEqualTest(
        "SELECT * FROM root.sg2.d1,root.sg2.d2,root.sg2.d6 soffset 1 slimit 1 ALIGN BY DEVICE;",
        expectedHeader,
        retArray);
  }

  @Test
  public void emptyResultTest() {
    String[] expectedHeader = new String[] {"Time,Device,s3,s1,s2"};
    String[] retArray = new String[] {};
    // We set timePartitionInterval to 1 in IoTDBAlignByDeviceWithTemplate3IT, the where predicate
    // is changed from 'time>=now()-1d' to 'time>=now()-1ms' to reduce memory use because of the
    // creation of timeslots.
    resultSetEqualTest(
        "SELECT * FROM root.sg1.** where time>=now()-1ms and time<=now() "
            + "ORDER BY TIME DESC ALIGN BY DEVICE;",
        expectedHeader,
        retArray);
    resultSetEqualTest(
        "SELECT * FROM root.sg2.** where time>=now()-1ms and time<=now() "
            + "ORDER BY TIME DESC ALIGN BY DEVICE;",
        expectedHeader,
        retArray);
  }

  protected static void insertData() {
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {

      for (String sql : sqls) {
        statement.execute(sql);
      }
    } catch (Exception e) {
      e.printStackTrace();
    }
  }
}
