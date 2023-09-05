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

package org.apache.iotdb.db.it.query;

import org.apache.iotdb.it.env.EnvFactory;
import org.apache.iotdb.it.framework.IoTDBTestRunner;
import org.apache.iotdb.itbase.category.ClusterIT;
import org.apache.iotdb.itbase.category.LocalStandaloneIT;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static org.apache.iotdb.db.it.utils.TestUtils.prepareData;
import static org.apache.iotdb.db.it.utils.TestUtils.resultSetEqualTest;
import static org.apache.iotdb.db.it.utils.TestUtils.resultSetEqualWithDescOrderTest;

@RunWith(IoTDBTestRunner.class)
@Category({LocalStandaloneIT.class, ClusterIT.class})
public class IoTDBNullValueFillIT {

  /**
   * [root.sg1.d1 (aligned)]
   *
   * <p>Time, s1[INT32], s2[INT64], s3[FLOAT], s4[DOUBLE], s5[BOOLEAN], s6[TEXT]<br>
   * 1, null, 1, null, 1.0, null, t1<br>
   * 2, 2, 2, 2.0, 2.0, true, t2<br>
   * 3, 3, null, 3.0, null, false, null<br>
   * 4, null, 4, null, 4.0, null, t4<br>
   * 5, 5, 5, 5.0, 5.0, false, t5<br>
   * 6, null, 6, null, null, false, null<br>
   * 7, null, null, null, null, null, null<br>
   * 8, 8, 8, 8.0, 8.0, true, t8<br>
   * 9, 9, null, 9.0, null, true, null
   *
   * <p>[root.sg1.d2 (non-aligned)]
   *
   * <p>Time, s1[INT32], s2[INT64], s3[FLOAT], s4[DOUBLE], s5[BOOLEAN], s6[TEXT]<br>
   * 1, 1, null, 1.0, null, true, null<br>
   * 2, 2, 2, 2.0, 2.0, true, t2<br>
   * 3, null, 3, null, 3.0, null, t3<br>
   * 4, 4, null, 4.0, null, false, null<br>
   * 5, 5, 5, 5.0, 5.0, false, t5<br>
   * 6, 6, null, 6.0, null, false, null<br>
   * 7, null, null, null, null, null, null<br>
   * 8, 8, 8, 8.0, 8.0, true, t8<br>
   * 9, null, 9, null, 9.0, null, t9
   */
  private static final String[] sqls =
      new String[] {
        "CREATE DATABASE root.sg1",
        "create aligned timeseries root.sg1.d1(s1 INT32, s2 INT64, s3 FLOAT, s4 DOUBLE, s5 BOOLEAN, s6 TEXT)",
        "insert into root.sg1.d1(time, s2, s4, s6) aligned values(1, 1, 1.0, 't1')",
        "insert into root.sg1.d1(time, s1, s2, s3, s4, s5, s6) aligned values(2, 2, 2, 2.0, 2.0, true, 't2')",
        "insert into root.sg1.d1(time, s1, s3, s5) aligned values(3, 3, 3.0, false)",
        "insert into root.sg1.d1(time, s2, s4, s6) aligned values(4, 4, 4.0, 't4')",
        "insert into root.sg1.d1(time, s1, s2, s3, s4, s5, s6) aligned values(5, 5, 5, 5.0, 5.0, false, 't5')",
        "insert into root.sg1.d1(time, s2, s4, s6) aligned values(6, 6, 6.0, 't6')",
        "insert into root.sg1.d1(time, s1, s2, s3, s4, s5, s6) aligned values(8, 8, 8, 8.0, 8.0, true, 't8')",
        "insert into root.sg1.d1(time, s1, s3, s5) aligned values(9, 9, 9.0, true)",
        "create timeseries root.sg1.d2.s1 INT32",
        "create timeseries root.sg1.d2.s2 INT64",
        "create timeseries root.sg1.d2.s3 FLOAT",
        "create timeseries root.sg1.d2.s4 DOUBLE",
        "create timeseries root.sg1.d2.s5 BOOLEAN",
        "create timeseries root.sg1.d2.s6 TEXT",
        "insert into root.sg1.d2(time, s1, s3, s5) values(1, 1, 1.0, true)",
        "insert into root.sg1.d2(time, s1, s2, s3, s4, s5, s6) values(2, 2, 2, 2.0, 2.0, true, 't2')",
        "insert into root.sg1.d2(time, s2, s4, s6) values(3, 3, 3.0, 't3')",
        "insert into root.sg1.d2(time, s1, s3, s5) values(4, 4, 4.0, false)",
        "insert into root.sg1.d2(time, s1, s2, s3, s4, s5, s6) values(5, 5, 5, 5.0, 5.0, false, 't5')",
        "insert into root.sg1.d2(time, s1, s3, s5) values(6, 6, 6.0, false)",
        "insert into root.sg1.d2(time, s1, s2, s3, s4, s5, s6) values(8, 8, 8, 8.0, 8.0, true, 't8')",
        "insert into root.sg1.d2(time, s2, s4, s6) values(9, 9, 9.0, 't9')"
      };

  private final String[] expectedHeader =
      new String[] {
        "Time",
        "root.sg1.d1.s1",
        "root.sg1.d1.s2",
        "root.sg1.d1.s3",
        "root.sg1.d1.s4",
        "root.sg1.d1.s5",
        "root.sg1.d1.s6"
      };

  private final String[] expectedAlignByDeviceHeader =
      new String[] {"Time", "Device", "s1", "s2", "s3", "s4", "s5", "s6"};

  @BeforeClass
  public static void setUp() throws Exception {
    EnvFactory.getEnv().initClusterEnvironment();
    prepareData(sqls);
  }

  @AfterClass
  public static void tearDown() throws Exception {
    EnvFactory.getEnv().cleanClusterEnvironment();
  }

  @Test
  public void previousFillTest() {
    String[] retArray =
        new String[] {
          "1,null,1,null,1.0,null,t1,",
          "2,2,2,2.0,2.0,true,t2,",
          "3,3,2,3.0,2.0,false,t2,",
          "4,3,4,3.0,4.0,false,t4,",
          "5,5,5,5.0,5.0,false,t5,",
          "6,5,6,5.0,6.0,false,t6,",
          "8,8,8,8.0,8.0,true,t8,",
          "9,9,8,9.0,8.0,true,t8,"
        };
    resultSetEqualTest(
        "select s1, s2, s3, s4, s5, s6 from root.sg1.d1 fill(previous)", expectedHeader, retArray);
  }

  @Test
  public void previousDescFillTest() {
    String[] retArray =
        new String[] {
          "9,9,null,9.0,null,true,null,",
          "8,8,8,8.0,8.0,true,t8,",
          "6,8,6,8.0,6.0,true,t6,",
          "5,5,5,5.0,5.0,false,t5,",
          "4,5,4,5.0,4.0,false,t4,",
          "3,3,4,3.0,4.0,false,t4,",
          "2,2,2,2.0,2.0,true,t2,",
          "1,2,1,2.0,1.0,true,t1,"
        };
    resultSetEqualTest(
        "select s1, s2, s3, s4, s5, s6 from root.sg1.d1 fill(previous) order by time desc",
        expectedHeader,
        retArray);
  }

  @Test
  public void previousFillAlignByDeviceTest() {
    String[] retArray =
        new String[] {
          "1,root.sg1.d1,null,1,null,1.0,null,t1,",
          "2,root.sg1.d1,2,2,2.0,2.0,true,t2,",
          "3,root.sg1.d1,3,2,3.0,2.0,false,t2,",
          "4,root.sg1.d1,3,4,3.0,4.0,false,t4,",
          "5,root.sg1.d1,5,5,5.0,5.0,false,t5,",
          "6,root.sg1.d1,5,6,5.0,6.0,false,t6,",
          "8,root.sg1.d1,8,8,8.0,8.0,true,t8,",
          "9,root.sg1.d1,9,8,9.0,8.0,true,t8,",
          "1,root.sg1.d2,1,8,1.0,8.0,true,t8,",
          "2,root.sg1.d2,2,2,2.0,2.0,true,t2,",
          "3,root.sg1.d2,2,3,2.0,3.0,true,t3,",
          "4,root.sg1.d2,4,3,4.0,3.0,false,t3,",
          "5,root.sg1.d2,5,5,5.0,5.0,false,t5,",
          "6,root.sg1.d2,6,5,6.0,5.0,false,t5,",
          "8,root.sg1.d2,8,8,8.0,8.0,true,t8,",
          "9,root.sg1.d2,8,9,8.0,9.0,true,t9,"
        };
    resultSetEqualTest(
        "select s1, s2, s3, s4, s5, s6 from root.sg1.* fill(previous) align by device",
        expectedAlignByDeviceHeader,
        retArray);
  }

  @Test
  public void previousDescFillAlignByDeviceTest() {
    String[] retArray =
        new String[] {
          "9,root.sg1.d1,9,null,9.0,null,true,null,",
          "8,root.sg1.d1,8,8,8.0,8.0,true,t8,",
          "6,root.sg1.d1,8,6,8.0,6.0,true,t6,",
          "5,root.sg1.d1,5,5,5.0,5.0,false,t5,",
          "4,root.sg1.d1,5,4,5.0,4.0,false,t4,",
          "3,root.sg1.d1,3,4,3.0,4.0,false,t4,",
          "2,root.sg1.d1,2,2,2.0,2.0,true,t2,",
          "1,root.sg1.d1,2,1,2.0,1.0,true,t1,",
          "9,root.sg1.d2,2,9,2.0,9.0,true,t9,",
          "8,root.sg1.d2,8,8,8.0,8.0,true,t8,",
          "6,root.sg1.d2,6,8,6.0,8.0,false,t8,",
          "5,root.sg1.d2,5,5,5.0,5.0,false,t5,",
          "4,root.sg1.d2,4,5,4.0,5.0,false,t5,",
          "3,root.sg1.d2,4,3,4.0,3.0,false,t3,",
          "2,root.sg1.d2,2,2,2.0,2.0,true,t2,",
          "1,root.sg1.d2,1,2,1.0,2.0,true,t2,"
        };
    resultSetEqualTest(
        "select s1, s2, s3, s4, s5, s6 from root.sg1.* fill(previous) order by device,time desc align by device",
        expectedAlignByDeviceHeader,
        retArray);
  }

  @Test
  public void linearFillTest() {
    String[] retArray =
        new String[] {
          "1,null,1,null,1.0,null,t1,",
          "2,2,2,2.0,2.0,true,t2,",
          "3,3,3,3.0,3.0,false,null,",
          "4,4,4,4.0,4.0,null,t4,",
          "5,5,5,5.0,5.0,false,t5,",
          "6,6,6,6.5,6.0,null,t6,",
          "8,8,8,8.0,8.0,true,t8,",
          "9,9,null,9.0,null,true,null,"
        };
    resultSetEqualWithDescOrderTest(
        "select s1, s2, s3, s4, s5, s6 from root.sg1.d1 fill(linear)", expectedHeader, retArray);
  }

  @Test
  public void linearFillAlignByDeviceTest() {
    String[] retArray =
        new String[] {
          "1,root.sg1.d1,null,1,null,1.0,null,t1,",
          "2,root.sg1.d1,2,2,2.0,2.0,true,t2,",
          "3,root.sg1.d1,3,3,3.0,3.0,false,null,",
          "4,root.sg1.d1,4,4,4.0,4.0,null,t4,",
          "5,root.sg1.d1,5,5,5.0,5.0,false,t5,",
          "6,root.sg1.d1,6,6,6.5,6.0,null,t6,",
          "8,root.sg1.d1,8,8,8.0,8.0,true,t8,",
          "9,root.sg1.d1,9,5,9.0,5.0,true,null,",
          "1,root.sg1.d2,1,5,1.0,5.0,true,null,",
          "2,root.sg1.d2,2,2,2.0,2.0,true,t2,",
          "3,root.sg1.d2,3,3,3.0,3.0,null,t3,",
          "4,root.sg1.d2,4,4,4.0,4.0,false,null,",
          "5,root.sg1.d2,5,5,5.0,5.0,false,t5,",
          "6,root.sg1.d2,6,6,6.0,6.5,false,null,",
          "8,root.sg1.d2,8,8,8.0,8.0,true,t8,",
          "9,root.sg1.d2,null,9,null,9.0,null,t9,"
        };
    resultSetEqualTest(
        "select s1, s2, s3, s4, s5, s6 from root.sg1.* fill(linear) align by device",
        expectedAlignByDeviceHeader,
        retArray);
  }

  @Test
  public void intFillTest() {
    String[] retArray =
        new String[] {
          "1,1000,1,1000.0,1.0,null,t1,",
          "2,2,2,2.0,2.0,true,t2,",
          "3,3,1000,3.0,1000.0,false,1000,",
          "4,1000,4,1000.0,4.0,null,t4,",
          "5,5,5,5.0,5.0,false,t5,",
          "6,1000,6,1000.0,6.0,null,t6,",
          "8,8,8,8.0,8.0,true,t8,",
          "9,9,1000,9.0,1000.0,true,1000,"
        };
    resultSetEqualWithDescOrderTest(
        "select s1, s2, s3, s4, s5, s6 from root.sg1.d1 fill(1000)", expectedHeader, retArray);
  }

  @Test
  public void floatFillTest() {
    String[] retArray =
        new String[] {
          "1,null,1,3.14,1.0,null,t1,",
          "2,2,2,2.0,2.0,true,t2,",
          "3,3,null,3.0,3.14,false,3.14,",
          "4,null,4,3.14,4.0,null,t4,",
          "5,5,5,5.0,5.0,false,t5,",
          "6,null,6,3.14,6.0,null,t6,",
          "8,8,8,8.0,8.0,true,t8,",
          "9,9,null,9.0,3.14,true,3.14,"
        };
    resultSetEqualWithDescOrderTest(
        "select s1, s2, s3, s4, s5, s6 from root.sg1.d1 fill(3.14)", expectedHeader, retArray);
  }

  @Test
  public void booleanFillTest() {
    String[] retArray =
        new String[] {
          "1,null,1,null,1.0,true,t1,",
          "2,2,2,2.0,2.0,true,t2,",
          "3,3,null,3.0,null,false,true,",
          "4,null,4,null,4.0,true,t4,",
          "5,5,5,5.0,5.0,false,t5,",
          "6,null,6,null,6.0,true,t6,",
          "8,8,8,8.0,8.0,true,t8,",
          "9,9,null,9.0,null,true,true,"
        };
    resultSetEqualWithDescOrderTest(
        "select s1, s2, s3, s4, s5, s6 from root.sg1.d1 fill(true)", expectedHeader, retArray);
  }

  @Test
  public void textFillTest() {
    String[] retArray =
        new String[] {
          "1,null,1,null,1.0,null,t1,",
          "2,2,2,2.0,2.0,true,t2,",
          "3,3,null,3.0,null,false,t0,",
          "4,null,4,null,4.0,null,t4,",
          "5,5,5,5.0,5.0,false,t5,",
          "6,null,6,null,6.0,null,t6,",
          "8,8,8,8.0,8.0,true,t8,",
          "9,9,null,9.0,null,true,t0,"
        };
    resultSetEqualWithDescOrderTest(
        "select s1, s2, s3, s4, s5, s6 from root.sg1.d1 fill('t0')", expectedHeader, retArray);
  }
}
