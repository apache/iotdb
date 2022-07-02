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
import org.apache.iotdb.it.env.IoTDBTestRunner;
import org.apache.iotdb.itbase.category.ClusterIT;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static org.apache.iotdb.db.it.utils.TestUtils.prepareData;
import static org.apache.iotdb.db.it.utils.TestUtils.resultSetEqualTest;
import static org.apache.iotdb.db.it.utils.TestUtils.resultSetEqualWithDescOrderTest;

@RunWith(IoTDBTestRunner.class)
@Category({ClusterIT.class}) // TODO add LocalStandaloneIT
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
   * 6, 6, null, 6.0, 6.0, null, t6<br>
   * 7, null, null, null, null, null, null<br>
   * 8, 8, 8, 8.0, 8.0, true, t8<br>
   * 9, null, 9, null, 9.0, null, t9
   */
  private static final String[] sqls =
      new String[] {
        "SET STORAGE GROUP TO root.sg1",
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

  @BeforeClass
  public static void setUp() throws Exception {
    EnvFactory.getEnv().initBeforeClass();
    prepareData(sqls);
  }

  @AfterClass
  public static void tearDown() throws Exception {
    EnvFactory.getEnv().cleanAfterClass();
  }

  @Test
  public void previousFillTest() {
    String[] expectedHeader =
        new String[] {
          "Time",
          "root.sg1.d1.s1",
          "root.sg1.d1.s2",
          "root.sg1.d1.s3",
          "root.sg1.d1.s4",
          "root.sg1.d1.s5",
          "root.sg1.d1.s6"
        };
    String[] retArray =
        new String[] {
          "1,null,1,null,1.0,null,t1,",
          "2,2,2,2.0,2.0,true,t2,",
          "3,3,2,3.0,2.0,false,t2,",
          "4,3,4,3.0,4.0,false,t4",
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
    String[] expectedHeader =
        new String[] {
          "Time",
          "root.sg1.d1.s1",
          "root.sg1.d1.s2",
          "root.sg1.d1.s3",
          "root.sg1.d1.s4",
          "root.sg1.d1.s5",
          "root.sg1.d1.s6"
        };
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
  public void linearFillTest() {
    String[] expectedHeader =
        new String[] {
          "Time", "root.sg1.d1.s1", "root.sg1.d1.s2", "root.sg1.d1.s3", "root.sg1.d1.s4"
        };
    String[] retArray =
        new String[] {
          "1,null,1,null,1.0,",
          "2,2,2,2.0,2.0,",
          "3,3,3,3.0,3.0,",
          "4,4,4,4.0,4.0,",
          "5,5,5,5.0,5.0,",
          "6,6,6,6.5,6.0,",
          "8,8,8,8.0,8.0,",
          "9,9,null,9.0,null,"
        };
    resultSetEqualWithDescOrderTest(
        "select s1, s2, s3, s4 from root.sg1.d1 fill(linear)", expectedHeader, retArray);
  }

  @Test
  public void intFillTest() {
    String[] expectedHeader =
        new String[] {
          "Time",
          "root.sg1.d1.s1",
          "root.sg1.d1.s2",
          "root.sg1.d1.s3",
          "root.sg1.d1.s4",
          "root.sg1.d1.s6"
        };
    String[] retArray =
        new String[] {
          "1,1000,1,1000.0,1.0,t1,",
          "2,2,2,2.0,2.0,t2,",
          "3,3,1000,3.0,1000.0,1000",
          "4,1000,4,1000.0,4.0,t4,",
          "5,5,5,5.0,5.0,t5,",
          "6,1000,6,1000.0,6.0,t6,",
          "8,8,8,8.0,8.0,1000,",
          "9,9,1000,9.0,1000.0,t8,"
        };
    resultSetEqualWithDescOrderTest(
        "select s1, s2, s3, s4, s6 from root.sg1.d1 fill(1000)", expectedHeader, retArray);
  }

  @Test
  public void floatFillTest() {
    String[] expectedHeader =
        new String[] {"Time", "root.sg1.d1.s3", "root.sg1.d1.s4", "root.sg1.d1.s6"};
    String[] retArray =
        new String[] {
          "1,3.14,1.0,t1,",
          "2,2.0,2.0,t2,",
          "3,3.0,3.14,3.14,",
          "4,3.14,4.0,t4,",
          "5,5.0,5.0,t5,",
          "6,3.14,6.0,t6,",
          "8,8.0,8.0,t8,",
          "9,9.0,3.14,3.14,"
        };
    resultSetEqualWithDescOrderTest(
        "select s3, s4, s6 from root.sg1.d1 fill(3.14)", expectedHeader, retArray);
  }

  @Test
  public void booleanFillTest() {
    String[] expectedHeader = new String[] {"Time", "root.sg1.d1.s5", "root.sg1.d1.s6"};
    String[] retArray =
        new String[] {
          "1,true,t1,",
          "2,true,t2,",
          "3,false,true,",
          "4,true,t4,",
          "5,false,t5,",
          "6,true,t6,",
          "8,true,t8,",
          "9,true,true,"
        };
    resultSetEqualWithDescOrderTest(
        "select s5, s6 from root.sg1.d1 fill(true)", expectedHeader, retArray);
  }

  @Test
  public void textFillTest() {
    String[] expectedHeader = new String[] {"Time", "root.sg1.d1.s6", "root.sg1.d2.s6"};
    String[] retArray =
        new String[] {
          "1,t1,t0,",
          "2,t2,t2,",
          "3,t0,t3,",
          "4,t4,t0,",
          "5,t5,t5,",
          "6,t6,t0,",
          "8,t8,t8,",
          "9,t0,t9,"
        };
    resultSetEqualWithDescOrderTest(
        "select s6 from root.sg1.d1, root.sg1.d2 fill('t0')", expectedHeader, retArray);
  }
}
