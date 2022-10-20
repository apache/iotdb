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

import org.apache.iotdb.it.env.ConfigFactory;
import org.apache.iotdb.it.env.EnvFactory;
import org.apache.iotdb.it.framework.IoTDBTestRunner;
import org.apache.iotdb.itbase.category.ClusterIT;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static org.apache.iotdb.db.it.utils.TestUtils.prepareData;
import static org.apache.iotdb.db.it.utils.TestUtils.resultSetEqualTest;

@RunWith(IoTDBTestRunner.class)
@Category({ClusterIT.class})
public class IoTDBSelectIntoIT {

  private int selectIntoInsertTabletPlanRowLimit;

  private static final String[] SQLs =
      new String[] {
        "SET STORAGE GROUP TO root.sg",
        "CREATE TIMESERIES root.sg.d1.s1 WITH DATATYPE=INT32, ENCODING=RLE",
        "CREATE TIMESERIES root.sg.d1.s2 WITH DATATYPE=FLOAT, ENCODING=RLE",
        "CREATE TIMESERIES root.sg.d2.s1 WITH DATATYPE=INT32, ENCODING=RLE",
        "CREATE TIMESERIES root.sg.d2.s2 WITH DATATYPE=FLOAT, ENCODING=RLE",
        "INSERT INTO root.sg.d1(time, s1) VALUES (1, 1)",
        "INSERT INTO root.sg.d1(time, s1, s2) VALUES (2, 2, 2)",
        "INSERT INTO root.sg.d1(time, s1, s2) VALUES (3, 3, 3)",
        "INSERT INTO root.sg.d1(time, s2) VALUES (4, 4)",
        "INSERT INTO root.sg.d1(time, s1) VALUES (5, 5)",
        "INSERT INTO root.sg.d1(time, s1, s2) VALUES (6, 6, 6)",
        "INSERT INTO root.sg.d1(time, s1, s2) VALUES (7, 7, 7)",
        "INSERT INTO root.sg.d1(time, s1, s2) VALUES (8, 8, 8)",
        "INSERT INTO root.sg.d1(time, s2) VALUES (9, 9)",
        "INSERT INTO root.sg.d1(time, s1) VALUES (10, 10)",
        "INSERT INTO root.sg.d1(time, s1, s2) VALUES (11, 11, 11)",
        "INSERT INTO root.sg.d1(time, s1, s2) VALUES (12, 12, 12)",
        "INSERT INTO root.sg.d2(time, s1, s2) VALUES (1, 1, 1)",
        "INSERT INTO root.sg.d2(time, s2) VALUES (2, 2)",
        "INSERT INTO root.sg.d2(time, s1) VALUES (3, 3)",
        "INSERT INTO root.sg.d2(time, s1, s2) VALUES (4, 4, 4)",
        "INSERT INTO root.sg.d2(time, s2) VALUES (5, 5)",
        "INSERT INTO root.sg.d2(time, s2) VALUES (6, 6)",
        "INSERT INTO root.sg.d2(time, s1) VALUES (7, 7)",
        "INSERT INTO root.sg.d2(time, s1, s2) VALUES (8, 8, 8)",
        "INSERT INTO root.sg.d2(time, s1, s2) VALUES (10, 10, 10)",
        "INSERT INTO root.sg.d2(time, s2) VALUES (11, 11)",
        "INSERT INTO root.sg.d2(time, s1) VALUES (12, 12)",
        "flush"
      };

  private static final String selectIntoHeader = "source column,target timeseries,written,";

  private static final String[] queryRetArray =
      new String[] {
        "1,1,null,1,1.0,",
        "2,2,2.0,null,2.0,",
        "3,3,3.0,3,null,",
        "4,null,4.0,4,4.0,",
        "5,5,null,null,5.0,",
        "6,6,6.0,null,6.0,",
        "7,7,7.0,7,null,",
        "8,8,8.0,8,8.0,",
        "9,null,9.0,null,null,",
        "10,10,null,10,10.0,",
        "11,11,11.0,null,11.0,",
        "12,12,12.0,12,null,"
      };

  @Before
  public void setUp() throws Exception {
    selectIntoInsertTabletPlanRowLimit =
        ConfigFactory.getConfig().getSelectIntoInsertTabletPlanRowLimit();
    ConfigFactory.getConfig().setSelectIntoInsertTabletPlanRowLimit(5);
    EnvFactory.getEnv().initBeforeTest();
    prepareData(SQLs);
  }

  @After
  public void tearDown() throws Exception {
    EnvFactory.getEnv().cleanAfterTest();
    ConfigFactory.getConfig()
        .setSelectIntoInsertTabletPlanRowLimit(selectIntoInsertTabletPlanRowLimit);
  }

  // -------------------------------------- ALIGN BY TIME ---------------------------------------

  @Test
  public void testRawDataQuery1() {
    String[] intoRetArray =
        new String[] {
          "root.sg.d1.s1,root.sg_bk.new_d.t1,10,",
          "root.sg.d2.s1,root.sg_bk.new_d.t2,7,",
          "root.sg.d1.s2,root.sg_bk.new_d.t3,9,",
          "root.sg.d2.s2,root.sg_bk.new_d.t4,8,",
        };
    resultSetEqualTest(
        "select s1, s2 into root.sg_bk.new_d(t1, t2, t3, t4) from root.sg.*;",
        selectIntoHeader,
        intoRetArray);

    String expectedQueryHeader =
        "Time,root.sg_bk.new_d.t1,root.sg_bk.new_d.t3,root.sg_bk.new_d.t2,root.sg_bk.new_d.t4,";
    resultSetEqualTest(
        "select t1, t3, t2, t4 from root.sg_bk.new_d;", expectedQueryHeader, queryRetArray);
  }

  @Test
  public void testRawDataQuery2() {
    String[] intoRetArray =
        new String[] {
          "root.sg.d1.s1,root.sg_bk.new_d1.s1,10,",
          "root.sg.d2.s1,root.sg_bk.new_d2.s1,7,",
          "root.sg.d1.s2,root.sg_bk.new_d1.s2,9,",
          "root.sg.d2.s2,root.sg_bk.new_d2.s2,8,",
        };
    resultSetEqualTest(
        "select s1, s2 into root.sg_bk.new_${2}(::) from root.sg.*;",
        selectIntoHeader, intoRetArray);

    String expectedQueryHeader =
        "Time,root.sg_bk.new_d1.s1,root.sg_bk.new_d1.s2,root.sg_bk.new_d2.s1,root.sg_bk.new_d2.s2,";
    resultSetEqualTest("select new_d1.s1, new_d1.s2, new_d2.s1, new_d2.s2 from root.sg_bk;", expectedQueryHeader, queryRetArray);
  }

  // -------------------------------------- ALIGN BY DEVICE -------------------------------------
}
