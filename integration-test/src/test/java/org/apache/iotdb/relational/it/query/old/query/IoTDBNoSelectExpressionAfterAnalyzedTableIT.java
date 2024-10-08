/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
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

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static org.apache.iotdb.db.it.utils.TestUtils.prepareTableData;
import static org.apache.iotdb.db.it.utils.TestUtils.tableAssertTestFail;
import static org.apache.iotdb.itbase.constant.TestConstant.TIMESTAMP_STR;

@RunWith(IoTDBTestRunner.class)
@Category({TableLocalStandaloneIT.class, TableClusterIT.class})
public class IoTDBNoSelectExpressionAfterAnalyzedTableIT {
  private static final String DATABASE_NAME = "test";
  private static final String[] SQLs =
      new String[] {
        "CREATE DATABASE " + DATABASE_NAME,
        "USE " + DATABASE_NAME,
        "CREATE TABLE sg(device STRING ID, s1 INT32 MEASUREMENT)",
        "insert into sg(time,device,s1) values(1,'d1',1)",
        "insert into sg(time,device,s1,s2) values(1,'d1',1,1)"
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
  public void testAlignByDevice() {
    String[] expectedHeader = new String[] {TIMESTAMP_STR};
    String[] retArray = new String[] {};
    tableAssertTestFail(
        "select s2 from sg where s1>0 order by device",
        "701: Column 's2' cannot be resolved",
        DATABASE_NAME);

    // TODO After Aggregation supported
    /*tableResultSetEqualTest(
    "select count(s2) from sg where s1>0 order by device", expectedHeader, retArray,DATABASE_NAME);*/

    // mix test
    /* expectedHeader = new String[] {DEVICE, count(s1), count(s2)};
    retArray = new String[] {"sg,1,null,", "root.sg.d2,1,1,"};
    tableResultSetEqualTest(
        "select count(s1), count(s2) from sg where s1>0 order by device",
        expectedHeader,
        retArray,DATABASE_NAME);*/

    tableAssertTestFail(
        "select s1, s2 from sg where s1>0 order by device",
        "701: Column 's2' cannot be resolved",
        DATABASE_NAME);
  }

  @Test
  public void testAlignByTime() {
    tableAssertTestFail(
        "select s2 from sg where s1>0", "701: Column 's2' cannot be resolved", DATABASE_NAME);

    /*tableResultSetEqualTest("select count(s2) from sg where s1>0", expectedHeader, retArray,DATABASE_NAME);*/
  }
}
