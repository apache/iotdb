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

import org.junit.Test;

import static org.apache.iotdb.db.it.utils.TestUtils.prepareTableData;
import static org.apache.iotdb.db.it.utils.TestUtils.tableAssertTestFail;
import static org.apache.iotdb.itbase.constant.TestConstant.TIMESTAMP_STR;

public abstract class IoTDBNoSelectExpressionAfterAnalyzedTableIT extends IoTDBQueryDemoTableIT {
  public static void prepareNoSelectExpressionAfterAnalyzedTableFixture() {
    prepareTableData(SQLs);
    tableAssertTestFail(
        "insert into sg(time,device,s1,s2) values(1,'d1',1,1)",
        "616: Unknown column category for s2. Cannot auto create column.",
        DATABASE_NAME);
  }

  private static final String DATABASE_NAME = "test_no_select_expression";
  private static final String[] SQLs =
      new String[] {
        "CREATE DATABASE " + DATABASE_NAME,
        "USE " + DATABASE_NAME,
        "CREATE TABLE sg(device STRING TAG, s1 INT32 FIELD)",
        "insert into sg(time,device,s1) values(1,'d1',1)"
      };

  @Test
  public void testAlignByDevice() {
    String[] expectedHeader = new String[] {TIMESTAMP_STR};
    String[] retArray = new String[] {};
    tableAssertTestFail(
        "select s2 from sg where s1>0 order by device",
        "616: Column 's2' cannot be resolved",
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
        "616: Column 's2' cannot be resolved",
        DATABASE_NAME);
  }

  @Test
  public void testAlignByTime() {
    tableAssertTestFail(
        "select s2 from sg where s1>0", "616: Column 's2' cannot be resolved", DATABASE_NAME);

    /*tableResultSetEqualTest("select count(s2) from sg where s1>0", expectedHeader, retArray,DATABASE_NAME);*/
  }
}
