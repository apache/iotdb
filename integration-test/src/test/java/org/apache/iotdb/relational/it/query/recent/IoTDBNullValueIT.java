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

import org.apache.iotdb.relational.it.query.view.recent.IoTDBNullValueTableViewBase;

import org.junit.Test;

import static org.apache.iotdb.db.it.utils.TestUtils.prepareTableData;
import static org.apache.iotdb.db.it.utils.TestUtils.tableResultSetEqualTest;

public abstract class IoTDBNullValueIT extends IoTDBFillTableIT {
  private static final String DATABASE_NAME = "null_value_table";

  private static final String[] createSqls =
      new String[] {
        "CREATE DATABASE " + DATABASE_NAME,
        "USE " + DATABASE_NAME,
        "create table table1(id1 tag, s1 string)",
        "insert into table1 values(0, 'd1', null), (1,'d1', 1)",
        "flush",
        "insert into table1 values(0, 'd1', 0)",
        "flush"
      };

  static void prepareNullValueFixture() throws Exception {
    prepareTableData(createSqls);
    IoTDBNullValueTableViewBase.prepareViewData();
  }

  @Test
  public void nullTest() {

    // case 1: all without time filter using previous fill without timeDuration
    String[] expectedHeader = new String[] {"time", "id1", "s1"};
    String[] retArray =
        new String[] {
          "1970-01-01T00:00:00.000Z,d1,0,", "1970-01-01T00:00:00.001Z,d1,1,",
        };
    tableResultSetEqualTest("select * from table1", expectedHeader, retArray, DATABASE_NAME);
  }
}
