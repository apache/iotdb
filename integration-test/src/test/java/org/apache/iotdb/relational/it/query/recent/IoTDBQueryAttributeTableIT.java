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

import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.StatementExecutionException;

import org.junit.Test;

import static org.apache.iotdb.db.it.utils.TestUtils.prepareTableData;
import static org.apache.iotdb.db.it.utils.TestUtils.tableResultSetEqualTest;

public abstract class IoTDBQueryAttributeTableIT extends IoTDBPredicateConversionTableIT {
  public static void prepareQueryAttributeTableFixture() {
    prepareTableData(createSqls);
  }

  protected static final String DATABASE_NAME = "test_query_attribute";
  protected static final String[] createSqls =
      new String[] {
        "CREATE DATABASE " + DATABASE_NAME,
        "USE " + DATABASE_NAME,
        "create table t1(device STRING TAG, a1 STRING ATTRIBUTE, s1 INT32 FIELD)",
        "insert into t1 (time, device, a1, s1) values (1, 'd1', 'ATTR1', 1)"
      };

  @Test
  public void test1() throws IoTDBConnectionException, StatementExecutionException {
    String[] expectedHeader = new String[] {"time", "device", "a1", "s1"};
    String[] retArray = new String[] {"1970-01-01T00:00:00.001Z,d1,ATTR1,1,"};
    tableResultSetEqualTest(
        "select * from t1 where lower(a1) = 'attr1' and device = 'd1'",
        expectedHeader,
        retArray,
        DATABASE_NAME);
  }
}
