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

package org.apache.iotdb.relational.it.query.recent.extract;

import org.apache.iotdb.it.env.EnvFactory;
import org.apache.iotdb.itbase.category.TableClusterIT;
import org.apache.iotdb.itbase.category.TableLocalStandaloneIT;

import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import static org.apache.iotdb.db.it.utils.TestUtils.prepareTableData;
import static org.apache.iotdb.db.it.utils.TestUtils.tableResultSetEqualTest;

@Category({TableLocalStandaloneIT.class, TableClusterIT.class})
public class IoTDBExtractTable2IT extends IoTDBExtractTableIT {
  public IoTDBExtractTable2IT() {
    this.decimal = "123456";
  }

  @BeforeClass
  public static void setUp() throws Exception {
    EnvFactory.getEnv().getConfig().getCommonConfig().setTimestampPrecision("us");
    EnvFactory.getEnv().initClusterEnvironment();
    prepareTableData(createSqls);
  }

  @Test
  public void extractUsNsTest() {
    String[] expectedHeader = new String[] {"time", "_col1", "_col2"};
    String[] retArray =
        new String[] {
          getTimeStrUTC("2025-07-08T01:18:51") + ",456,0,",
        };
    tableResultSetEqualTest(
        "SELECT time, extract(us from time),extract(ns from time)"
            + " FROM table1 order by time limit 1",
        expectedHeader,
        retArray,
        DATABASE_NAME);

    retArray =
        new String[] {
          getTimeStrUTC8("2025-07-08T09:18:51") + ",456,0,",
        };
    tableResultSetEqualTest(
        "SELECT time, extract(us from time),extract(ns from time)"
            + " FROM table1 order by time limit 1",
        "+08:00",
        expectedHeader,
        retArray,
        DATABASE_NAME);
  }
}
