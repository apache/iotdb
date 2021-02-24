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
package org.apache.iotdb.db.query.dataset.groupby;

import org.apache.iotdb.db.exception.query.QueryProcessException;
import org.apache.iotdb.db.qp.Planner;
import org.apache.iotdb.db.qp.executor.IPlanExecutor;
import org.apache.iotdb.db.qp.executor.PlanExecutor;
import org.apache.iotdb.db.qp.physical.crud.QueryPlan;
import org.apache.iotdb.db.service.IoTDB;
import org.apache.iotdb.db.utils.EnvironmentUtils;
import org.apache.iotdb.tsfile.read.query.dataset.QueryDataSet;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.*;

public class GroupByLevelDataSetTest {
  private IPlanExecutor queryExecutor = new PlanExecutor();
  private Planner processor = new Planner();
  private String[] sqls = {
    "SET STORAGE GROUP TO root.vehicle",
    "SET STORAGE GROUP TO root.test",
    "CREATE TIMESERIES root.vehicle.d0.s0 WITH DATATYPE=INT32, ENCODING=RLE",
    "CREATE TIMESERIES root.vehicle.d0.s1 WITH DATATYPE=TEXT, ENCODING=PLAIN",
    "CREATE TIMESERIES root.test.d0.s0 WITH DATATYPE=INT32, ENCODING=RLE",
    "CREATE TIMESERIES root.test.d0.s1 WITH DATATYPE=TEXT, ENCODING=PLAIN",
    "CREATE TIMESERIES root.test.d0.s3 WITH DATATYPE=TEXT, ENCODING=PLAIN",
    "CREATE TIMESERIES root.test.d1.\"s3.xy\" WITH DATATYPE=TEXT, ENCODING=PLAIN",
    "insert into root.vehicle.d0(timestamp,s0) values(10,100)",
    "insert into root.vehicle.d0(timestamp,s0,s1) values(12,101,'102')",
    "insert into root.vehicle.d0(timestamp,s1) values(19,'103')",
    "insert into root.vehicle.d0(timestamp,s0) values(20,1000)",
    "insert into root.vehicle.d0(timestamp,s0,s1) values(22,1001,'1002')",
    "insert into root.vehicle.d0(timestamp,s1) values(29,'1003')",
    "insert into root.test.d0(timestamp,s0) values(10,106)",
    "insert into root.test.d0(timestamp,s0,s1) values(14,107,'108')",
    "insert into root.test.d0(timestamp,s1) values(16,'109')",
    "insert into root.test.d0(timestamp,s0) values(30,1006)",
    "insert into root.test.d0(timestamp,s0,s1) values(34,1007,'1008')",
    "insert into root.test.d0(timestamp,s1) values(36,'1090')",
    "insert into root.vehicle.d0(timestamp,s0) values(6,120)",
    "insert into root.vehicle.d0(timestamp,s0,s1) values(38,121,'122')",
    "insert into root.vehicle.d0(timestamp,s1) values(9,'123')",
    "insert into root.vehicle.d0(timestamp,s0) values(16,128)",
    "insert into root.vehicle.d0(timestamp,s0,s1) values(18,189,'198')",
    "insert into root.vehicle.d0(timestamp,s1) values(99,'1234')",
    "insert into root.test.d0(timestamp,s0) values(15,126)",
    "insert into root.test.d0(timestamp,s0,s1) values(8,127,'128')",
    "insert into root.test.d0(timestamp,s1) values(20,'129')",
    "insert into root.test.d0(timestamp,s0) values(150,426)",
    "insert into root.test.d0(timestamp,s0,s1) values(80,427,'528')",
    "insert into root.test.d0(timestamp,s1) values(2,'1209')",
    "insert into root.vehicle.d0(timestamp,s0) values(209,130)",
    "insert into root.vehicle.d0(timestamp,s0,s1) values(206,131,'132')",
    "insert into root.vehicle.d0(timestamp,s1) values(70,'33')",
    "insert into root.test.d0(timestamp,s0) values(19,136)",
    "insert into root.test.d0(timestamp,s0,s1) values(7,137,'138')",
    "insert into root.test.d0(timestamp,s1) values(30,'139')",
    "insert into root.test.d0(timestamp,s0) values(1900,1316)",
    "insert into root.test.d0(timestamp,s0,s1) values(700,1307,'1038')",
    "insert into root.test.d0(timestamp,s1) values(3000,'1309')",
    "insert into root.test.d0(timestamp,s3) values(10,'100')",
    "insert into root.test.d1(timestamp, \"s3.xy\") values(10, 'text')"
  };

  static {
    IoTDB.metaManager.init();
  }

  public GroupByLevelDataSetTest() throws QueryProcessException {}

  @Before
  public void setUp() throws Exception {
    EnvironmentUtils.envSetUp();
    for (String sql : sqls) {
      queryExecutor.processNonQuery(processor.parseSQLToPhysicalPlan(sql));
    }
  }

  @After
  public void tearDown() throws Exception {
    EnvironmentUtils.cleanEnv();
  }

  @Test
  public void testGroupByLevel() throws Exception {
    QueryPlan queryPlan =
        (QueryPlan)
            processor.parseSQLToPhysicalPlan("select count(s1) from root.test.* group by level=1");
    QueryDataSet dataSet =
        queryExecutor.processQuery(queryPlan, EnvironmentUtils.TEST_QUERY_CONTEXT);

    assertTrue(dataSet.hasNext());
    assertEquals("0\t12", dataSet.next().toString());

    queryPlan =
        (QueryPlan)
            processor.parseSQLToPhysicalPlan("select count(s1) from root.test.* group by level=0");
    dataSet = queryExecutor.processQuery(queryPlan, EnvironmentUtils.TEST_QUERY_CONTEXT);

    assertTrue(dataSet.hasNext());
    assertEquals("0\t12", dataSet.next().toString());

    queryPlan =
        (QueryPlan)
            processor.parseSQLToPhysicalPlan("select count(s1) from root.test.* group by level=6");
    dataSet = queryExecutor.processQuery(queryPlan, EnvironmentUtils.TEST_QUERY_CONTEXT);

    assertTrue(dataSet.hasNext());
    assertEquals("0\t12", dataSet.next().toString());

    // multi paths
    queryPlan =
        (QueryPlan)
            processor.parseSQLToPhysicalPlan(
                "select count(s1) from root.test.*,root.vehicle.* group by level=1");
    dataSet = queryExecutor.processQuery(queryPlan, EnvironmentUtils.TEST_QUERY_CONTEXT);

    assertTrue(dataSet.hasNext());
    assertEquals("0\t12\t10", dataSet.next().toString());

    // with double quotation mark
    queryPlan =
        (QueryPlan)
            processor.parseSQLToPhysicalPlan(
                "select count(\"s3.xy\") from root.test.* group by level=2");
    dataSet = queryExecutor.processQuery(queryPlan, EnvironmentUtils.TEST_QUERY_CONTEXT);

    assertTrue(dataSet.hasNext());
    assertEquals("0\t1", dataSet.next().toString());
  }
}
