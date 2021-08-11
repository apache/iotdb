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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class GroupByFillDataSetTest {

  private IPlanExecutor queryExecutor = new PlanExecutor();
  private Planner processor = new Planner();
  private String[] sqls = {
    "SET STORAGE GROUP TO root.vehicle",
    "CREATE TIMESERIES root.vehicle.d0.s0 WITH DATATYPE=INT32, ENCODING=RLE",
    "CREATE TIMESERIES root.vehicle.d0.s1 WITH DATATYPE=INT32, ENCODING=RLE",
    "CREATE TIMESERIES root.vehicle.d0.s2 WITH DATATYPE=INT32, ENCODING=RLE",
    "insert into root.vehicle.d0(timestamp,s0) values(1,1)",
    "insert into root.vehicle.d0(timestamp,s1) values(1,1)",
    "insert into root.vehicle.d0(timestamp,s2) values(1,1)",
    "insert into root.vehicle.d0(timestamp,s2) values(2,2)",
    "insert into root.vehicle.d0(timestamp,s2) values(3,3)",
    "insert into root.vehicle.d0(timestamp,s2) values(4,4)",
    "insert into root.vehicle.d0(timestamp,s2) values(5,5)",
    "flush",
    "insert into root.vehicle.d0(timestamp,s0) values(6,6)",
    "insert into root.vehicle.d0(timestamp,s0) values(7,7)",
    "insert into root.vehicle.d0(timestamp,s0) values(8,8)",
    "insert into root.vehicle.d0(timestamp,s1) values(6,6)",
    "insert into root.vehicle.d0(timestamp,s1) values(7,7)",
    "insert into root.vehicle.d0(timestamp,s2) values(6,6)",
    "insert into root.vehicle.d0(timestamp,s2) values(7,7)",
  };

  static {
    IoTDB.metaManager.init();
  }

  public GroupByFillDataSetTest() throws QueryProcessException {}

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
  public void groupByFillTest() throws Exception {
    QueryPlan queryPlan =
        (QueryPlan)
            processor.parseSQLToPhysicalPlan(
                "select last_value(s0) from root.vehicle.* group by ([0,20), 1ms) fill (int32[Previous]) order by time desc");
    QueryDataSet dataSet =
        queryExecutor.processQuery(queryPlan, EnvironmentUtils.TEST_QUERY_CONTEXT);
    assertTrue(dataSet.hasNext());
    assertEquals("19\t8", dataSet.next().toString());
    for (int i = 0; i < 10; i++) {
      dataSet.hasNext();
      dataSet.next();
    }
    assertTrue(dataSet.hasNext());
    assertEquals("8\t8", dataSet.next().toString());
    for (int i = 7; i > -1; i--) {
      assertTrue(dataSet.hasNext());
      if (i > 5) {
        assertEquals(i + "\t" + i, dataSet.next().toString());
      } else if (i > 0) {
        assertEquals(i + "\t" + 1, dataSet.next().toString());
      } else {
        assertEquals(i + "\t" + "null", dataSet.next().toString());
      }
    }
  }

  @Test
  public void groupByWithValueFilterFillTest() throws Exception {
    QueryPlan queryPlan =
        (QueryPlan)
            processor.parseSQLToPhysicalPlan(
                "select last_value(s0) from root.vehicle.* where s1 > 1  group by ([0,20), 1ms) fill (int32[Previous]) order by time desc");
    QueryDataSet dataSet =
        queryExecutor.processQuery(queryPlan, EnvironmentUtils.TEST_QUERY_CONTEXT);
    for (int i = 19; i >= 7; i--) {
      assertTrue(dataSet.hasNext());
      assertEquals(i + "\t7", dataSet.next().toString());
    }
    assertTrue(dataSet.hasNext());
    assertEquals("6\t6", dataSet.next().toString());
    for (int i = 5; i >= 0; i--) {
      assertTrue(dataSet.hasNext());
      assertEquals(i + "\tnull", dataSet.next().toString());
    }
  }

  @Test
  public void groupByWithAndFilterFillTest() throws Exception {
    QueryPlan queryPlan =
        (QueryPlan)
            processor.parseSQLToPhysicalPlan(
                "select last_value(s0) from root.vehicle.* where s1 > 1 or s0 > 1  group by ([0,20), 1ms) fill (int32[Previous]) order by time desc");
    QueryDataSet dataSet =
        queryExecutor.processQuery(queryPlan, EnvironmentUtils.TEST_QUERY_CONTEXT);
    for (int i = 19; i >= 8; i--) {
      assertTrue(dataSet.hasNext());
      assertEquals(i + "\t8", dataSet.next().toString());
    }
    assertTrue(dataSet.hasNext());
    assertEquals("7\t7", dataSet.next().toString());
    assertTrue(dataSet.hasNext());
    assertEquals("6\t6", dataSet.next().toString());
    for (int i = 5; i >= 0; i--) {
      assertTrue(dataSet.hasNext());
      assertEquals(i + "\tnull", dataSet.next().toString());
    }
  }

  @Test
  public void groupByWithFirstNullTest() throws Exception {
    QueryPlan queryPlan =
        (QueryPlan)
            processor.parseSQLToPhysicalPlan(
                "select last_value(s0) from root.vehicle.* where s1 > 1 or s0 > 1  group by ([5,20), 1ms) fill (int32[Previous]) order by time desc");
    QueryDataSet dataSet =
        queryExecutor.processQuery(queryPlan, EnvironmentUtils.TEST_QUERY_CONTEXT);
    for (int i = 19; i >= 8; i--) {
      assertTrue(dataSet.hasNext());
      assertEquals(i + "\t8", dataSet.next().toString());
    }
    assertTrue(dataSet.hasNext());
    assertEquals("7\t7", dataSet.next().toString());
    assertTrue(dataSet.hasNext());
    assertEquals("6\t6", dataSet.next().toString());
    assertTrue(dataSet.hasNext());
    assertEquals("5\t1", dataSet.next().toString());
  }

  @Test
  public void groupByWithCross() throws Exception {
    QueryPlan queryPlan =
        (QueryPlan)
            processor.parseSQLToPhysicalPlan(
                "select last_value(s0) from root.vehicle.* where s2 > 1 group by ([0,20), 1ms) fill (int32[Previous]) order by time desc");
    QueryDataSet dataSet =
        queryExecutor.processQuery(queryPlan, EnvironmentUtils.TEST_QUERY_CONTEXT);
    for (int i = 19; i >= 8; i--) {
      assertTrue(dataSet.hasNext());
      assertEquals(i + "\t7", dataSet.next().toString());
    }
    assertTrue(dataSet.hasNext());
    assertEquals("7\t7", dataSet.next().toString());
    assertTrue(dataSet.hasNext());
    assertEquals("6\t6", dataSet.next().toString());
    for (int i = 5; i >= 0; i--) {
      assertTrue(dataSet.hasNext());
      assertEquals(i + "\tnull", dataSet.next().toString());
    }
  }
}
