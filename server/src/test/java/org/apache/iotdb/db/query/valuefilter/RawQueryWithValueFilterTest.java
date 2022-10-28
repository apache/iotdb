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

package org.apache.iotdb.db.query.valuefilter;

import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.exception.query.QueryProcessException;
import org.apache.iotdb.db.qp.Planner;
import org.apache.iotdb.db.qp.physical.crud.QueryPlan;
import org.apache.iotdb.db.query.filter.executor.IPlanExecutor;
import org.apache.iotdb.db.query.filter.executor.PlanExecutor;
import org.apache.iotdb.db.utils.EnvironmentUtils;
import org.apache.iotdb.tsfile.read.query.dataset.QueryDataSet;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.*;

public class RawQueryWithValueFilterTest {

  private static final IoTDBConfig config = IoTDBDescriptor.getInstance().getConfig();
  private IPlanExecutor queryExecutor = new PlanExecutor();
  private Planner processor = new Planner();
  private String[] sqls = {
    "SET STORAGE GROUP TO root.test",
    "CREATE TIMESERIES root.test.d0.s0 WITH DATATYPE=INT32, ENCODING=RLE",
    "insert into root.test.d0(timestamp,s0) values(1,1)",
    "insert into root.test.d0(timestamp,s0) values(2,2)",
    "insert into root.test.d0(timestamp,s0) values(3,3)",
    "insert into root.test.d0(timestamp,s0) values(10,10)",
    "flush",
    "insert into root.test.d0(timestamp,s0) values(5,5)",
    "flush"
  };
  protected static boolean enableSeqSpaceCompaction;
  protected static boolean enableUnseqSpaceCompaction;
  protected static boolean enableCrossSpaceCompaction;

  public RawQueryWithValueFilterTest() throws QueryProcessException {}

  @Before
  public void setUp() throws Exception {
    enableSeqSpaceCompaction =
        IoTDBDescriptor.getInstance().getConfig().isEnableSeqSpaceCompaction();
    enableUnseqSpaceCompaction =
        IoTDBDescriptor.getInstance().getConfig().isEnableUnseqSpaceCompaction();
    enableCrossSpaceCompaction =
        IoTDBDescriptor.getInstance().getConfig().isEnableCrossSpaceCompaction();
    config.setEnableSeqSpaceCompaction(false);
    config.setEnableUnseqSpaceCompaction(false);
    config.setEnableCrossSpaceCompaction(false);
    EnvironmentUtils.envSetUp();
    for (String sql : sqls) {
      queryExecutor.processNonQuery(processor.parseSQLToPhysicalPlan(sql));
    }
  }

  @After
  public void tearDown() throws Exception {
    config.setEnableCrossSpaceCompaction(enableCrossSpaceCompaction);
    config.setEnableSeqSpaceCompaction(enableSeqSpaceCompaction);
    config.setEnableUnseqSpaceCompaction(enableUnseqSpaceCompaction);
    EnvironmentUtils.cleanEnv();
  }

  @Test
  public void testQuery() throws Exception {
    QueryPlan queryPlan =
        (QueryPlan) processor.parseSQLToPhysicalPlan("select * from root.test.d0");
    QueryDataSet dataSet =
        queryExecutor.processQuery(queryPlan, EnvironmentUtils.TEST_QUERY_CONTEXT);
    assertTrue(dataSet.hasNext());
    assertEquals("1\t1", dataSet.next().toString());
    assertTrue(dataSet.hasNext());
    assertEquals("2\t2", dataSet.next().toString());
    assertTrue(dataSet.hasNext());
    assertEquals("3\t3", dataSet.next().toString());
    assertTrue(dataSet.hasNext());
    assertEquals("5\t5", dataSet.next().toString());
    assertTrue(dataSet.hasNext());
    assertEquals("10\t10", dataSet.next().toString());
    assertFalse(dataSet.hasNext());
  }

  @Test
  public void testOverlapped() throws Exception {
    queryExecutor.processNonQuery(
        processor.parseSQLToPhysicalPlan("insert into root.test.d0(timestamp,s0) values(2,-2)"));
    QueryPlan queryPlan =
        (QueryPlan) processor.parseSQLToPhysicalPlan("select * from root.test.d0 where s0 > 0");
    QueryDataSet dataSet =
        queryExecutor.processQuery(queryPlan, EnvironmentUtils.TEST_QUERY_CONTEXT);
    assertTrue(dataSet.hasNext());
    assertEquals("1\t1", dataSet.next().toString());
    assertTrue(dataSet.hasNext());
    assertEquals("3\t3", dataSet.next().toString());
    assertTrue(dataSet.hasNext());
    assertEquals("5\t5", dataSet.next().toString());
    assertTrue(dataSet.hasNext());
    assertEquals("10\t10", dataSet.next().toString());
    assertFalse(dataSet.hasNext());
  }

  @Test
  public void testOverlapped2() throws Exception {
    queryExecutor.processNonQuery(
        processor.parseSQLToPhysicalPlan("insert into root.test.d0(timestamp,s0) values(4,-4)"));
    queryExecutor.processNonQuery(
        processor.parseSQLToPhysicalPlan("insert into root.test.d0(timestamp,s0) values(6,-6)"));
    queryExecutor.processNonQuery(
        processor.parseSQLToPhysicalPlan("insert into root.test.d0(timestamp,s0) values(8,-8)"));
    queryExecutor.processNonQuery(
        processor.parseSQLToPhysicalPlan("insert into root.test.d0(timestamp,s0) values(7,7)"));
    QueryPlan queryPlan =
        (QueryPlan) processor.parseSQLToPhysicalPlan("select * from root.test.d0 where s0 < 0");
    QueryDataSet dataSet =
        queryExecutor.processQuery(queryPlan, EnvironmentUtils.TEST_QUERY_CONTEXT);
    assertTrue(dataSet.hasNext());
    assertEquals("4\t-4", dataSet.next().toString());
    assertTrue(dataSet.hasNext());
    assertEquals("6\t-6", dataSet.next().toString());
    assertTrue(dataSet.hasNext());
    assertEquals("8\t-8", dataSet.next().toString());
    assertFalse(dataSet.hasNext());
  }
}
