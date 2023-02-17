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
package org.apache.iotdb.db.query.dataset;

// public class SingleDataSetTest {
//  private final IPlanExecutor queryExecutor = new PlanExecutor();
//  private final Planner processor = new Planner();
//
//  private final String[] sqls = {
//    "CREATE DATABASE root.vehicle",
//    "CREATE DATABASE root.test",
//    "CREATE TIMESERIES root.vehicle.d0.s0 WITH DATATYPE=INT32, ENCODING=RLE",
//    "CREATE TIMESERIES root.vehicle.d0.s1 WITH DATATYPE=TEXT, ENCODING=PLAIN",
//    "CREATE TIMESERIES root.test.d0.s0 WITH DATATYPE=INT32, ENCODING=RLE",
//    "CREATE TIMESERIES root.test.d0.s1 WITH DATATYPE=TEXT, ENCODING=PLAIN",
//    "CREATE TIMESERIES root.test.d1.`\"s3+xy\"` WITH DATATYPE=TEXT, ENCODING=PLAIN"
//  };
//
//  public SingleDataSetTest() throws QueryProcessException {}
//
//  @Before
//  public void setUp() throws Exception {
//    EnvironmentUtils.envSetUp();
//    for (String sql : sqls) {
//      queryExecutor.processNonQuery(processor.parseSQLToPhysicalPlan(sql));
//    }
//  }
//
//  @After
//  public void tearDown() throws Exception {
//    EnvironmentUtils.cleanEnv();
//  }
//
//  @Test
//  public void countDevice()
//      throws QueryProcessException, TException, StorageEngineException,
//          QueryFilterOptimizationException, MetadataException, IOException, InterruptedException,
//          SQLException {
//    PhysicalPlan plan = processor.parseSQLToPhysicalPlan("count devices");
//    QueryDataSet dataSet = queryExecutor.processQuery(plan, EnvironmentUtils.TEST_QUERY_CONTEXT);
//    Assert.assertTrue(dataSet instanceof SingleDataSet);
//    Assert.assertEquals("[devices]", dataSet.getPaths().toString());
//    while (dataSet.hasNext()) {
//      RowRecord record = dataSet.next();
//      Assert.assertEquals("0\t3", record.toString());
//    }
//  }
//
//  @Test
//  public void countTimeSeries()
//      throws QueryProcessException, TException, StorageEngineException,
//          QueryFilterOptimizationException, MetadataException, IOException, InterruptedException,
//          SQLException {
//    PhysicalPlan plan = processor.parseSQLToPhysicalPlan("count TimeSeries");
//    QueryDataSet dataSet = queryExecutor.processQuery(plan, EnvironmentUtils.TEST_QUERY_CONTEXT);
//    Assert.assertTrue(dataSet instanceof SingleDataSet);
//    Assert.assertEquals("[count]", dataSet.getPaths().toString());
//    while (dataSet.hasNext()) {
//      RowRecord record = dataSet.next();
//      Assert.assertEquals("0\t5", record.toString());
//    }
//  }
//
//  @Test
//  public void countStorageGroup()
//      throws TException, StorageEngineException, QueryFilterOptimizationException,
//          MetadataException, IOException, InterruptedException, SQLException,
//          QueryProcessException {
//    PhysicalPlan plan = processor.parseSQLToPhysicalPlan("count databases");
//    QueryDataSet dataSet = queryExecutor.processQuery(plan, EnvironmentUtils.TEST_QUERY_CONTEXT);
//    Assert.assertTrue(dataSet instanceof SingleDataSet);
//    Assert.assertEquals("[database]", dataSet.getPaths().toString());
//    while (dataSet.hasNext()) {
//      RowRecord record = dataSet.next();
//      Assert.assertEquals("0\t2", record.toString());
//    }
//  }
//
//  @Test
//  public void countNodes()
//      throws QueryProcessException, TException, StorageEngineException,
//          QueryFilterOptimizationException, MetadataException, IOException, InterruptedException,
//          SQLException {
//    PhysicalPlan plan = processor.parseSQLToPhysicalPlan("count nodes root.test.** level=2");
//    QueryDataSet dataSet = queryExecutor.processQuery(plan, EnvironmentUtils.TEST_QUERY_CONTEXT);
//    Assert.assertTrue(dataSet instanceof SingleDataSet);
//    Assert.assertEquals("[count]", dataSet.getPaths().toString());
//    while (dataSet.hasNext()) {
//      RowRecord record = dataSet.next();
//      Assert.assertEquals("0\t2", record.toString());
//    }
//  }
// }
