/**
 * Copyright © 2019 Apache IoTDB(incubating) (dev@iotdb.apache.org)
 *
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
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.iotdb.db.qp.plan;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.iotdb.db.exception.ArgsErrorException;
import org.apache.iotdb.db.exception.FileNodeManagerException;
import org.apache.iotdb.db.exception.ProcessorException;
import org.apache.iotdb.db.exception.qp.QueryProcessorException;
import org.apache.iotdb.db.qp.QueryProcessor;
import org.apache.iotdb.db.qp.physical.PhysicalPlan;
import org.apache.iotdb.db.qp.utils.MemIntQpExecutor;
import org.apache.iotdb.tsfile.read.query.dataset.QueryDataSet;
import org.junit.After;
import org.junit.Test;

public class QPUpdateTest {

  private QueryProcessor processor;

  @After
  public void after() throws ProcessorException {
  }

  @Test
  public void test()
      throws QueryProcessorException, ArgsErrorException, ProcessorException, IOException {
    init();
    // testUpdate();
    // testUpdate2();
    // testDelete();
    // testInsert();
    // testDeletePaths();
  }

  private void init() throws QueryProcessorException, ArgsErrorException, ProcessorException {
    MemIntQpExecutor memProcessor = new MemIntQpExecutor();
    Map<String, List<String>> fakeAllPaths = new HashMap<String, List<String>>() {
      {
        put("root.laptop.d1.s1", new ArrayList<String>() {
          {
            add("root.laptop.d1.s1");
          }
        });
        put("root.laptop.d2.s1", new ArrayList<String>() {
          {
            add("root.laptop.d1.s1");
          }
        });
        put("root.laptop.d2.s2", new ArrayList<String>() {
          {
            add("root.laptop.d1.s2");
          }
        });
        put("root.laptop.*.s1", new ArrayList<String>() {
          {
            add("root.laptop.d1.s1");
            add("root.laptop.d2.s1");
          }
        });
      }
    };
    memProcessor.setFakeAllPaths(fakeAllPaths);
    processor = new QueryProcessor(memProcessor);
  }

  private void testUpdate2() throws ArgsErrorException, ProcessorException, IOException {
    PhysicalPlan plan = null;
    // String sql = "update root.qp_update_test.device_1.sensor_1 set value=100 where time>100 or (time<=50 and
    // time>10)";
    String sql = "UPDATE root.laptop SET d1.s1 = -33000, d2.s1 = 'string' WHERE time < 100";
    try {
      plan = processor.parseSQLToPhysicalPlan(sql);
    } catch (QueryProcessorException e) {
      assertEquals("UPDATE clause doesn't support multi-update yet.", e.getMessage());
    }
    sql = "UPDATE root.laptop SET d1.s1 = -33000 WHERE time < 100";
    try {
      plan = processor.parseSQLToPhysicalPlan(sql);
    } catch (QueryProcessorException e) {
      assertTrue(false);
    }
    assertEquals("UpdatePlan:  paths:  root.laptop.d1.s1\n" + "  value:-33000\n" + "  filter: \n"
            + "    199\n",
        plan.printQueryPlan());
    // System.out.println(plan.printQueryPlan());

  }

  private void testUpdate()
      throws QueryProcessorException, ArgsErrorException, ProcessorException, IOException,
      FileNodeManagerException {
    String sqlStr = "update root.qp_update_test.device_1.sensor_1 set value = 33000 where time >= 10 and time <= 10";
    PhysicalPlan plan1 = processor.parseSQLToPhysicalPlan(sqlStr);
    boolean upRet = processor.getExecutor().processNonQuery(plan1);

    assertTrue(upRet);
    // query to assert
    sqlStr = "select sensor_1,sensor_2 from root.qp_update_test.device_1";
    PhysicalPlan plan2 = processor.parseSQLToPhysicalPlan(sqlStr);
    QueryDataSet queryDataSet = processor.getExecutor().processQuery(plan2);
    String[] expect = {"10	33000	null", "20	null	10"};
    int i = 0;
    while (queryDataSet.hasNext()) {
      assertEquals(queryDataSet.next().toString(), expect[i++]);
    }
    assertEquals(expect.length, i);
  }

  private void testDeletePaths()
      throws QueryProcessorException, ProcessorException, ArgsErrorException, IOException,
      FileNodeManagerException {
    String sqlStr = "delete from root.qp_update_test.device_1 where time < 15";
    PhysicalPlan plan1 = processor.parseSQLToPhysicalPlan(sqlStr);
    boolean upRet = processor.getExecutor().processNonQuery(plan1);

    assertTrue(upRet);
    // query to assert
    sqlStr = "select sensor_1,sensor_2 from root.qp_update_test.device_1";
    PhysicalPlan plan2 = processor.parseSQLToPhysicalPlan(sqlStr);
    // RecordReaderFactory.getInstance().removeRecordReader("root.qp_update_test.device_1", "sensor_1");
    // RecordReaderFactory.getInstance().removeRecordReader("root.qp_update_test.device_1", "sensor_2");
    QueryDataSet queryDataSet = processor.getExecutor().processQuery(plan2);

    String[] expect = {"20	null	10"};
    int i = 0;
    while (queryDataSet.hasNext()) {
      assertEquals(queryDataSet.next().toString(), expect[i++]);
    }
    assertEquals(expect.length, i);
  }

  private void testDelete()
      throws QueryProcessorException, ProcessorException, ArgsErrorException, IOException,
      FileNodeManagerException {
    String sqlStr = "delete from root.qp_update_test.device_1.sensor_1 where time < 15";
    PhysicalPlan plan1 = processor.parseSQLToPhysicalPlan(sqlStr);
    boolean upRet = processor.getExecutor().processNonQuery(plan1);

    assertTrue(upRet);
    // query to assert
    sqlStr = "select sensor_1,sensor_2 from root.qp_update_test.device_1";
    PhysicalPlan plan2 = processor.parseSQLToPhysicalPlan(sqlStr);
    // RecordReaderFactory.getInstance().removeRecordReader("root.qp_update_test.device_1", "sensor_1");
    // RecordReaderFactory.getInstance().removeRecordReader("root.qp_update_test.device_1", "sensor_2");
    QueryDataSet queryDataSet = processor.getExecutor().processQuery(plan2);

    String[] expect = {"20	null	10"};
    int i = 0;
    while (queryDataSet.hasNext()) {
      assertEquals(queryDataSet.next().toString(), expect[i++]);
    }
    assertEquals(expect.length, i);
  }

  private void testInsert()
      throws QueryProcessorException, ProcessorException, ArgsErrorException, IOException,
      FileNodeManagerException {
    String sqlStr = "insert into root.qp_update_test.device_1 (timestamp, sensor_1, sensor_2) values (13, 50, 40)";
    PhysicalPlan plan1 = processor.parseSQLToPhysicalPlan(sqlStr);

    // executeWithGlobalTimeFilter insert
    boolean upRet = processor.getExecutor().processNonQuery(plan1);
    assertTrue(upRet);

    // RecordReaderFactory.getInstance().removeRecordReader("root.qp_update_test.device_1", "sensor_1");
    // RecordReaderFactory.getInstance().removeRecordReader("root.qp_update_test.device_1", "sensor_2");
    // query to assert
    sqlStr = "select sensor_1,sensor_2 from root.qp_update_test.device_1";
    PhysicalPlan plan2 = processor.parseSQLToPhysicalPlan(sqlStr);
    QueryDataSet queryDataSet = processor.getExecutor().processQuery(plan2);

    String[] expect = {"13	50	40", "20	null	10"};
    int i = 0;
    while (queryDataSet.hasNext()) {
      assertEquals(queryDataSet.next().toString(), expect[i++]);
    }
    assertEquals(expect.length, i);
  }
}
