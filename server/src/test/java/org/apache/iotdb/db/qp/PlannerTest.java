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
package org.apache.iotdb.db.qp;

import org.apache.iotdb.db.exception.metadata.IllegalPathException;
import org.apache.iotdb.db.exception.query.QueryProcessException;
import org.apache.iotdb.db.exception.runtime.SQLParserException;
import org.apache.iotdb.db.metadata.MManager;
import org.apache.iotdb.db.metadata.PartialPath;
import org.apache.iotdb.db.qp.logical.Operator.OperatorType;
import org.apache.iotdb.db.qp.physical.PhysicalPlan;
import org.apache.iotdb.db.qp.physical.crud.InsertRowPlan;
import org.apache.iotdb.db.service.IoTDB;
import org.apache.iotdb.db.utils.EnvironmentUtils;
import org.apache.iotdb.service.rpc.thrift.TSLastDataQueryReq;
import org.apache.iotdb.service.rpc.thrift.TSRawDataQueryReq;
import org.apache.iotdb.tsfile.common.conf.TSFileDescriptor;
import org.apache.iotdb.tsfile.file.metadata.enums.CompressionType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;

import org.antlr.v4.runtime.misc.ParseCancellationException;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class PlannerTest {

  private CompressionType compressionType =
      TSFileDescriptor.getInstance().getConfig().getCompressor();
  private MManager mManager = IoTDB.metaManager;
  private Planner processor = new Planner();

  static {
    IoTDB.metaManager.init();
  }

  @Before
  public void setUp() throws Exception {
    EnvironmentUtils.envSetUp();
    mManager.setStorageGroup(new PartialPath("root.vehicle"));
    mManager.setStorageGroup(new PartialPath("root.vehicle1"));
    mManager.createTimeseries(
        new PartialPath("root.vehicle.device1.sensor1"),
        TSDataType.valueOf("INT32"),
        TSEncoding.valueOf("RLE"),
        compressionType,
        Collections.emptyMap());
    mManager.createTimeseries(
        new PartialPath("root.vehicle.device1.sensor2"),
        TSDataType.valueOf("INT32"),
        TSEncoding.valueOf("RLE"),
        compressionType,
        Collections.emptyMap());
    mManager.createTimeseries(
        new PartialPath("root.vehicle.device1.sensor3"),
        TSDataType.valueOf("INT32"),
        TSEncoding.valueOf("RLE"),
        compressionType,
        Collections.emptyMap());
    mManager.createTimeseries(
        new PartialPath("root.vehicle.device2.sensor1"),
        TSDataType.valueOf("INT32"),
        TSEncoding.valueOf("RLE"),
        compressionType,
        Collections.emptyMap());
    mManager.createTimeseries(
        new PartialPath("root.vehicle.device2.sensor2"),
        TSDataType.valueOf("INT32"),
        TSEncoding.valueOf("RLE"),
        compressionType,
        Collections.emptyMap());
    mManager.createTimeseries(
        new PartialPath("root.vehicle.device2.sensor3"),
        TSDataType.valueOf("INT32"),
        TSEncoding.valueOf("RLE"),
        compressionType,
        Collections.emptyMap());
    mManager.createTimeseries(
        new PartialPath("root.vehicle1.device1.sensor1"),
        TSDataType.valueOf("INT32"),
        TSEncoding.valueOf("RLE"),
        compressionType,
        Collections.emptyMap());
    mManager.createTimeseries(
        new PartialPath("root.vehicle1.device1.sensor2"),
        TSDataType.valueOf("INT32"),
        TSEncoding.valueOf("RLE"),
        compressionType,
        Collections.emptyMap());
    mManager.createTimeseries(
        new PartialPath("root.vehicle1.device1.sensor3"),
        TSDataType.valueOf("INT32"),
        TSEncoding.valueOf("RLE"),
        compressionType,
        Collections.emptyMap());
    mManager.createTimeseries(
        new PartialPath("root.vehicle1.device2.sensor1"),
        TSDataType.valueOf("INT32"),
        TSEncoding.valueOf("RLE"),
        compressionType,
        Collections.emptyMap());
    mManager.createTimeseries(
        new PartialPath("root.vehicle1.device2.sensor2"),
        TSDataType.valueOf("INT32"),
        TSEncoding.valueOf("RLE"),
        compressionType,
        Collections.emptyMap());
    mManager.createTimeseries(
        new PartialPath("root.vehicle1.device2.sensor3"),
        TSDataType.valueOf("INT32"),
        TSEncoding.valueOf("RLE"),
        compressionType,
        Collections.emptyMap());
  }

  @After
  public void tearDown() throws Exception {
    EnvironmentUtils.cleanEnv();
  }

  @Test
  public void parseSQLToPhysicalPlan() throws Exception {
    String createSGStatement = "set storage group to root.vehicle";
    PhysicalPlan plan1 = processor.parseSQLToPhysicalPlan(createSGStatement);
    assertEquals(OperatorType.SET_STORAGE_GROUP, plan1.getOperatorType());

    String createTSStatement1 =
        "create timeseries root.vehicle.d1.s1 with datatype=INT32,encoding=RLE";
    PhysicalPlan plan2 = processor.parseSQLToPhysicalPlan(createTSStatement1);
    assertEquals(OperatorType.CREATE_TIMESERIES, plan2.getOperatorType());

    String deleteTSStatement = "delete timeseries root.vehicle.d1.s1";
    PhysicalPlan plan3 = processor.parseSQLToPhysicalPlan(deleteTSStatement);
    assertEquals(OperatorType.DELETE_TIMESERIES, plan3.getOperatorType());

    String insertStatement = "insert into root.vehicle.d0(timestamp,s0) values(10,100)";
    PhysicalPlan plan4 = processor.parseSQLToPhysicalPlan(insertStatement);
    assertEquals(OperatorType.INSERT, plan4.getOperatorType());

    String deleteStatement =
        "DELETE FROM root.device0.sensor0,root.device0.sensor1 WHERE time <= 5000";
    PhysicalPlan plan6 = processor.parseSQLToPhysicalPlan(deleteStatement);
    assertEquals(OperatorType.DELETE, plan6.getOperatorType());

    String queryStatement1 =
        "select * from root.vehicle.** where root.vehicle.device1.sensor1 > 50";
    PhysicalPlan plan7 = processor.parseSQLToPhysicalPlan(queryStatement1);
    assertEquals(OperatorType.QUERY, plan7.getOperatorType());

    String aggregationStatement =
        "select sum(*) from root.vehicle where root.vehicle.device1.sensor1 > 50";
    PhysicalPlan plan8 = processor.parseSQLToPhysicalPlan(aggregationStatement);
    assertEquals(OperatorType.AGGREGATION, plan8.getOperatorType());

    String groupbyStatement =
        "select sum(*) from root.vehicle where root.vehicle.device1.sensor1 > 50 group by ([100,1100), 20ms)";
    PhysicalPlan plan9 = processor.parseSQLToPhysicalPlan(groupbyStatement);
    assertEquals(OperatorType.GROUP_BY_TIME, plan9.getOperatorType());

    String fillStatement =
        "select sensor1 from root.vehicle.device1 where time = 50 Fill(int32[linear, 5m, 5m], boolean[previous, 5m])";
    PhysicalPlan plan10 = processor.parseSQLToPhysicalPlan(fillStatement);
    assertEquals(OperatorType.FILL, plan10.getOperatorType());

    String insertTimeStatement = "insert into root.vehicle.d0(time,s0) values(10,100)";
    PhysicalPlan plan11 = processor.parseSQLToPhysicalPlan(insertTimeStatement);
    assertEquals(OperatorType.INSERT, plan11.getOperatorType());

    String createTSStatement2 =
        "create timeseries root.a.b.d_1.1s with datatype=FLOAT,encoding=RLE";
    PhysicalPlan plan12 = processor.parseSQLToPhysicalPlan(createTSStatement2);
    assertEquals(OperatorType.CREATE_TIMESERIES, plan12.getOperatorType());

    String queryStatement2 =
        "select windDirection10min from root.national.4.5.585.9_6666.9_333.88_9";
    PhysicalPlan plan13 = processor.parseSQLToPhysicalPlan(queryStatement2);
    assertEquals(OperatorType.QUERY, plan13.getOperatorType());

    String insertStatementException = "insert into root.vehicle.d0(timestamp,s0,s1) values(10,100)";
    try {
      processor.parseSQLToPhysicalPlan(insertStatementException);
    } catch (Exception e) {
      assertEquals(
          new SQLParserException(
                  "the measurementList's size 2 is not consistent with the valueList's size 1")
              .getMessage(),
          e.getMessage());
    }
  }

  @Test
  public void parseShowChildNodeToPhysicalPlan() throws Exception {
    String showChildNodesStatement = "show child nodes root.vehicle1.device1";
    PhysicalPlan plan14 = processor.parseSQLToPhysicalPlan(showChildNodesStatement);
    assertEquals(OperatorType.SHOW, plan14.getOperatorType());
  }

  @Test(expected = ParseCancellationException.class)
  public void parseErrorSQLToPhysicalPlan() throws QueryProcessException {
    String createTSStatement =
        "create timeseriess root.vehicle.d1.s1 with datatype=INT32,encoding=RLE";
    processor.parseSQLToPhysicalPlan(createTSStatement);
  }

  @Test
  public void insertStatementWithNullValue() throws QueryProcessException {
    String createTSStatement = "insert into root.vehicle.d0(time,s0) values(10,NaN)";
    PhysicalPlan physicalPlan = processor.parseSQLToPhysicalPlan(createTSStatement);

    assertTrue(physicalPlan instanceof InsertRowPlan);
    assertEquals("NaN", ((InsertRowPlan) physicalPlan).getValues()[0]);
    // Later we will use Double.parseDouble so we have to ensure that it is parsed right
    assertEquals(Double.NaN, Double.parseDouble("NaN"), 1e-15);
  }

  @Test
  public void rawDataQueryReqToPhysicalPlanTest()
      throws QueryProcessException, IllegalPathException {
    TSRawDataQueryReq tsRawDataQueryReq = new TSRawDataQueryReq();
    List<String> paths = new ArrayList<>();
    paths.add("root.vehicle.device1.sensor1");
    paths.add("root.vehicle.device1.sensor2");
    tsRawDataQueryReq.setPaths(paths);
    tsRawDataQueryReq.setStartTime(0);
    tsRawDataQueryReq.setEndTime(100);
    tsRawDataQueryReq.setFetchSize(1000);
    PhysicalPlan physicalPlan =
        processor.rawDataQueryReqToPhysicalPlan(tsRawDataQueryReq, ZoneId.of("Asia/Shanghai"));
    assertEquals(OperatorType.QUERY, physicalPlan.getOperatorType());
    assertEquals(paths.get(0), physicalPlan.getPaths().get(0).getFullPath());
    assertEquals(paths.get(1), physicalPlan.getPaths().get(1).getFullPath());
  }

  @Test
  public void lastDataQueryReqToPhysicalPlanTest()
      throws QueryProcessException, IllegalPathException {
    TSLastDataQueryReq tsLastDataQueryReq = new TSLastDataQueryReq();
    List<String> paths = new ArrayList<>();
    paths.add("root.vehicle.device1.sensor1");
    tsLastDataQueryReq.setPaths(paths);
    tsLastDataQueryReq.setTime(0);
    tsLastDataQueryReq.setFetchSize(1000);
    PhysicalPlan physicalPlan =
        processor.lastDataQueryReqToPhysicalPlan(tsLastDataQueryReq, ZoneId.of("Asia/Shanghai"));
    assertEquals(OperatorType.LAST, physicalPlan.getOperatorType());
    assertEquals(paths.get(0), physicalPlan.getPaths().get(0).getFullPath());
  }
}
