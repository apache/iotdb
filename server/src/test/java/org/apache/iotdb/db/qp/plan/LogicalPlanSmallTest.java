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
package org.apache.iotdb.db.qp.plan;

import org.antlr.v4.runtime.misc.ParseCancellationException;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.exception.query.LogicalOptimizeException;
import org.apache.iotdb.db.exception.query.QueryProcessException;
import org.apache.iotdb.db.exception.runtime.SQLParserException;
import org.apache.iotdb.db.qp.logical.RootOperator;
import org.apache.iotdb.db.qp.logical.crud.QueryOperator;
import org.apache.iotdb.db.qp.logical.crud.SFWOperator;
import org.apache.iotdb.db.qp.logical.sys.DeleteStorageGroupOperator;
import org.apache.iotdb.db.qp.physical.crud.InsertPlan;
import org.apache.iotdb.db.qp.strategy.ParseDriver;
import org.apache.iotdb.db.qp.strategy.optimizer.ConcatPathOptimizer;
import org.apache.iotdb.db.qp.utils.MemIntQpExecutor;
import org.apache.iotdb.tsfile.common.constant.TsFileConstant;
import org.apache.iotdb.tsfile.read.common.Path;
import org.apache.iotdb.tsfile.utils.StringContainer;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class LogicalPlanSmallTest {

  private ParseDriver parseDriver;

  @Before
  public void before() {
    parseDriver = new ParseDriver();
  }

  @Test
  public void testLimit() {
    String sqlStr = "select * from root.vehicle.d1 limit 10";
    RootOperator operator = (RootOperator) parseDriver
        .parse(sqlStr, IoTDBDescriptor.getInstance().getConfig().getZoneID());
    Assert.assertEquals(QueryOperator.class, operator.getClass());
    Assert.assertEquals(10, ((QueryOperator) operator).getRowLimit());
    Assert.assertEquals(0, ((QueryOperator) operator).getRowOffset());
    Assert.assertEquals(0, ((QueryOperator) operator).getSeriesLimit());
    Assert.assertEquals(0, ((QueryOperator) operator).getSeriesOffset());
  }

  @Test
  public void testOffset() {
    String sqlStr = "select * from root.vehicle.d1 limit 10 offset 20";
    RootOperator operator = (RootOperator) parseDriver
        .parse(sqlStr, IoTDBDescriptor.getInstance().getConfig().getZoneID());
    Assert.assertEquals(QueryOperator.class, operator.getClass());
    Assert.assertEquals(10, ((QueryOperator) operator).getRowLimit());
    Assert.assertEquals(20, ((QueryOperator) operator).getRowOffset());
    Assert.assertEquals(0, ((QueryOperator) operator).getSeriesLimit());
    Assert.assertEquals(0, ((QueryOperator) operator).getSeriesOffset());
  }

  @Test
  public void testSlimit() {
    String sqlStr = "select * from root.vehicle.d1 limit 10 slimit 1";
    RootOperator operator = (RootOperator) parseDriver
        .parse(sqlStr, IoTDBDescriptor.getInstance().getConfig().getZoneID());
    Assert.assertEquals(QueryOperator.class, operator.getClass());
    Assert.assertEquals(10, ((QueryOperator) operator).getRowLimit());
    Assert.assertEquals(0, ((QueryOperator) operator).getRowOffset());
    Assert.assertEquals(1, ((QueryOperator) operator).getSeriesLimit());
    Assert.assertEquals(0, ((QueryOperator) operator).getSeriesOffset());
  }

  @Test
  public void testSOffset() {
    String sqlStr = "select * from root.vehicle.d1 where s1 < 20 and time <= now() limit 50 slimit 10 soffset 100";
    RootOperator operator = (RootOperator) parseDriver
        .parse(sqlStr, IoTDBDescriptor.getInstance().getConfig().getZoneID());
    Assert.assertEquals(QueryOperator.class, operator.getClass());
    Assert.assertEquals(50, ((QueryOperator) operator).getRowLimit());
    Assert.assertEquals(0, ((QueryOperator) operator).getRowOffset());
    Assert.assertEquals(10, ((QueryOperator) operator).getSeriesLimit());
    Assert.assertEquals(100, ((QueryOperator) operator).getSeriesOffset());
  }

  @Test
  public void testSOffsetTimestamp() {
    String sqlStr = "select * from root.vehicle.d1 where s1 < 20 and timestamp <= now() limit 50 slimit 10 soffset 100";
    RootOperator operator = (RootOperator) parseDriver
        .parse(sqlStr, IoTDBDescriptor.getInstance().getConfig().getZoneID());
    Assert.assertEquals(QueryOperator.class, operator.getClass());
    Assert.assertEquals(50, ((QueryOperator) operator).getRowLimit());
    Assert.assertEquals(0, ((QueryOperator) operator).getRowOffset());
    Assert.assertEquals(10, ((QueryOperator) operator).getSeriesLimit());
    Assert.assertEquals(100, ((QueryOperator) operator).getSeriesOffset());
  }

  @Test(expected = SQLParserException.class)
  public void testLimitOutOfRange() {
    String sqlStr = "select * from root.vehicle.d1 where s1 < 20 and time <= now() limit 1111111111111111111111";
    RootOperator operator = (RootOperator) parseDriver
        .parse(sqlStr, IoTDBDescriptor.getInstance().getConfig().getZoneID());
    // expected to throw SQLParserException: Out of range. LIMIT <N>: N should be Int32.
  }

  @Test(expected = SQLParserException.class)
  public void testLimitNotPositive() {
    String sqlStr = "select * from root.vehicle.d1 where s1 < 20 and time <= now() limit 0";
    RootOperator operator = (RootOperator) parseDriver
        .parse(sqlStr, IoTDBDescriptor.getInstance().getConfig().getZoneID());
    // expected to throw SQLParserException: LIMIT <N>: N should be greater than 0.
  }

  @Test(expected = SQLParserException.class)
  public void testOffsetOutOfRange() {
    String sqlStr = "select * from root.vehicle.d1 where s1 < 20 and time <= now() "
        + "limit 1 offset 1111111111111111111111";
    RootOperator operator = (RootOperator) parseDriver
        .parse(sqlStr, IoTDBDescriptor.getInstance().getConfig().getZoneID());
    // expected to throw SQLParserException: Out of range. OFFSET <OFFSETValue>: OFFSETValue should be Int32.
  }

  @Test(expected = SQLParserException.class)
  public void testOffsetNotPositive() {
    String sqlStr = "select * from root.vehicle.d1 where s1 < 20 and time <= now() limit 1 offset 0";
    RootOperator operator = (RootOperator) parseDriver
        .parse(sqlStr, IoTDBDescriptor.getInstance().getConfig().getZoneID());
    // expected to throw SQLParserException: OFFSET <OFFSETValue>: OFFSETValue should be greater than 0.
  }

  @Test(expected = SQLParserException.class)
  public void testSlimitOutOfRange() {
    String sqlStr = "select * from root.vehicle.d1 where s1 < 20 and time <= now() slimit 1111111111111111111111";
    RootOperator operator = (RootOperator) parseDriver
        .parse(sqlStr, IoTDBDescriptor.getInstance().getConfig().getZoneID());
    // expected to throw SQLParserException: Out of range. SLIMIT <SN>: SN should be Int32.
  }

  @Test(expected = SQLParserException.class)
  public void testSlimitNotPositive() {
    String sqlStr = "select * from root.vehicle.d1 where s1 < 20 and time <= now() slimit 0";
    RootOperator operator = (RootOperator) parseDriver
        .parse(sqlStr, IoTDBDescriptor.getInstance().getConfig().getZoneID());
    // expected to throw SQLParserException: SLIMIT <SN>: SN should be greater than 0.
  }

  @Test(expected = SQLParserException.class)
  public void testSoffsetOutOfRange() {
    String sqlStr = "select * from root.vehicle.d1 where s1 < 20 and time <= now() "
        + "slimit 1 soffset 1111111111111111111111";
    RootOperator operator = (RootOperator) parseDriver
        .parse(sqlStr, IoTDBDescriptor.getInstance().getConfig().getZoneID());
    // expected to throw SQLParserException: Out of range. SOFFSET <SOFFSETValue>: SOFFSETValue should be Int32.
  }

  @Test(expected = SQLParserException.class)
  public void testSoffsetNotPositive() {
    String sqlStr = "select * from root.vehicle.d1 where s1 < 20 and time <= now() slimit 1 soffset 0";
    RootOperator operator = (RootOperator) parseDriver
        .parse(sqlStr, IoTDBDescriptor.getInstance().getConfig().getZoneID());
    // expected to throw SQLParserException: SOFFSET <SOFFSETValue>: SOFFSETValue should be greater than 0.
  }

  @Test(expected = LogicalOptimizeException.class)
  public void testSoffsetExceedColumnNum() throws QueryProcessException {
    String sqlStr = "select s1 from root.vehicle.d1 where s1 < 20 and time <= now() slimit 2 soffset 1";
    RootOperator operator = (RootOperator) parseDriver
        .parse(sqlStr, IoTDBDescriptor.getInstance().getConfig().getZoneID());

    MemIntQpExecutor executor = new MemIntQpExecutor();
    Path path1 = new Path(
        new StringContainer(new String[]{"root", "vehicle", "d1", "s1"},
            TsFileConstant.PATH_SEPARATOR));
    Path path2 = new Path(
        new StringContainer(new String[]{"root", "vehicle", "d2", "s1"},
            TsFileConstant.PATH_SEPARATOR));
    Path path3 = new Path(
        new StringContainer(new String[]{"root", "vehicle", "d3", "s1"},
            TsFileConstant.PATH_SEPARATOR));
    Path path4 = new Path(
        new StringContainer(new String[]{"root", "vehicle", "d4", "s1"},
            TsFileConstant.PATH_SEPARATOR));
    executor.insert(new InsertPlan(path1.getDevice(), 10, path1.getMeasurement(), "10"));
    executor.insert(new InsertPlan(path2.getDevice(), 10, path2.getMeasurement(), "10"));
    executor.insert(new InsertPlan(path3.getDevice(), 10, path3.getMeasurement(), "10"));
    executor.insert(new InsertPlan(path4.getDevice(), 10, path4.getMeasurement(), "10"));
    ConcatPathOptimizer concatPathOptimizer = new ConcatPathOptimizer(executor);
    operator = (SFWOperator) concatPathOptimizer.transform(operator);
    // expected to throw LogicalOptimizeException: SOFFSET <SOFFSETValue>: SOFFSETValue exceeds the range.
  }

  @Test
  public void testDeleteStorageGroup() {
    String sqlStr = "delete storage group root.vehicle.d1";
    RootOperator operator = (RootOperator) parseDriver
        .parse(sqlStr, IoTDBDescriptor.getInstance().getConfig().getZoneID());
    Assert.assertEquals(DeleteStorageGroupOperator.class, operator.getClass());
    Path path = new Path("root.vehicle.d1");
    Assert.assertEquals(path, ((DeleteStorageGroupOperator) operator).getDeletePathList().get(0));
  }

  @Test
  public void testDisableAlign() {
    String sqlStr = "select * from root.vehicle disable align";
    RootOperator operator = (RootOperator) parseDriver
            .parse(sqlStr, IoTDBDescriptor.getInstance().getConfig().getZoneID());
    Assert.assertEquals(QueryOperator.class, operator.getClass());
    Assert.assertFalse(((QueryOperator)operator).isAlign());
  }

  @Test
  public void testNotDisableAlign() {
    String sqlStr = "select * from root.vehicle";
    RootOperator operator = (RootOperator) parseDriver
            .parse(sqlStr, IoTDBDescriptor.getInstance().getConfig().getZoneID());
    Assert.assertEquals(QueryOperator.class, operator.getClass());
    Assert.assertTrue(((QueryOperator)operator).isAlign());
  }

  @Test (expected = ParseCancellationException.class)
  public void testDisableAlignConflictGroupByDevice() {
    String sqlStr = "select * from root.vehicle disable align group by device";
    RootOperator operator = (RootOperator) parseDriver
            .parse(sqlStr, IoTDBDescriptor.getInstance().getConfig().getZoneID());
  }
}
