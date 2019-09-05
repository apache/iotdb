/**
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

package org.apache.iotdb.db.qp.plan.logicalPlan;

import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.exception.qp.LogicalOperatorException;
import org.apache.iotdb.db.exception.qp.LogicalOptimizeException;
import org.apache.iotdb.db.qp.constant.DatetimeUtils;
import org.apache.iotdb.db.qp.constant.SQLConstant;
import org.apache.iotdb.db.qp.logical.ExecutableOperator;
import org.apache.iotdb.db.qp.logical.crud.*;
import org.apache.iotdb.db.qp.physical.crud.InsertPlan;
import org.apache.iotdb.db.qp.strategy.LogicalGenerator;
import org.apache.iotdb.db.qp.strategy.optimizer.ConcatPathOptimizer;
import org.apache.iotdb.db.qp.utils.MemIntQpExecutor;
import org.apache.iotdb.db.query.fill.IFill;
import org.apache.iotdb.db.query.fill.PreviousFill;
import org.apache.iotdb.db.sql.parse.SqlParseException;
import org.apache.iotdb.tsfile.common.constant.TsFileConstant;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.common.Path;
import org.apache.iotdb.tsfile.utils.Pair;
import org.apache.iotdb.tsfile.utils.StringContainer;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.*;

public class TestQueryStatement {
  private LogicalGenerator generator;

  @Before
  public void before() {
    IoTDBConfig config = IoTDBDescriptor.getInstance().getConfig();
    generator = new LogicalGenerator(config.getZoneID());
  }

  @Test
  public void query1() {
    ExecutableOperator op = generator.getLogicalPlan("SELECT device_1.sensor_1,device_2.sensor_2 FROM root.vehicle WHERE "
            + "not(root.laptop.device_1.sensor_1 < 2000) and root.laptop.device_2.sensor_2 > 1000");
    assertEquals(SQLConstant.TOK_QUERY, op.getTokenIntType());
    SetPathOperator select = ((QueryOperator) op).getSetPathOperator();
    assertEquals(2, select.getSuffixPaths().size());
    assertEquals(new Path("device_1.sensor_1"), select.getSuffixPaths().get(0));
    assertEquals(new Path("device_2.sensor_2"), select.getSuffixPaths().get(1));
    FromOperator from = ((QueryOperator) op).getFromOperator();
    assertEquals(1, from.getPrefixPaths().size());
    assertEquals(new Path("root.vehicle"), from.getPrefixPaths().get(0));
    FilterOperator where = ((QueryOperator) op).getFilterOperator();
    assertEquals(SQLConstant.KW_AND, where.getTokenIntType());
    FilterOperator notOp = where.getChildren().get(0);
    BasicFunctionOperator op1 = (BasicFunctionOperator) notOp.getChildren().get(0);
    assertEquals(SQLConstant.LESSTHAN, op1.getTokenIntType());
    assertEquals(new Path("root.laptop.device_1.sensor_1"), op1.getSinglePath());
    assertEquals("2000", op1.getValue());
    BasicFunctionOperator op2 = (BasicFunctionOperator) where.getChildren().get(1);
    assertEquals(SQLConstant.GREATERTHAN, op2.getTokenIntType());
    assertEquals(new Path("root.laptop.device_2.sensor_2"), op2.getSinglePath());
    assertEquals("1000", op2.getValue());
  }

  @Test
  public void query2() {
    ExecutableOperator op = generator.getLogicalPlan("SELECT device_1.sensor_1,device_2.sensor_2 FROM root.vehicle "
            + "WHERE root.laptop.device_1.sensor_1 < -2.2E10;");
    assertEquals(SQLConstant.TOK_QUERY, op.getTokenIntType());
    SetPathOperator select = ((QueryOperator) op).getSetPathOperator();
    assertEquals(2, select.getSuffixPaths().size());
    assertEquals(new Path("device_1.sensor_1"), select.getSuffixPaths().get(0));
    assertEquals(new Path("device_2.sensor_2"), select.getSuffixPaths().get(1));
    FromOperator from = ((QueryOperator) op).getFromOperator();
    assertEquals(1, from.getPrefixPaths().size());
    assertEquals(new Path("root.vehicle"), from.getPrefixPaths().get(0));
    FilterOperator where = ((QueryOperator) op).getFilterOperator();
    assertEquals(SQLConstant.LESSTHAN, where.getTokenIntType());
    assertEquals(new Path("root.laptop.device_1.sensor_1"), where.getSinglePath());
    assertEquals("-2.2E10", ((BasicFunctionOperator) where).getValue());
  }

  @Test
  public void query3() {
    ExecutableOperator op = generator.getLogicalPlan("SELECT device_1.sensor_1,device_2.sensor_2 FROM root.vehicle "
            + "WHERE root.laptop.device_1.sensor_1 < +2.2E10");
    assertEquals(SQLConstant.TOK_QUERY, op.getTokenIntType());
    SetPathOperator select = ((QueryOperator) op).getSetPathOperator();
    assertEquals(2, select.getSuffixPaths().size());
    assertEquals(new Path("device_1.sensor_1"), select.getSuffixPaths().get(0));
    assertEquals(new Path("device_2.sensor_2"), select.getSuffixPaths().get(1));
    FromOperator from = ((QueryOperator) op).getFromOperator();
    assertEquals(1, from.getPrefixPaths().size());
    assertEquals(new Path("root.vehicle"), from.getPrefixPaths().get(0));
    FilterOperator where = ((QueryOperator) op).getFilterOperator();
    assertEquals(SQLConstant.LESSTHAN, where.getTokenIntType());
    assertEquals(new Path("root.laptop.device_1.sensor_1"), where.getSinglePath());
    assertEquals("+2.2E10", ((BasicFunctionOperator) where).getValue());
  }

  @Test
  public void query4() {
    ExecutableOperator op = generator.getLogicalPlan("SELECT device_1.sensor_1,device_2.sensor_2 FROM root.vehicle "
            + "WHERE root.laptop.device_1.sensor_1 < 2.2E10");
    assertEquals(SQLConstant.TOK_QUERY, op.getTokenIntType());
    SetPathOperator select = ((QueryOperator) op).getSetPathOperator();
    assertEquals(2, select.getSuffixPaths().size());
    assertEquals(new Path("device_1.sensor_1"), select.getSuffixPaths().get(0));
    assertEquals(new Path("device_2.sensor_2"), select.getSuffixPaths().get(1));
    FromOperator from = ((QueryOperator) op).getFromOperator();
    assertEquals(1, from.getPrefixPaths().size());
    assertEquals(new Path("root.vehicle"), from.getPrefixPaths().get(0));
    FilterOperator where = ((QueryOperator) op).getFilterOperator();
    assertEquals(SQLConstant.LESSTHAN, where.getTokenIntType());
    assertEquals(new Path("root.laptop.device_1.sensor_1"), where.getSinglePath());
    assertEquals("2.2E10", ((BasicFunctionOperator) where).getValue());
  }

  @Test
  public void query5() {
    ExecutableOperator op = generator.getLogicalPlan("SELECT device_1.sensor_1,device_2.sensor_2 FROM root.vehicle "
            + "WHERE root.laptop.device_1.sensor_1 < -2.2");
    assertEquals(SQLConstant.TOK_QUERY, op.getTokenIntType());
    SetPathOperator select = ((QueryOperator) op).getSetPathOperator();
    assertEquals(2, select.getSuffixPaths().size());
    assertEquals(new Path("device_1.sensor_1"), select.getSuffixPaths().get(0));
    assertEquals(new Path("device_2.sensor_2"), select.getSuffixPaths().get(1));
    FromOperator from = ((QueryOperator) op).getFromOperator();
    assertEquals(1, from.getPrefixPaths().size());
    assertEquals(new Path("root.vehicle"), from.getPrefixPaths().get(0));
    FilterOperator where = ((QueryOperator) op).getFilterOperator();
    assertEquals(SQLConstant.LESSTHAN, where.getTokenIntType());
    assertEquals(new Path("root.laptop.device_1.sensor_1"), where.getSinglePath());
    assertEquals("-2.2", ((BasicFunctionOperator) where).getValue());
  }

  @Test
  public void query6() {
    ExecutableOperator op = generator.getLogicalPlan("SELECT device_1.sensor_1,device_2.sensor_2 FROM root.vehicle "
            + "WHERE root.laptop.device_1.sensor_1 < +2.2");
    assertEquals(SQLConstant.TOK_QUERY, op.getTokenIntType());
    SetPathOperator select = ((QueryOperator) op).getSetPathOperator();
    assertEquals(2, select.getSuffixPaths().size());
    assertEquals(new Path("device_1.sensor_1"), select.getSuffixPaths().get(0));
    assertEquals(new Path("device_2.sensor_2"), select.getSuffixPaths().get(1));
    FromOperator from = ((QueryOperator) op).getFromOperator();
    assertEquals(1, from.getPrefixPaths().size());
    assertEquals(new Path("root.vehicle"), from.getPrefixPaths().get(0));
    FilterOperator where = ((QueryOperator) op).getFilterOperator();
    assertEquals(SQLConstant.LESSTHAN, where.getTokenIntType());
    assertEquals(new Path("root.laptop.device_1.sensor_1"), where.getSinglePath());
    assertEquals("+2.2", ((BasicFunctionOperator) where).getValue());
  }

  @Test
  public void query7() {
    ExecutableOperator op = generator.getLogicalPlan("SELECT device_1.sensor_1,device_2.sensor_2 FROM root.vehicle "
            + "WHERE root.laptop.device_1.sensor_1 < 2.2");
    assertEquals(SQLConstant.TOK_QUERY, op.getTokenIntType());
    SetPathOperator select = ((QueryOperator) op).getSetPathOperator();
    assertEquals(2, select.getSuffixPaths().size());
    assertEquals(new Path("device_1.sensor_1"), select.getSuffixPaths().get(0));
    assertEquals(new Path("device_2.sensor_2"), select.getSuffixPaths().get(1));
    FromOperator from = ((QueryOperator) op).getFromOperator();
    assertEquals(1, from.getPrefixPaths().size());
    assertEquals(new Path("root.vehicle"), from.getPrefixPaths().get(0));
    FilterOperator where = ((QueryOperator) op).getFilterOperator();
    assertEquals(SQLConstant.LESSTHAN, where.getTokenIntType());
    assertEquals(new Path("root.laptop.device_1.sensor_1"), where.getSinglePath());
    assertEquals("2.2", ((BasicFunctionOperator) where).getValue());
  }

  @Test
  public void query8() {
    ExecutableOperator op = generator.getLogicalPlan("SELECT device_1.sensor_1,device_2.sensor_2 FROM root.vehicle "
            + "WHERE root.laptop.device_1.sensor_1 < .2e2");
    assertEquals(SQLConstant.TOK_QUERY, op.getTokenIntType());
    SetPathOperator select = ((QueryOperator) op).getSetPathOperator();
    assertEquals(2, select.getSuffixPaths().size());
    assertEquals(new Path("device_1.sensor_1"), select.getSuffixPaths().get(0));
    assertEquals(new Path("device_2.sensor_2"), select.getSuffixPaths().get(1));
    FromOperator from = ((QueryOperator) op).getFromOperator();
    assertEquals(1, from.getPrefixPaths().size());
    assertEquals(new Path("root.vehicle"), from.getPrefixPaths().get(0));
    FilterOperator where = ((QueryOperator) op).getFilterOperator();
    assertEquals(SQLConstant.LESSTHAN, where.getTokenIntType());
    assertEquals(new Path("root.laptop.device_1.sensor_1"), where.getSinglePath());
    assertEquals(".2e2", ((BasicFunctionOperator) where).getValue());
  }

  @Test
  public void query9() {
    ExecutableOperator op = generator.getLogicalPlan("SELECT device_1.sensor_1,device_2.sensor_2 FROM root.vehicle "
            + "WHERE root.laptop.device_1.sensor_1 < .2");
    assertEquals(SQLConstant.TOK_QUERY, op.getTokenIntType());
    SetPathOperator select = ((QueryOperator) op).getSetPathOperator();
    assertEquals(2, select.getSuffixPaths().size());
    assertEquals(new Path("device_1.sensor_1"), select.getSuffixPaths().get(0));
    assertEquals(new Path("device_2.sensor_2"), select.getSuffixPaths().get(1));
    FromOperator from = ((QueryOperator) op).getFromOperator();
    assertEquals(1, from.getPrefixPaths().size());
    assertEquals(new Path("root.vehicle"), from.getPrefixPaths().get(0));
    FilterOperator where = ((QueryOperator) op).getFilterOperator();
    assertEquals(SQLConstant.LESSTHAN, where.getTokenIntType());
    assertEquals(new Path("root.laptop.device_1.sensor_1"), where.getSinglePath());
    assertEquals(".2", ((BasicFunctionOperator) where).getValue());
  }

  @Test
  public void query10() {
    ExecutableOperator op = generator.getLogicalPlan("SELECT device_1.sensor_1,device_2.sensor_2 FROM root.vehicle "
            + "WHERE root.laptop.device_1.sensor_1 < 2.");
    assertEquals(SQLConstant.TOK_QUERY, op.getTokenIntType());
    SetPathOperator select = ((QueryOperator) op).getSetPathOperator();
    assertEquals(2, select.getSuffixPaths().size());
    assertEquals(new Path("device_1.sensor_1"), select.getSuffixPaths().get(0));
    assertEquals(new Path("device_2.sensor_2"), select.getSuffixPaths().get(1));
    FromOperator from = ((QueryOperator) op).getFromOperator();
    assertEquals(1, from.getPrefixPaths().size());
    assertEquals(new Path("root.vehicle"), from.getPrefixPaths().get(0));
    FilterOperator where = ((QueryOperator) op).getFilterOperator();
    assertEquals(SQLConstant.LESSTHAN, where.getTokenIntType());
    assertEquals(new Path("root.laptop.device_1.sensor_1"), where.getSinglePath());
    assertEquals("2", ((BasicFunctionOperator) where).getValue());
  }

  @Test
  public void query11() {
    ExecutableOperator op = generator.getLogicalPlan("SELECT device_1.sensor_1,device_2.sensor_2 FROM root.vehicle "
            + "WHERE root.laptop.device_1.sensor_1 < +2.");
    assertEquals(SQLConstant.TOK_QUERY, op.getTokenIntType());
    SetPathOperator select = ((QueryOperator) op).getSetPathOperator();
    assertEquals(2, select.getSuffixPaths().size());
    assertEquals(new Path("device_1.sensor_1"), select.getSuffixPaths().get(0));
    assertEquals(new Path("device_2.sensor_2"), select.getSuffixPaths().get(1));
    FromOperator from = ((QueryOperator) op).getFromOperator();
    assertEquals(1, from.getPrefixPaths().size());
    assertEquals(new Path("root.vehicle"), from.getPrefixPaths().get(0));
    FilterOperator where = ((QueryOperator) op).getFilterOperator();
    assertEquals(SQLConstant.LESSTHAN, where.getTokenIntType());
    assertEquals(new Path("root.laptop.device_1.sensor_1"), where.getSinglePath());
    assertEquals("+2", ((BasicFunctionOperator) where).getValue());
  }


  @Test
  public void query12() {
    ExecutableOperator op = generator.getLogicalPlan("SELECT device_1.sensor_1,device_2.sensor_2 FROM root.vehicle "
            + "WHERE root.laptop.device_1.sensor_1 < -2.");
    assertEquals(SQLConstant.TOK_QUERY, op.getTokenIntType());
    SetPathOperator select = ((QueryOperator) op).getSetPathOperator();
    assertEquals(2, select.getSuffixPaths().size());
    assertEquals(new Path("device_1.sensor_1"), select.getSuffixPaths().get(0));
    assertEquals(new Path("device_2.sensor_2"), select.getSuffixPaths().get(1));
    FromOperator from = ((QueryOperator) op).getFromOperator();
    assertEquals(1, from.getPrefixPaths().size());
    assertEquals(new Path("root.vehicle"), from.getPrefixPaths().get(0));
    FilterOperator where = ((QueryOperator) op).getFilterOperator();
    assertEquals(SQLConstant.LESSTHAN, where.getTokenIntType());
    assertEquals(new Path("root.laptop.device_1.sensor_1"), where.getSinglePath());
    assertEquals("-2", ((BasicFunctionOperator) where).getValue());
  }

  @Test
  public void query13() {
    ExecutableOperator op = generator.getLogicalPlan("SELECT device_1.sensor_1,device_2.sensor_2 FROM root.vehicle "
            + "WHERE root.laptop.device_1.sensor_1 < -.2");
    assertEquals(SQLConstant.TOK_QUERY, op.getTokenIntType());
    SetPathOperator select = ((QueryOperator) op).getSetPathOperator();
    assertEquals(2, select.getSuffixPaths().size());
    assertEquals(new Path("device_1.sensor_1"), select.getSuffixPaths().get(0));
    assertEquals(new Path("device_2.sensor_2"), select.getSuffixPaths().get(1));
    FromOperator from = ((QueryOperator) op).getFromOperator();
    assertEquals(1, from.getPrefixPaths().size());
    assertEquals(new Path("root.vehicle"), from.getPrefixPaths().get(0));
    FilterOperator where = ((QueryOperator) op).getFilterOperator();
    assertEquals(SQLConstant.LESSTHAN, where.getTokenIntType());
    assertEquals(new Path("root.laptop.device_1.sensor_1"), where.getSinglePath());
    assertEquals("-.2", ((BasicFunctionOperator) where).getValue());
  }

  @Test
  public void query14() {
    ExecutableOperator op = generator.getLogicalPlan("SELECT device_1.sensor_1,device_2.sensor_2 FROM root.vehicle "
            + "WHERE root.laptop.device_1.sensor_1 < -.2e10");
    assertEquals(SQLConstant.TOK_QUERY, op.getTokenIntType());
    SetPathOperator select = ((QueryOperator) op).getSetPathOperator();
    assertEquals(2, select.getSuffixPaths().size());
    assertEquals(new Path("device_1.sensor_1"), select.getSuffixPaths().get(0));
    assertEquals(new Path("device_2.sensor_2"), select.getSuffixPaths().get(1));
    FromOperator from = ((QueryOperator) op).getFromOperator();
    assertEquals(1, from.getPrefixPaths().size());
    assertEquals(new Path("root.vehicle"), from.getPrefixPaths().get(0));
    FilterOperator where = ((QueryOperator) op).getFilterOperator();
    assertEquals(SQLConstant.LESSTHAN, where.getTokenIntType());
    assertEquals(new Path("root.laptop.device_1.sensor_1"), where.getSinglePath());
    assertEquals("-.2e10", ((BasicFunctionOperator) where).getValue());
  }

  @Test
  public void query15() {
    ExecutableOperator op = generator.getLogicalPlan("SELECT device_1.sensor_1,device_2.sensor_2 FROM root.vehicle "
            + "WHERE root.laptop.device_1.sensor_1 < -2.2E10 && time > now();");
    assertEquals(SQLConstant.TOK_QUERY, op.getTokenIntType());
    SetPathOperator select = ((QueryOperator) op).getSetPathOperator();
    assertEquals(2, select.getSuffixPaths().size());
    assertEquals(new Path("device_1.sensor_1"), select.getSuffixPaths().get(0));
    assertEquals(new Path("device_2.sensor_2"), select.getSuffixPaths().get(1));
    FromOperator from = ((QueryOperator) op).getFromOperator();
    assertEquals(1, from.getPrefixPaths().size());
    assertEquals(new Path("root.vehicle"), from.getPrefixPaths().get(0));
    FilterOperator where = ((QueryOperator) op).getFilterOperator();
    assertEquals(SQLConstant.KW_AND, where.getTokenIntType());
    assertEquals(2, where.getChildren().size());
    BasicFunctionOperator exp1 = (BasicFunctionOperator) where.getChildren().get(0);
    BasicFunctionOperator exp2 = (BasicFunctionOperator) where.getChildren().get(1);
    assertEquals(SQLConstant.LESSTHAN, exp1.getTokenIntType());
    assertEquals(new Path("root.laptop.device_1.sensor_1"), exp1.getSinglePath());
    assertEquals("-2.2E10", exp1.getValue());
    assertEquals(SQLConstant.GREATERTHAN, exp2.getTokenIntType());
    assertEquals(new Path("time"), exp2.getSinglePath());
    assertTrue(System.currentTimeMillis() - Long.parseLong(exp2.getValue()) < 10);
  }

  @Test
  public void query16() throws LogicalOperatorException {
    ExecutableOperator op = generator.getLogicalPlan("SELECT device_1.sensor_1,device_2.sensor_2 FROM root.vehicle "
            + "WHERE time < 1234567 & time > 2017-6-2T12:00:12+07:00");
    assertEquals(SQLConstant.TOK_QUERY, op.getTokenIntType());
    SetPathOperator select = ((QueryOperator) op).getSetPathOperator();
    assertEquals(2, select.getSuffixPaths().size());
    assertEquals(new Path("device_1.sensor_1"), select.getSuffixPaths().get(0));
    assertEquals(new Path("device_2.sensor_2"), select.getSuffixPaths().get(1));
    FromOperator from = ((QueryOperator) op).getFromOperator();
    assertEquals(1, from.getPrefixPaths().size());
    assertEquals(new Path("root.vehicle"), from.getPrefixPaths().get(0));
    FilterOperator where = ((QueryOperator) op).getFilterOperator();
    assertEquals(SQLConstant.KW_AND, where.getTokenIntType());
    assertEquals(2, where.getChildren().size());
    BasicFunctionOperator exp1 = (BasicFunctionOperator) where.getChildren().get(0);
    BasicFunctionOperator exp2 = (BasicFunctionOperator) where.getChildren().get(1);
    assertEquals(SQLConstant.LESSTHAN, exp1.getTokenIntType());
    assertEquals(new Path("time"), exp1.getSinglePath());
    assertEquals("1234567", exp1.getValue());
    assertEquals(SQLConstant.GREATERTHAN, exp2.getTokenIntType());
    assertEquals(new Path("time"), exp2.getSinglePath());
    assertEquals(parseTimeFormat("2017-6-2T12:00:12+07:00") + "", exp2.getValue());
  }

  @Test
  public void query17() throws LogicalOperatorException {
    ExecutableOperator op = generator.getLogicalPlan("SELECT * FROM root.vehicle WHERE time < 1234567 || time > 2017.6.2 12:00:12+07:00;");
    assertEquals(SQLConstant.TOK_QUERY, op.getTokenIntType());
    SetPathOperator select = ((QueryOperator) op).getSetPathOperator();
    assertEquals(1, select.getSuffixPaths().size());
    assertEquals(new Path("*"), select.getSuffixPaths().get(0));
    FromOperator from = ((QueryOperator) op).getFromOperator();
    assertEquals(1, from.getPrefixPaths().size());
    assertEquals(new Path("root.vehicle"), from.getPrefixPaths().get(0));
    FilterOperator where = ((QueryOperator) op).getFilterOperator();
    assertEquals(SQLConstant.KW_OR, where.getTokenIntType());
    assertEquals(2, where.getChildren().size());
    BasicFunctionOperator exp1 = (BasicFunctionOperator) where.getChildren().get(0);
    BasicFunctionOperator exp2 = (BasicFunctionOperator) where.getChildren().get(1);
    assertEquals(SQLConstant.LESSTHAN, exp1.getTokenIntType());
    assertEquals(new Path("time"), exp1.getSinglePath());
    assertEquals("1234567", exp1.getValue());
    assertEquals(SQLConstant.GREATERTHAN, exp2.getTokenIntType());
    assertEquals(new Path("time"), exp2.getSinglePath());
    assertEquals(parseTimeFormat("2017.6.2 12:00:12+07:00") + "", exp2.getValue());
  }

  @Test
  public void query18() {
    ExecutableOperator op = generator.getLogicalPlan("SELECT 456.*.890 FROM root.vehicle.123.abc WHERE root.333.222 < 11");
    assertEquals(SQLConstant.TOK_QUERY, op.getTokenIntType());
    SetPathOperator select = ((QueryOperator) op).getSetPathOperator();
    assertEquals(1, select.getSuffixPaths().size());
    assertEquals(new Path("456.*.890"), select.getSuffixPaths().get(0));
    FromOperator from = ((QueryOperator) op).getFromOperator();
    assertEquals(1, from.getPrefixPaths().size());
    assertEquals(new Path("root.vehicle.123.abc"), from.getPrefixPaths().get(0));
    FilterOperator where = ((QueryOperator) op).getFilterOperator();
    assertEquals(SQLConstant.LESSTHAN, where.getTokenIntType());
    assertEquals(new Path("root.333.222"), ((BasicFunctionOperator) where).getSinglePath());
    assertEquals("11", ((BasicFunctionOperator) where).getValue());
  }

  @Test
  public void query19() {
    ExecutableOperator op = generator.getLogicalPlan("SELECT 000 FROM root.vehicle.123.abc WHERE 000 < 11");
    assertEquals(SQLConstant.TOK_QUERY, op.getTokenIntType());
    SetPathOperator select = ((QueryOperator) op).getSetPathOperator();
    assertEquals(1, select.getSuffixPaths().size());
    assertEquals(new Path("000"), select.getSuffixPaths().get(0));
    FromOperator from = ((QueryOperator) op).getFromOperator();
    assertEquals(1, from.getPrefixPaths().size());
    assertEquals(new Path("root.vehicle.123.abc"), from.getPrefixPaths().get(0));
    FilterOperator where = ((QueryOperator) op).getFilterOperator();
    assertEquals(SQLConstant.LESSTHAN, where.getTokenIntType());
    assertEquals(new Path("000"), ((BasicFunctionOperator) where).getSinglePath());
    assertEquals("11", ((BasicFunctionOperator) where).getValue());
  }

  @Test
  public void aggregation1() {
    ExecutableOperator op = generator.getLogicalPlan("select count(s1),max_time(s2) from root.vehicle.d1 where root.vehicle.d1.s1 < 0.32e6 and time <= now()");
    assertEquals(SQLConstant.TOK_QUERY, op.getTokenIntType());
    SetPathOperator select = ((QueryOperator) op).getSetPathOperator();
    AggregationOperator aggregationOperator = ((QueryOperator)op).getAggregationOperator();
    assertEquals(2, select.getSuffixPaths().size());
    assertEquals(new Path("s1"), select.getSuffixPaths().get(0));
    assertEquals("count", aggregationOperator.getAggregations().get(0));
    assertEquals(new Path("s2"), select.getSuffixPaths().get(1));
    assertEquals("max_time", aggregationOperator.getAggregations().get(1));
    FromOperator from = ((QueryOperator) op).getFromOperator();
    assertEquals(1, from.getPrefixPaths().size());
    assertEquals(new Path("root.vehicle.d1"), from.getPrefixPaths().get(0));
    FilterOperator where = ((QueryOperator) op).getFilterOperator();
    assertEquals(SQLConstant.KW_AND, where.getTokenIntType());
    assertEquals(2, where.getChildren().size());
    BasicFunctionOperator exp1 = (BasicFunctionOperator) where.getChildren().get(0);
    BasicFunctionOperator exp2 = (BasicFunctionOperator) where.getChildren().get(1);
    assertEquals(new Path("root.vehicle.d1.s1"), exp1.getSinglePath());
    assertEquals(SQLConstant.LESSTHAN, exp1.getTokenIntType());
    assertEquals("0.32e6", exp1.getValue());
    assertEquals(new Path("time"), exp2.getSinglePath());
    assertEquals(SQLConstant.LESSTHANOREQUALTO, exp2.getTokenIntType());
    assertTrue(System.currentTimeMillis() - Long.parseLong(exp2.getValue()) < 10);
  }

  @Test
  public void aggregation2() {
    ExecutableOperator op = generator.getLogicalPlan("select sum(s2) FROM root.vehicle.d1 WHERE s1 < 2000 or time >= 1234567;");
    assertEquals(SQLConstant.TOK_QUERY, op.getTokenIntType());
    SetPathOperator select = ((QueryOperator) op).getSetPathOperator();
    AggregationOperator aggregationOperator = ((QueryOperator)op).getAggregationOperator();
    assertEquals(1, select.getSuffixPaths().size());
    assertEquals(new Path("s2"), select.getSuffixPaths().get(0));
    assertEquals(1, aggregationOperator.getAggregations().size());
    assertEquals("sum", aggregationOperator.getAggregations().get(0));
    FromOperator from = ((QueryOperator) op).getFromOperator();
    assertEquals(1, from.getPrefixPaths().size());
    assertEquals(new Path("root.vehicle.d1"), from.getPrefixPaths().get(0));
    FilterOperator where = ((QueryOperator) op).getFilterOperator();
    assertEquals(SQLConstant.KW_OR, where.getTokenIntType());
    assertEquals(2, where.getChildren().size());
    BasicFunctionOperator exp1 = (BasicFunctionOperator) where.getChildren().get(0);
    BasicFunctionOperator exp2 = (BasicFunctionOperator) where.getChildren().get(1);
    assertEquals(new Path("s1"), exp1.getSinglePath());
    assertEquals(SQLConstant.LESSTHAN, exp1.getTokenIntType());
    assertEquals("2000", exp1.getValue());
    assertEquals(new Path("time"), exp2.getSinglePath());
    assertEquals(SQLConstant.GREATERTHANOREQUALTO, exp2.getTokenIntType());
    assertEquals("1234567", exp2.getValue());
  }

  @Test
  public void aggregation3() {
    // TODO how to decide it is s1, sum(s2), or sum(s1), s2
    ExecutableOperator op = generator.getLogicalPlan("select s1,sum(s2) FROM root.vehicle.d1 WHERE root.vehicle.d1.s1 < 2000 | time >= 1234567");
    assertEquals(SQLConstant.TOK_QUERY, op.getTokenIntType());
    SetPathOperator select = ((QueryOperator) op).getSetPathOperator();
    AggregationOperator aggregationOperator = ((QueryOperator)op).getAggregationOperator();
    assertEquals(2, select.getSuffixPaths().size());
    assertEquals(new Path("s1"), select.getSuffixPaths().get(0));
    assertEquals(new Path("s2"), select.getSuffixPaths().get(1));
    assertEquals(1, aggregationOperator.getAggregations().size());
    assertEquals("sum", aggregationOperator.getAggregations().get(0));
    FromOperator from = ((QueryOperator) op).getFromOperator();
    assertEquals(1, from.getPrefixPaths().size());
    assertEquals(new Path("root.vehicle.d1"), from.getPrefixPaths().get(0));
    FilterOperator where = ((QueryOperator) op).getFilterOperator();
    assertEquals(SQLConstant.KW_OR, where.getTokenIntType());
    assertEquals(2, where.getChildren().size());
    BasicFunctionOperator exp1 = (BasicFunctionOperator) where.getChildren().get(0);
    BasicFunctionOperator exp2 = (BasicFunctionOperator) where.getChildren().get(1);
    assertEquals(new Path("root.vehicle.d1.s1"), exp1.getSinglePath());
    assertEquals(SQLConstant.LESSTHAN, exp1.getTokenIntType());
    assertEquals("2000", exp1.getValue());
    assertEquals(new Path("time"), exp2.getSinglePath());
    assertEquals(SQLConstant.GREATERTHANOREQUALTO, exp2.getTokenIntType());
    assertEquals("1234567", exp2.getValue());
    assertFalse(((QueryOperator) op).isGroupBy());
  }

  @Test
  public void groupby1() {
    ExecutableOperator op = generator.getLogicalPlan("select count(s1),max_time(s2) " + "from root.vehicle.d1 "
            + "where root.vehicle.d1.s1 < 0.32e6 and time <= now() "
            + "group by(10w, 44, [1,3], [4,5])");
    assertEquals(SQLConstant.TOK_QUERY, op.getTokenIntType());
    SetPathOperator select = ((QueryOperator) op).getSetPathOperator();
    AggregationOperator aggregationOperator = ((QueryOperator)op).getAggregationOperator();
    assertEquals(2, select.getSuffixPaths().size());
    assertEquals(new Path("s1"), select.getSuffixPaths().get(0));
    assertEquals(new Path("s2"), select.getSuffixPaths().get(1));
    assertEquals(2, aggregationOperator.getAggregations().size());
    assertEquals("count", aggregationOperator.getAggregations().get(0));
    assertEquals("max_time", aggregationOperator.getAggregations().get(1));
    FromOperator from = ((QueryOperator) op).getFromOperator();
    assertEquals(1, from.getPrefixPaths().size());
    assertEquals(new Path("root.vehicle.d1"), from.getPrefixPaths().get(0));
    FilterOperator where = ((QueryOperator) op).getFilterOperator();
    assertEquals(SQLConstant.KW_AND, where.getTokenIntType());
    assertEquals(2, where.getChildren().size());
    BasicFunctionOperator exp1 = (BasicFunctionOperator) where.getChildren().get(0);
    BasicFunctionOperator exp2 = (BasicFunctionOperator) where.getChildren().get(1);
    assertEquals(new Path("root.vehicle.d1.s1"), exp1.getSinglePath());
    assertEquals(SQLConstant.LESSTHAN, exp1.getTokenIntType());
    assertEquals("0.32e6", exp1.getValue());
    assertEquals(new Path("time"), exp2.getSinglePath());
    assertTrue(System.currentTimeMillis() - Long.parseLong(exp2.getValue()) < 10);
    assertTrue(((QueryOperator) op).isGroupBy());
    assertEquals(parseTimeUnit("10", "w"), ((QueryOperator) op).getUnit());
    List<Pair<Long, Long>> intervals = ((QueryOperator) op).getIntervals();
    assertEquals(2, intervals.size());
    assertEquals(1L, (long) intervals.get(0).left);
    assertEquals(3L, (long) intervals.get(0).right);
    assertEquals(4L, (long) intervals.get(1).left);
    assertEquals(5L, (long) intervals.get(1).right);
    assertEquals(44, ((QueryOperator) op).getOrigin());
  }

  @Test
  public void groupby2() throws LogicalOperatorException {
    ExecutableOperator op = generator.getLogicalPlan("select sum(s2) "
            + "FROM root.vehicle.d1 "
            + "WHERE s1 < 2000 or time >= 1234567 "
            + "group by(111ms, [123,2017-6-2T12:00:12+07:00], [55555, now()]);");
    assertEquals(SQLConstant.TOK_QUERY, op.getTokenIntType());
    SetPathOperator select = ((QueryOperator) op).getSetPathOperator();
    AggregationOperator aggregationOperator = ((QueryOperator)op).getAggregationOperator();
    assertEquals(1, select.getSuffixPaths().size());
    assertEquals(new Path("s2"), select.getSuffixPaths().get(0));
    assertEquals(1, aggregationOperator.getAggregations().size());
    assertEquals("sum", aggregationOperator.getAggregations().get(0));
    FromOperator from = ((QueryOperator) op).getFromOperator();
    assertEquals(1, from.getPrefixPaths().size());
    assertEquals(new Path("root.vehicle.d1"), from.getPrefixPaths().get(0));
    FilterOperator where = ((QueryOperator) op).getFilterOperator();
    assertEquals(SQLConstant.KW_OR, where.getTokenIntType());
    assertEquals(2, where.getChildren().size());
    BasicFunctionOperator exp1 = (BasicFunctionOperator) where.getChildren().get(0);
    BasicFunctionOperator exp2 = (BasicFunctionOperator) where.getChildren().get(1);
    assertEquals(new Path("s1"), exp1.getSinglePath());
    assertEquals(SQLConstant.LESSTHAN, exp1.getTokenIntType());
    assertEquals("2000", exp1.getValue());
    assertEquals(new Path("time"), exp2.getSinglePath());
    assertEquals(SQLConstant.GREATERTHANOREQUALTO, exp2.getTokenIntType());
    assertEquals("1234567", exp2.getValue());
    assertTrue(((QueryOperator) op).isGroupBy());
    assertEquals(parseTimeUnit("111", "ms"), ((QueryOperator) op).getUnit());
    List<Pair<Long, Long>> intervals = ((QueryOperator) op).getIntervals();
    assertEquals(2, intervals.size());
    assertEquals(123L, (long) intervals.get(0).left);
    assertEquals(parseTimeFormat("2017-6-2T12:00:12+07:00"), (long) intervals.get(0).right);
    assertEquals(55555L, (long) intervals.get(1).left);
    assertTrue(System.currentTimeMillis() - Long.parseLong(String.valueOf(intervals.get(1).right)) < 10);
    assertEquals(parseTimeFormat("1970-1-01T00:00:00"), ((QueryOperator) op).getOrigin());
  }

  @Test(expected = SqlParseException.class)
  public void groupby3() throws LogicalOperatorException {
    ExecutableOperator op = generator.getLogicalPlan("select s1,sum(s2) "
            + "FROM root.vehicle.d1 "
            + "WHERE root.vehicle.d1.s1 < 2000 | time >= 1234567 "
            + "group by(111w, 2017-6-2T02:00:12+07:00, [2017-6-2T12:00:12+07:00, now()])");
  }

  @Test
  public void fill1() {
    ExecutableOperator op = generator.getLogicalPlan(
            "select s1 "
                    + "FROM root.vehicle.d1 "
                    + "WHERE time = 1234567 "
                    + "fill(float[previous, 11h])");
    assertEquals(SQLConstant.TOK_QUERY, op.getTokenIntType());
    SetPathOperator select = ((QueryOperator) op).getSetPathOperator();
    assertEquals(1, select.getSuffixPaths().size());
    assertEquals(new Path("s1"), select.getSuffixPaths().get(0));
    FromOperator from = ((QueryOperator) op).getFromOperator();
    assertEquals(1, from.getPrefixPaths().size());
    assertEquals(new Path("root.vehicle.d1"), from.getPrefixPaths().get(0));
    FilterOperator where = ((QueryOperator) op).getFilterOperator();
    assertEquals(SQLConstant.EQUAL, where.getTokenIntType());
    assertEquals(new Path("time"), where.getSinglePath());
    assertEquals("1234567", ((BasicFunctionOperator) where).getValue());
    assertTrue(((QueryOperator) op).isFill());
    assertFalse(((QueryOperator) op).isGroupBy());
    Map<TSDataType, IFill> fillType = ((QueryOperator) op).getFillTypes();
    assertEquals(1, fillType.size());
    List<TSDataType> keyList = new ArrayList<>();
    List<IFill> valueList = new ArrayList<>();
    for (Map.Entry<TSDataType, IFill> e : fillType.entrySet()) {
      keyList.add(e.getKey());
      valueList.add(e.getValue());
    }
    assertEquals(TSDataType.FLOAT, keyList.get(0));
    assertEquals(parseTimeUnit("11", "h"), ((PreviousFill) valueList.get(0)).getBeforeRange());
  }


  @Test
  public void fill2() {
    ExecutableOperator op = generator.getLogicalPlan(
            "select s1 "
                    + "FROM root.vehicle.d1 "
                    + "WHERE time = 1234567 "
                    + "fill(float[previous])");
    assertEquals(SQLConstant.TOK_QUERY, op.getTokenIntType());
    SetPathOperator select = ((QueryOperator) op).getSetPathOperator();
    assertEquals(1, select.getSuffixPaths().size());
    assertEquals(new Path("s1"), select.getSuffixPaths().get(0));
    FromOperator from = ((QueryOperator) op).getFromOperator();
    assertEquals(1, from.getPrefixPaths().size());
    assertEquals(new Path("root.vehicle.d1"), from.getPrefixPaths().get(0));
    FilterOperator where = ((QueryOperator) op).getFilterOperator();
    assertEquals(SQLConstant.EQUAL, where.getTokenIntType());
    assertEquals(new Path("time"), where.getSinglePath());
    assertEquals("1234567", ((BasicFunctionOperator) where).getValue());
    assertTrue(((QueryOperator) op).isFill());
    assertFalse(((QueryOperator) op).isGroupBy());
    Map<TSDataType, IFill> fillType = ((QueryOperator) op).getFillTypes();
    assertEquals(1, fillType.size());
    List<TSDataType> keyList = new ArrayList<>();
    List<IFill> valueList = new ArrayList<>();
    for (Map.Entry<TSDataType, IFill> e : fillType.entrySet()) {
      keyList.add(e.getKey());
      valueList.add(e.getValue());
    }
    assertEquals(TSDataType.FLOAT, keyList.get(0));
    assertEquals(new PreviousFill(-1).getBeforeRange(), ((PreviousFill) valueList.get(0)).getBeforeRange());
  }

  @Test(expected = SqlParseException.class)
  public void fill3() {
    ExecutableOperator op = generator.getLogicalPlan(
            "select s1 "
                    + "FROM root.vehicle.d1 "
                    + "WHERE time = 1234567 "
                    + "fill(boolean[linear, 31s, 123d])");
  }

  @Test(expected = SqlParseException.class)
  public void fill4() {
    ExecutableOperator op = generator.getLogicalPlan(
            "select s1 "
                    + "FROM root.vehicle.d1 "
                    + "WHERE time = 1234567 "
                    + "fill(boolean[linear])");
  }

  @Test(expected = SqlParseException.class)
  public void fill5() {
    ExecutableOperator op = generator.getLogicalPlan(
            "select s1,s2 "
                    + "FROM root.vehicle.d1 "
                    + "WHERE time = 1234567 "
                    + "fill(int[linear, 5ms, 7d], float[previous], boolean[linear], text[previous, 66w])");
  }

  @Test
  public void testFill6() {
    ExecutableOperator op = generator.getLogicalPlan(
            "select sensor1 " +
                    "from root.vehicle.device1 " +
                    "where time = 50 " +
                    "Fill(int32[linear, 1m, 2m])");

  }

  @Test(expected = SqlParseException.class)
  public void limit1() {
    ExecutableOperator op = generator.getLogicalPlan("SELECT device_1.sensor_1,device_2.sensor_2 FROM root.vehicle WHERE not(root.laptop.device_1.sensor_1 < 2000) "
            + "and root.laptop.device_2.sensor_2 > 1000 LIMIT 0");
  }

  @Test
  public void limit2() {
    ExecutableOperator op = generator.getLogicalPlan(
            "SELECT device_1.sensor_1,device_2.sensor_2 " +
                    "FROM root.vehicle " +
                    "WHERE not(root.laptop.device_1.sensor_1 < 2000) " +
                    "and root.laptop.device_2.sensor_2 > 1000 " +
                    "LIMIT 10 OFFSET 2");
    assertEquals(SQLConstant.TOK_QUERY, op.getTokenIntType());
    SetPathOperator select = ((QueryOperator) op).getSetPathOperator();
    assertEquals(2, select.getSuffixPaths().size());
    assertEquals(new Path("device_1.sensor_1"), select.getSuffixPaths().get(0));
    assertEquals(new Path("device_2.sensor_2"), select.getSuffixPaths().get(1));
    FromOperator from = ((QueryOperator) op).getFromOperator();
    assertEquals(1, from.getPrefixPaths().size());
    assertEquals(new Path("root.vehicle"), from.getPrefixPaths().get(0));
    FilterOperator where = ((QueryOperator) op).getFilterOperator();
    assertEquals(SQLConstant.KW_AND, where.getTokenIntType());
    assertEquals(2, where.getChildren().size());
    FilterOperator exp1 = where.getChildren().get(0);
    assertEquals(SQLConstant.KW_NOT, exp1.getTokenIntType());
    assertEquals(1, exp1.getChildren().size());
    BasicFunctionOperator exp2 = (BasicFunctionOperator) exp1.getChildren().get(0);
    assertEquals(SQLConstant.LESSTHAN, exp2.getTokenIntType());
    assertEquals(new Path("root.laptop.device_1.sensor_1"), exp2.getSinglePath());
    assertEquals("2000", exp2.getValue());
    BasicFunctionOperator exp3 = (BasicFunctionOperator) where.getChildren().get(1);
    assertEquals(SQLConstant.GREATERTHAN, exp3.getTokenIntType());
    assertEquals(new Path("root.laptop.device_2.sensor_2"), exp3.getSinglePath());
    assertEquals("1000", exp3.getValue());

  }

  @Test
  public void limit3() {
    ExecutableOperator op = generator.getLogicalPlan(
            "SELECT device_1.sensor_1,device_2.* " +
                    "FROM root.vehicle " +
                    "WHERE root.laptop.device_1.sensor_1 < -2.2E10 && time > now() " +
                    "SLIMIT 10");
    assertEquals(SQLConstant.TOK_QUERY, op.getTokenIntType());
    SetPathOperator select = ((QueryOperator) op).getSetPathOperator();
    assertEquals(2, select.getSuffixPaths().size());
    assertEquals(new Path("device_1.sensor_1"), select.getSuffixPaths().get(0));
    assertEquals(new Path("device_2.*"), select.getSuffixPaths().get(1));
    FromOperator from = ((QueryOperator) op).getFromOperator();
    assertEquals(1, from.getPrefixPaths().size());
    assertEquals(new Path("root.vehicle"), from.getPrefixPaths().get(0));
    FilterOperator where = ((QueryOperator) op).getFilterOperator();
    assertEquals(SQLConstant.KW_AND, where.getTokenIntType());
    assertEquals(2, where.getChildren().size());
    BasicFunctionOperator exp1 = (BasicFunctionOperator) where.getChildren().get(0);
    assertEquals(SQLConstant.LESSTHAN, exp1.getTokenIntType());
    assertEquals(new Path("root.laptop.device_1.sensor_1"), exp1.getSinglePath());
    assertEquals("-2.2E10", exp1.getValue());
    BasicFunctionOperator exp2 = (BasicFunctionOperator) where.getChildren().get(1);
    assertEquals(SQLConstant.GREATERTHAN, exp2.getTokenIntType());
    assertEquals(new Path("time"), exp2.getSinglePath());
    assertTrue(System.currentTimeMillis() - Long.parseLong(String.valueOf(exp2.getValue())) < 10);
    assertTrue(((QueryOperator) op).hasSlimit());
    assertEquals(Long.parseLong("10"), ((QueryOperator) op).getSeriesLimit());
  }

  @Test
  public void limit5() {
    ExecutableOperator op = generator.getLogicalPlan(
            "SELECT device_1.sensor_1,device_2.sensor_2 " +
                    "FROM root.*" +
                    "WHERE root.laptop.device_1.sensor_1 < -2.2E10 && time > now() " +
                    "SLIMIT 10 SOFFSET 3");
    assertEquals(SQLConstant.TOK_QUERY, op.getTokenIntType());
    SetPathOperator select = ((QueryOperator) op).getSetPathOperator();
    assertEquals(2, select.getSuffixPaths().size());
    assertEquals(new Path("device_1.sensor_1"), select.getSuffixPaths().get(0));
    assertEquals(new Path("device_2.sensor_2"), select.getSuffixPaths().get(1));
    FromOperator from = ((QueryOperator) op).getFromOperator();
    assertEquals(1, from.getPrefixPaths().size());
    assertEquals(new Path("root.*"), from.getPrefixPaths().get(0));
    FilterOperator where = ((QueryOperator) op).getFilterOperator();
    assertEquals(SQLConstant.KW_AND, where.getTokenIntType());
    assertEquals(2, where.getChildren().size());
    BasicFunctionOperator exp1 = (BasicFunctionOperator) where.getChildren().get(0);
    assertEquals(SQLConstant.LESSTHAN, exp1.getTokenIntType());
    assertEquals(new Path("root.laptop.device_1.sensor_1"), exp1.getSinglePath());
    assertEquals("-2.2E10", exp1.getValue());
    BasicFunctionOperator exp2 = (BasicFunctionOperator) where.getChildren().get(1);
    assertEquals(SQLConstant.GREATERTHAN, exp2.getTokenIntType());
    assertEquals(new Path("time"), exp2.getSinglePath());
    assertTrue(System.currentTimeMillis() - Long.parseLong(String.valueOf(exp2.getValue())) < 10);
    assertTrue(((QueryOperator) op).hasSlimit());
    assertEquals(Long.parseLong("10"), ((QueryOperator) op).getSeriesLimit());
    assertEquals(Integer.parseInt("3"), ((QueryOperator) op).getSeriesOffset());

  }

  @Test
  public void limit6() {
    ExecutableOperator op = generator.getLogicalPlan(
            "select count(s1),max_time(s2) " +
                    "from root.vehicle.* " +
                    "where root.vehicle.d1.s1 < 0.32e6 and time <= now() " +
                    "group by(10w, 44, [1,3], [4,5])" +
                    "slimit 1" + "soffset 1" + "limit 11" + "offset 3");
    assertTrue(((QueryOperator) op).isGroupBy());
    assertEquals(parseTimeUnit("10", "w"), ((QueryOperator) op).getUnit());
    List<Pair<Long, Long>> intervals = ((QueryOperator) op).getIntervals();
    assertEquals(2, intervals.size());
    assertEquals(1L, (long) intervals.get(0).left);
    assertEquals(3L, (long) intervals.get(0).right);
    assertEquals(4L, (long) intervals.get(1).left);
    assertEquals(5L, (long) intervals.get(1).right);
    assertEquals(44, ((QueryOperator) op).getOrigin());
    assertTrue(((QueryOperator) op).hasSlimit());
    assertEquals(Long.parseLong("1"), ((QueryOperator) op).getSeriesLimit());
    assertEquals(Integer.parseInt("1"), ((QueryOperator) op).getSeriesOffset());
  }

  @Test
  public void limit7() {
    ExecutableOperator op = generator.getLogicalPlan(
            "select * " +
                    "FROM root.vehicle.d1 " +
                    "WHERE time = 1234567 " +
                    "fill(float[previous, 11h])"
                    + "slimit 15" + "soffset 50");
    assertEquals(SQLConstant.TOK_QUERY, op.getTokenIntType());
    SetPathOperator select = ((QueryOperator) op).getSetPathOperator();
    assertEquals(1, select.getSuffixPaths().size());
    assertEquals(new Path("*"), select.getSuffixPaths().get(0));
    FromOperator from = ((QueryOperator) op).getFromOperator();
    assertEquals(1, from.getPrefixPaths().size());
    assertEquals(new Path("root.vehicle.d1"), from.getPrefixPaths().get(0));
    FilterOperator where = ((QueryOperator) op).getFilterOperator();
    assertEquals(SQLConstant.EQUAL, where.getTokenIntType());
    assertEquals(new Path("time"), where.getSinglePath());
    assertEquals("1234567", ((BasicFunctionOperator) where).getValue());
    assertTrue(((QueryOperator) op).isFill());
    assertFalse(((QueryOperator) op).isGroupBy());
    Map<TSDataType, IFill> fillType = ((QueryOperator) op).getFillTypes();
    assertEquals(1, fillType.size());
    List<TSDataType> keyList = new ArrayList<>();
    List<IFill> valueList = new ArrayList<>();
    for (Map.Entry<TSDataType, IFill> e : fillType.entrySet()) {
      keyList.add(e.getKey());
      valueList.add(e.getValue());
    }
    assertEquals(TSDataType.FLOAT, keyList.get(0));
    assertEquals(parseTimeUnit("11", "h"), ((PreviousFill) valueList.get(0)).getBeforeRange());
    assertTrue(((QueryOperator) op).hasSlimit());
    assertEquals(Long.parseLong("15"), ((QueryOperator) op).getSeriesLimit());
    assertEquals(Integer.parseInt("50"), ((QueryOperator) op).getSeriesOffset());
  }

  @Test
  public void limit8() {
    ExecutableOperator operator = generator.getLogicalPlan("select * from root.vehicle.d1 where s1 < 20 and time <= now() slimit 10");
    assertEquals(operator.getClass(), QueryOperator.class);
    assertEquals(10, ((QueryOperator) operator).getSeriesLimit());
  }

  @Test(expected = SqlParseException.class)
  public void limit9() {
    ExecutableOperator op = generator.getLogicalPlan("select * from root.vehicle.d1 where s1 < 20 and time <= now() slimit 1111111111111111111111");
  }

  @Test(expected = SqlParseException.class)
  public void limit10() {
    ExecutableOperator op = generator.getLogicalPlan("select * from root.vehicle.d1 where s1 < 20 and time <= now() slimit 0");
  }

  @Test
  public void offset1() {
    ExecutableOperator operator = generator.getLogicalPlan("select * from root.vehicle.d1 where s1 < 20 and time <= now() slimit 10 soffset 1");
    assertEquals(operator.getClass(), QueryOperator.class);
    assertEquals(10, ((QueryOperator) operator).getSeriesLimit());
    assertEquals(1, ((QueryOperator) operator).getSeriesOffset());
  }

  @Test(expected = LogicalOptimizeException.class)
  public void testSlimitLogicalOptimize() throws LogicalOptimizeException {
    ExecutableOperator op = generator.getLogicalPlan("select s1 from root.vehicle.d1 where s1 < 20 and time <= now() slimit 10 soffset 1");

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
    op = (QueryOperator) concatPathOptimizer.transform(op);
  }

  @Test
  public void testx() {
    ExecutableOperator op = generator.getLogicalPlan( "SELECT s1 FROM root.vehicle.d1 WHERE time > 50 and time <= 100 or s1 < 10");
    FilterOperator fo = ((QueryOperator)op).getFilterOperator();
    System.out.println(fo.toString());
  }

  @Test (expected = SqlParseException.class)
  public void testLimit1() {
    ExecutableOperator op = generator.getLogicalPlan("select s1 from root.vehicle.d1 where s1 < 20 and time <= now() limit 111111111111111111111111");

  }

  private long parseTimeFormat(String timestampStr) throws LogicalOperatorException {
    if (timestampStr.equalsIgnoreCase(SQLConstant.NOW_FUNC)) {
      return System.currentTimeMillis();
    }
    return DatetimeUtils.convertDatetimeStrToLong(timestampStr, IoTDBDescriptor.getInstance().getConfig().getZoneID());
  }

  private long parseTimeUnit(String time, String unit) {
    long timeInterval = Long.parseLong(time);
    if (timeInterval <= 0) {
      throw new SqlParseException("Interval must more than 0.");
    }
    switch (unit) {
      case "w":
        timeInterval *= 7;
      case "d":
        timeInterval *= 24;
      case "h":
        timeInterval *= 60;
      case "m":
        timeInterval *= 60;
      case "s":
        timeInterval *= 1000;
      default:
        break;
    }
    return timeInterval;
  }


}
