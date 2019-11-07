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

import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.exception.MetadataException;
import org.apache.iotdb.db.exception.query.IllegalASTFormatException;
import org.apache.iotdb.db.exception.query.LogicalOperatorException;
import org.apache.iotdb.db.exception.query.LogicalOptimizeException;
import org.apache.iotdb.db.exception.query.QueryProcessException;
import org.apache.iotdb.db.qp.logical.RootOperator;
import org.apache.iotdb.db.qp.logical.crud.QueryOperator;
import org.apache.iotdb.db.qp.logical.crud.SFWOperator;
import org.apache.iotdb.db.qp.logical.sys.DeleteStorageGroupOperator;
import org.apache.iotdb.db.qp.physical.crud.InsertPlan;
import org.apache.iotdb.db.qp.strategy.LogicalGenerator;
import org.apache.iotdb.db.qp.strategy.optimizer.ConcatPathOptimizer;
import org.apache.iotdb.db.qp.utils.MemIntQpExecutor;
import org.apache.iotdb.db.sql.ParseGenerator;
import org.apache.iotdb.db.sql.parse.AstNode;
import org.apache.iotdb.db.sql.parse.ParseException;
import org.apache.iotdb.db.sql.parse.ParseUtils;
import org.apache.iotdb.tsfile.common.constant.TsFileConstant;
import org.apache.iotdb.tsfile.read.common.Path;
import org.apache.iotdb.tsfile.utils.StringContainer;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class LogicalPlanSmallTest {

  private LogicalGenerator generator;

  @Before
  public void before() {
    IoTDBConfig config = IoTDBDescriptor.getInstance().getConfig();
    generator = new LogicalGenerator(config.getZoneID());
  }

  @Test
  public void testSlimit1()
      throws QueryProcessException, MetadataException {
    String sqlStr = "select * from root.vehicle.d1 where s1 < 20 and time <= now() slimit 10";
    AstNode astTree;
    try {
      astTree = ParseGenerator.generateAST(sqlStr); // parse string to ASTTree
    } catch (ParseException e) {
      // e.printStackTrace();
      throw new IllegalASTFormatException(sqlStr, e.getMessage());
    }
    AstNode astNode = ParseUtils.findRootNonNullToken(astTree);
    RootOperator operator = generator.getLogicalPlan(astNode);
    Assert.assertEquals(operator.getClass(), QueryOperator.class);
    Assert.assertEquals(10, ((QueryOperator) operator).getSeriesLimit());
  }

  @Test(expected = LogicalOperatorException.class)
  public void testSlimit2()
      throws QueryProcessException, MetadataException {
    String sqlStr = "select * from root.vehicle.d1 where s1 < 20 and time <= now() slimit 1111111111111111111111";
    AstNode astTree;
    try {
      astTree = ParseGenerator.generateAST(sqlStr); // parse string to ASTTree
    } catch (ParseException e) {
      // e.printStackTrace();
      throw new IllegalASTFormatException(sqlStr, e.getMessage());
    }
    AstNode astNode = ParseUtils.findRootNonNullToken(astTree);
    RootOperator operator = generator.getLogicalPlan(astNode);
    // expected to throw LogicalOperatorException: SLIMIT <SN>: SN should be Int32.
  }

  @Test(expected = LogicalOperatorException.class)
  public void testSlimit3()
      throws QueryProcessException, MetadataException {
    String sqlStr = "select * from root.vehicle.d1 where s1 < 20 and time <= now() slimit 0";
    AstNode astTree;
    try {
      astTree = ParseGenerator.generateAST(sqlStr); // parse string to ASTTree
    } catch (ParseException e) {
      // e.printStackTrace();
      throw new IllegalASTFormatException(sqlStr, e.getMessage());
    }
    AstNode astNode = ParseUtils.findRootNonNullToken(astTree);
    RootOperator operator = generator.getLogicalPlan(astNode);
    // expected to throw LogicalOperatorException: SLIMIT <SN>: SN must be a positive integer and can not be zero.
  }

  @Test
  public void testSoffset()
      throws QueryProcessException, MetadataException {
    String sqlStr = "select * from root.vehicle.d1 where s1 < 20 and time <= now() slimit 10 soffset 1";
    AstNode astTree;
    try {
      astTree = ParseGenerator.generateAST(sqlStr); // parse string to ASTTree
    } catch (ParseException e) {
      // e.printStackTrace();
      throw new IllegalASTFormatException(sqlStr, e.getMessage());
    }
    AstNode astNode = ParseUtils.findRootNonNullToken(astTree);
    RootOperator operator = generator.getLogicalPlan(astNode);
    Assert.assertEquals(operator.getClass(), QueryOperator.class);
    Assert.assertEquals(10, ((QueryOperator) operator).getSeriesLimit());
    Assert.assertEquals(1, ((QueryOperator) operator).getSeriesOffset());
  }

  @Test(expected = LogicalOptimizeException.class)
  public void testSlimitLogicalOptimize()
      throws QueryProcessException, MetadataException {
    String sqlStr = "select s1 from root.vehicle.d1 where s1 < 20 and time <= now() slimit 10 soffset 1";
    AstNode astTree;
    try {
      astTree = ParseGenerator.generateAST(sqlStr); // parse string to ASTTree
    } catch (ParseException e) {
      throw new IllegalASTFormatException(sqlStr, e.getMessage());
    }
    AstNode astNode = ParseUtils.findRootNonNullToken(astTree);
    RootOperator operator = generator.getLogicalPlan(astNode);

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
    // expected to throw LogicalOptimizeException: Wrong use of SLIMIT: SLIMIT is not allowed to be used with
    // complete paths.
  }

  @Test(expected = LogicalOperatorException.class)
  public void testLimit1()
      throws QueryProcessException, MetadataException {
    String sqlStr = "select s1 from root.vehicle.d1 where s1 < 20 and time <= now() limit 111111111111111111111111";
    AstNode astTree;
    try {
      astTree = ParseGenerator.generateAST(sqlStr); // parse string to ASTTree
    } catch (ParseException e) {
      // e.printStackTrace();
      throw new IllegalASTFormatException(sqlStr, e.getMessage());
    }
    AstNode astNode = ParseUtils.findRootNonNullToken(astTree);
    RootOperator operator = generator.getLogicalPlan(astNode);
    // expected to throw LogicalOperatorException: LIMIT <N>: N should be Int32.
  }

  @Test(expected = LogicalOperatorException.class)
  public void testLimit2()
      throws QueryProcessException, MetadataException {
    String sqlStr = "select s1 from root.vehicle.d1 where s1 < 20 and time <= now() limit 0";
    AstNode astTree;
    try {
      astTree = ParseGenerator.generateAST(sqlStr); // parse string to ASTTree
    } catch (ParseException e) {
      // e.printStackTrace();
      throw new IllegalASTFormatException(sqlStr, e.getMessage());
    }
    AstNode astNode = ParseUtils.findRootNonNullToken(astTree);
    RootOperator operator = generator.getLogicalPlan(astNode);
    // expected to throw LogicalOperatorException: LIMIT <N>: N must be a positive integer and can not be zero.
  }

  @Test
  public void testDeleteStorageGroup()
      throws QueryProcessException, MetadataException {
    String sqlStr = "delete storage group root.vehicle.d1";
    AstNode astTree;
    try {
      astTree = ParseGenerator.generateAST(sqlStr);
    } catch (ParseException e) {
      throw new IllegalASTFormatException(sqlStr, e.getMessage());
    }
    AstNode astNode = ParseUtils.findRootNonNullToken(astTree);
    RootOperator operator = generator.getLogicalPlan(astNode);

    Assert.assertEquals(DeleteStorageGroupOperator.class, operator.getClass());
    Path path = new Path("root.vehicle.d1");
    Assert.assertEquals(path, ((DeleteStorageGroupOperator) operator).getDeletePathList().get(0));
  }

}
