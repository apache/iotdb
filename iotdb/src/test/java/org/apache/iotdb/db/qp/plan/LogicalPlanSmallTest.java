/**
 * Copyright © 2019 Apache IoTDB(incubating) (dev@iotdb.apache.org)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
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

import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.exception.ArgsErrorException;
import org.apache.iotdb.db.exception.qp.IllegalASTFormatException;
import org.apache.iotdb.db.exception.qp.LogicalOperatorException;
import org.apache.iotdb.db.exception.qp.LogicalOptimizeException;
import org.apache.iotdb.db.exception.qp.QueryProcessorException;
import org.apache.iotdb.db.qp.logical.RootOperator;
import org.apache.iotdb.db.qp.logical.crud.QueryOperator;
import org.apache.iotdb.db.qp.logical.crud.SFWOperator;
import org.apache.iotdb.db.qp.strategy.LogicalGenerator;
import org.apache.iotdb.db.qp.strategy.optimizer.ConcatPathOptimizer;
import org.apache.iotdb.db.qp.utils.MemIntQpExecutor;
import org.apache.iotdb.db.sql.ParseGenerator;
import org.apache.iotdb.db.sql.parse.ASTNode;
import org.apache.iotdb.db.sql.parse.ParseException;
import org.apache.iotdb.db.sql.parse.ParseUtils;
import org.apache.iotdb.tsfile.common.constant.SystemConstant;
import org.apache.iotdb.tsfile.read.common.Path;
import org.apache.iotdb.tsfile.utils.StringContainer;
import org.apache.iotdb.db.exception.ArgsErrorException;
import org.apache.iotdb.db.qp.logical.RootOperator;
import org.apache.iotdb.db.qp.logical.crud.QueryOperator;
import org.apache.iotdb.db.qp.logical.crud.SFWOperator;
import org.apache.iotdb.db.qp.utils.MemIntQpExecutor;
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
    public void testSlimit1() throws QueryProcessorException, ArgsErrorException {
        String sqlStr = "select * from root.vehicle.d1 where s1 < 20 and time <= now() slimit 10";
        ASTNode astTree;
        try {
            astTree = ParseGenerator.generateAST(sqlStr); // parse string to ASTTree
        } catch (ParseException e) {
            // e.printStackTrace();
            throw new IllegalASTFormatException("parsing error,statement: " + sqlStr + " .message:" + e.getMessage());
        }
        ASTNode astNode = ParseUtils.findRootNonNullToken(astTree);
        RootOperator operator = generator.getLogicalPlan(astNode);
        Assert.assertEquals(operator.getClass(), QueryOperator.class);
        Assert.assertEquals(((QueryOperator) operator).getSeriesLimit(), 10);
    }

    @Test(expected = LogicalOperatorException.class)
    public void testSlimit2() throws QueryProcessorException, ArgsErrorException {
        String sqlStr = "select * from root.vehicle.d1 where s1 < 20 and time <= now() slimit 1111111111111111111111";
        ASTNode astTree;
        try {
            astTree = ParseGenerator.generateAST(sqlStr); // parse string to ASTTree
        } catch (ParseException e) {
            // e.printStackTrace();
            throw new IllegalASTFormatException("parsing error,statement: " + sqlStr + " .message:" + e.getMessage());
        }
        ASTNode astNode = ParseUtils.findRootNonNullToken(astTree);
        RootOperator operator = generator.getLogicalPlan(astNode);
        // expected to throw LogicalOperatorException: SLIMIT <SN>: SN should be Int32.
    }

    @Test(expected = LogicalOperatorException.class)
    public void testSlimit3() throws QueryProcessorException, ArgsErrorException {
        String sqlStr = "select * from root.vehicle.d1 where s1 < 20 and time <= now() slimit 0";
        ASTNode astTree;
        try {
            astTree = ParseGenerator.generateAST(sqlStr); // parse string to ASTTree
        } catch (ParseException e) {
            // e.printStackTrace();
            throw new IllegalASTFormatException("parsing error,statement: " + sqlStr + " .message:" + e.getMessage());
        }
        ASTNode astNode = ParseUtils.findRootNonNullToken(astTree);
        RootOperator operator = generator.getLogicalPlan(astNode);
        // expected to throw LogicalOperatorException: SLIMIT <SN>: SN must be a positive integer and can not be zero.
    }

    @Test
    public void testSoffset() throws QueryProcessorException, ArgsErrorException {
        String sqlStr = "select * from root.vehicle.d1 where s1 < 20 and time <= now() slimit 10 soffset 1";
        ASTNode astTree;
        try {
            astTree = ParseGenerator.generateAST(sqlStr); // parse string to ASTTree
        } catch (ParseException e) {
            // e.printStackTrace();
            throw new IllegalASTFormatException("parsing error,statement: " + sqlStr + " .message:" + e.getMessage());
        }
        ASTNode astNode = ParseUtils.findRootNonNullToken(astTree);
        RootOperator operator = generator.getLogicalPlan(astNode);
        Assert.assertEquals(operator.getClass(), QueryOperator.class);
        Assert.assertEquals(((QueryOperator) operator).getSeriesLimit(), 10);
        Assert.assertEquals(((QueryOperator) operator).getSeriesOffset(), 1);
    }

    @Test(expected = LogicalOptimizeException.class)
    public void testSlimitLogicalOptimize() throws QueryProcessorException, ArgsErrorException {
        String sqlStr = "select s1 from root.vehicle.d1 where s1 < 20 and time <= now() slimit 10 soffset 1";
        ASTNode astTree;
        try {
            astTree = ParseGenerator.generateAST(sqlStr); // parse string to ASTTree
        } catch (ParseException e) {
            // e.printStackTrace();
            throw new IllegalASTFormatException("parsing error,statement: " + sqlStr + " .message:" + e.getMessage());
        }
        ASTNode astNode = ParseUtils.findRootNonNullToken(astTree);
        RootOperator operator = generator.getLogicalPlan(astNode);

        MemIntQpExecutor executor = new MemIntQpExecutor();
        Path path1 = new Path(
                new StringContainer(new String[] { "root", "vehicle", "d1", "s1" }, SystemConstant.PATH_SEPARATOR));
        Path path2 = new Path(
                new StringContainer(new String[] { "root", "vehicle", "d2", "s1" }, SystemConstant.PATH_SEPARATOR));
        Path path3 = new Path(
                new StringContainer(new String[] { "root", "vehicle", "d3", "s1" }, SystemConstant.PATH_SEPARATOR));
        Path path4 = new Path(
                new StringContainer(new String[] { "root", "vehicle", "d4", "s1" }, SystemConstant.PATH_SEPARATOR));
        executor.insert(path1, 10, "10");
        executor.insert(path2, 10, "10");
        executor.insert(path3, 10, "10");
        executor.insert(path4, 10, "10");
        ConcatPathOptimizer concatPathOptimizer = new ConcatPathOptimizer(executor);
        operator = (SFWOperator) concatPathOptimizer.transform(operator);
        // expected to throw LogicalOptimizeException: Wrong use of SLIMIT: SLIMIT is not allowed to be used with
        // complete paths.
    }

    @Test(expected = LogicalOperatorException.class)
    public void testLimit1() throws QueryProcessorException, ArgsErrorException {
        String sqlStr = "select s1 from root.vehicle.d1 where s1 < 20 and time <= now() limit 111111111111111111111111";
        ASTNode astTree;
        try {
            astTree = ParseGenerator.generateAST(sqlStr); // parse string to ASTTree
        } catch (ParseException e) {
            // e.printStackTrace();
            throw new IllegalASTFormatException("parsing error,statement: " + sqlStr + " .message:" + e.getMessage());
        }
        ASTNode astNode = ParseUtils.findRootNonNullToken(astTree);
        RootOperator operator = generator.getLogicalPlan(astNode);
        // expected to throw LogicalOperatorException: LIMIT <N>: N should be Int32.
    }

    @Test(expected = LogicalOperatorException.class)
    public void testLimit2() throws QueryProcessorException, ArgsErrorException {
        String sqlStr = "select s1 from root.vehicle.d1 where s1 < 20 and time <= now() limit 0";
        ASTNode astTree;
        try {
            astTree = ParseGenerator.generateAST(sqlStr); // parse string to ASTTree
        } catch (ParseException e) {
            // e.printStackTrace();
            throw new IllegalASTFormatException("parsing error,statement: " + sqlStr + " .message:" + e.getMessage());
        }
        ASTNode astNode = ParseUtils.findRootNonNullToken(astTree);
        RootOperator operator = generator.getLogicalPlan(astNode);
        // expected to throw LogicalOperatorException: LIMIT <N>: N must be a positive integer and can not be zero.
    }

}
