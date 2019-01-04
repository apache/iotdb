package cn.edu.tsinghua.iotdb.qp.plan;

import cn.edu.tsinghua.iotdb.conf.TsfileDBConfig;
import cn.edu.tsinghua.iotdb.conf.TsfileDBDescriptor;
import cn.edu.tsinghua.iotdb.exception.ArgsErrorException;
import cn.edu.tsinghua.iotdb.qp.exception.IllegalASTFormatException;
import cn.edu.tsinghua.iotdb.qp.exception.LogicalOperatorException;
import cn.edu.tsinghua.iotdb.qp.exception.LogicalOptimizeException;
import cn.edu.tsinghua.iotdb.qp.exception.QueryProcessorException;
import cn.edu.tsinghua.iotdb.qp.logical.RootOperator;
import cn.edu.tsinghua.iotdb.qp.logical.crud.QueryOperator;
import cn.edu.tsinghua.iotdb.qp.logical.crud.SFWOperator;
import cn.edu.tsinghua.iotdb.qp.strategy.LogicalGenerator;
import cn.edu.tsinghua.iotdb.qp.strategy.optimizer.ConcatPathOptimizer;
import cn.edu.tsinghua.iotdb.qp.utils.MemIntQpExecutor;
import cn.edu.tsinghua.iotdb.sql.ParseGenerator;
import cn.edu.tsinghua.iotdb.sql.parse.ASTNode;
import cn.edu.tsinghua.iotdb.sql.parse.ParseException;
import cn.edu.tsinghua.iotdb.sql.parse.ParseUtils;
import cn.edu.tsinghua.tsfile.common.constant.SystemConstant;
import cn.edu.tsinghua.tsfile.timeseries.read.support.Path;
import cn.edu.tsinghua.tsfile.timeseries.utils.StringContainer;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class LogicalPlanSmallTest {
    private LogicalGenerator generator;

    @Before
    public void before() {
        TsfileDBConfig config = TsfileDBDescriptor.getInstance().getConfig();
        generator = new LogicalGenerator(config.getZoneID());
    }

    @Test
    public void testSlimit1() throws QueryProcessorException, ArgsErrorException {
        String sqlStr =
                "select * from root.vehicle.d1 where s1 < 20 and time <= now() slimit 10";
        ASTNode astTree;
        try {
            astTree = ParseGenerator.generateAST(sqlStr); // parse string to ASTTree
        } catch (ParseException e) {
            //e.printStackTrace();
            throw new IllegalASTFormatException("parsing error,statement: " + sqlStr + " .message:" + e.getMessage());
        }
        ASTNode astNode = ParseUtils.findRootNonNullToken(astTree);
        RootOperator operator = generator.getLogicalPlan(astNode);
        Assert.assertEquals(operator.getClass(), QueryOperator.class);
        Assert.assertEquals(((QueryOperator) operator).getSeriesLimit(), 10);
    }

    @Test(expected = LogicalOperatorException.class)
    public void testSlimit2() throws QueryProcessorException, ArgsErrorException {
        String sqlStr =
                "select * from root.vehicle.d1 where s1 < 20 and time <= now() slimit 1111111111111111111111";
        ASTNode astTree;
        try {
            astTree = ParseGenerator.generateAST(sqlStr); // parse string to ASTTree
        } catch (ParseException e) {
            //e.printStackTrace();
            throw new IllegalASTFormatException("parsing error,statement: " + sqlStr + " .message:" + e.getMessage());
        }
        ASTNode astNode = ParseUtils.findRootNonNullToken(astTree);
        RootOperator operator = generator.getLogicalPlan(astNode);
        // expected to throw LogicalOperatorException: SLIMIT <SN>: SN should be Int32.
    }

    @Test(expected = LogicalOperatorException.class)
    public void testSlimit3() throws QueryProcessorException, ArgsErrorException {
        String sqlStr =
                "select * from root.vehicle.d1 where s1 < 20 and time <= now() slimit 0";
        ASTNode astTree;
        try {
            astTree = ParseGenerator.generateAST(sqlStr); // parse string to ASTTree
        } catch (ParseException e) {
            //e.printStackTrace();
            throw new IllegalASTFormatException("parsing error,statement: " + sqlStr + " .message:" + e.getMessage());
        }
        ASTNode astNode = ParseUtils.findRootNonNullToken(astTree);
        RootOperator operator = generator.getLogicalPlan(astNode);
        // expected to throw LogicalOperatorException: SLIMIT <SN>: SN must be a positive integer and can not be zero.
    }

    @Test
    public void testSoffset() throws QueryProcessorException, ArgsErrorException {
        String sqlStr =
                "select * from root.vehicle.d1 where s1 < 20 and time <= now() slimit 10 soffset 1";
        ASTNode astTree;
        try {
            astTree = ParseGenerator.generateAST(sqlStr); // parse string to ASTTree
        } catch (ParseException e) {
            //e.printStackTrace();
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
        String sqlStr =
                "select s1 from root.vehicle.d1 where s1 < 20 and time <= now() slimit 10 soffset 1";
        ASTNode astTree;
        try {
            astTree = ParseGenerator.generateAST(sqlStr); // parse string to ASTTree
        } catch (ParseException e) {
            //e.printStackTrace();
            throw new IllegalASTFormatException("parsing error,statement: " + sqlStr + " .message:" + e.getMessage());
        }
        ASTNode astNode = ParseUtils.findRootNonNullToken(astTree);
        RootOperator operator = generator.getLogicalPlan(astNode);

        MemIntQpExecutor executor = new MemIntQpExecutor();
        Path path1 =
                new Path(new StringContainer(
                        new String[]{"root", "vehicle", "d1", "s1"},
                        SystemConstant.PATH_SEPARATOR));
        Path path2 =
                new Path(new StringContainer(
                        new String[]{"root", "vehicle", "d2", "s1"},
                        SystemConstant.PATH_SEPARATOR));
        Path path3 =
                new Path(new StringContainer(
                        new String[]{"root", "vehicle", "d3", "s1"},
                        SystemConstant.PATH_SEPARATOR));
        Path path4 =
                new Path(new StringContainer(
                        new String[]{"root", "vehicle", "d4", "s1"},
                        SystemConstant.PATH_SEPARATOR));
        executor.insert(path1, 10, "10");
        executor.insert(path2, 10, "10");
        executor.insert(path3, 10, "10");
        executor.insert(path4, 10, "10");
        ConcatPathOptimizer concatPathOptimizer = new ConcatPathOptimizer(executor);
        operator = (SFWOperator) concatPathOptimizer.transform(operator);
        // expected to throw LogicalOptimizeException: Wrong use of SLIMIT: SLIMIT is not allowed to be used with complete paths.
    }

    @Test(expected = LogicalOperatorException.class)
    public void testLimit1() throws QueryProcessorException, ArgsErrorException {
        String sqlStr =
                "select s1 from root.vehicle.d1 where s1 < 20 and time <= now() limit 111111111111111111111111";
        ASTNode astTree;
        try {
            astTree = ParseGenerator.generateAST(sqlStr); // parse string to ASTTree
        } catch (ParseException e) {
            //e.printStackTrace();
            throw new IllegalASTFormatException("parsing error,statement: " + sqlStr + " .message:" + e.getMessage());
        }
        ASTNode astNode = ParseUtils.findRootNonNullToken(astTree);
        RootOperator operator = generator.getLogicalPlan(astNode);
        // expected to throw LogicalOperatorException: LIMIT <N>: N should be Int32.
    }

    @Test(expected = LogicalOperatorException.class)
    public void testLimit2() throws QueryProcessorException, ArgsErrorException {
        String sqlStr =
                "select s1 from root.vehicle.d1 where s1 < 20 and time <= now() limit 0";
        ASTNode astTree;
        try {
            astTree = ParseGenerator.generateAST(sqlStr); // parse string to ASTTree
        } catch (ParseException e) {
            //e.printStackTrace();
            throw new IllegalASTFormatException("parsing error,statement: " + sqlStr + " .message:" + e.getMessage());
        }
        ASTNode astNode = ParseUtils.findRootNonNullToken(astTree);
        RootOperator operator = generator.getLogicalPlan(astNode);
        // expected to throw LogicalOperatorException: LIMIT <N>: N must be a positive integer and can not be zero.
    }

}
