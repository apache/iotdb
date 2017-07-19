package cn.edu.thu.tsfiledb.qp.other;

import cn.edu.thu.tsfiledb.qp.exception.QueryProcessorException;
import cn.edu.thu.tsfiledb.qp.logical.sys.LoadDataOperator;
import cn.edu.thu.tsfiledb.sql.ParseGenerator;
import cn.edu.thu.tsfiledb.qp.strategy.LogicalGenerator;
import cn.edu.thu.tsfiledb.sql.parse.ASTNode;
import cn.edu.thu.tsfiledb.sql.parse.ParseUtils;
import org.junit.Test;

import static org.junit.Assert.*;

/**
 * test ast node parsing on authorization
 * 
 * @author kangrong
 *
 */
public class TSPlanContextDataLoadTest {

//    public LoadDataOperator constructData(String input) throws Exception {
//        ASTNode astTree;
//        astTree = ParseGenerator.generateAST(input);
//        astTree = ParseUtils.findRootNonNullToken(astTree);
//        LogicalGenerator generator = new LogicalGenerator();
//        LoadDataOperator loadDataOp = (LoadDataOperator) generator.getLogicalPlan(astTree);
//        if (loadDataOp == null)
//            fail();
//        return loadDataOp;
//    }
//
//    @Test
//    public void testNormalAndError() {
//        // normal
//        LoadDataOperator loadDataOp;
//        String csvFile = "/abs.c";
//        String measureType = "root.a.b.c.d";
//        try {
//            loadDataOp = constructData("LOAD timeseries '/abs.c' root.a.b.c.d");
//            assertEquals(csvFile, loadDataOp.getInputFilePath());
//            assertEquals(measureType, loadDataOp.getMeasureType());
//        } catch (Exception e) {
//            fail();
//        }
//        // error file format
//        try {
//            constructData("LOAD timeseries '' root.a.b.c.d");
//        } catch (Exception e) {
//            assertTrue(e instanceof QueryProcessorException);
//            assertEquals("data load: error format csvPath:''", e.getMessage());
//        }
//        // error node path
//        try {
//            constructData("LOAD timeseries '' root");
//        } catch (Exception e) {
//            assertTrue(e instanceof QueryProcessorException);
//            assertTrue(e.getMessage().startsWith("data load command: child count < 3"));
//        }
//        
//        try {
//            loadDataOp = constructData("LOAD timeseries '\".c' root.a.b.c.d");
//            assertEquals("\".c", loadDataOp.getInputFilePath());
//            assertEquals("root.a.b.c.d", loadDataOp.getMeasureType());
//        } catch (Exception e) {
//            fail();
//        }
//        
//    }
}
