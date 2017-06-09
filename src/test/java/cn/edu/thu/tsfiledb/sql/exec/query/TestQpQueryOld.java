package cn.edu.thu.tsfiledb.sql.exec.query;

import cn.edu.thu.tsfile.common.constant.SystemConstant;
import cn.edu.thu.tsfile.timeseries.read.qp.Path;
import cn.edu.thu.tsfile.timeseries.read.query.QueryDataSet;
import cn.edu.thu.tsfile.timeseries.utils.StringContainer;
import cn.edu.thu.tsfiledb.qp.constant.SQLConstant;
import cn.edu.thu.tsfiledb.qp.exception.QueryProcessorException;
import cn.edu.thu.tsfiledb.qp.logical.operator.RootOperator;
import cn.edu.thu.tsfiledb.qp.logical.operator.crud.SFWOperator;
import cn.edu.thu.tsfiledb.sql.exec.TSqlParserV2;
import cn.edu.thu.tsfiledb.sql.exec.utils.MemIntQpExecutor;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;


/**
 * test query operation
 * 
 * @author kangrong
 *
 */
public class TestQpQueryOld {
    private static final Logger LOG = LoggerFactory.getLogger(TestQpQueryOld.class);
    private MemIntQpExecutor exec = new MemIntQpExecutor();
    private Path path1;
    private Path path2;

    @Before
    public void before() {
        path1 =
                new Path(new StringContainer(
                        new String[] {"root", "laptop", "device_1", "sensor_1"},
                        SystemConstant.PATH_SEPARATOR));
        path2 =
                new Path(new StringContainer(
                        new String[] {"root", "laptop", "device_1", "sensor_2"},
                        SystemConstant.PATH_SEPARATOR));
        for (int i = 1; i <= 10; i++) {
            exec.insert(path1, i * 20, new Integer(i * 20 + 1).toString());
            exec.insert(path2, i * 50, new Integer(i * 50 + 2).toString());
        }
    }


    @Test
    public void testQueryBasic() throws QueryProcessorException {
        String sqlStr =
                "select sensor_1,sensor_2 " + "from device_1 "
                        + "where time <= 51 or !(time != 100 and time < 460)";
        sqlStr =
                "select device_1.sensor_1,device_1.sensor_2 " + "from root.laptop "
                        + "where time <= 51 or !(time != 100 and time < 460)";
        TSqlParserV2 parser = new TSqlParserV2();
        RootOperator root = parser.parseSQLToOperator(sqlStr);
        if (!root.isQuery())
            fail();
        Iterator<QueryDataSet> iter = parser.query(root, exec);
        System.out.println("query result:\n");
        while (iter.hasNext()) {
            QueryDataSet set = iter.next();
            while (set.hasNextRecord()) {
                System.out.println(set.getNextRecord().toString());
            }
        }
        /**
         * 20, <device_1.sensor_1,21> <device_1.sensor_2,null> <br>
         * 50, <device_1.sensor_1,null> <device_1.sensor_2,52> <br>
         * 100, <device_1.sensor_1,101> <device_1.sensor_2,102>
         */
        // assertTrue(result);
        // TODO query to assert

    }

    @Test
    public void testAggregation() throws QueryProcessorException {
        String sqlStr =
                "select sum(device_1.sensor_1) " + "from root.laptop "
                        + "where time <= 51 or !(time != 100 and time < 460)";
        TSqlParserV2 parser = new TSqlParserV2();
        RootOperator root = parser.parseSQLToOperator(sqlStr);
        if (!root.isQuery())
            fail();
        SFWOperator sfw = (SFWOperator) root;
        sfw = parser.logicalOptimize(sfw);
        parser.transformToPhysicalPlan(sfw, exec);
        assertEquals("sum", exec.getParameter(SQLConstant.IS_AGGREGATION));
    }
}
