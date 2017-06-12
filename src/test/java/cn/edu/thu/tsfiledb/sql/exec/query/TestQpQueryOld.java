package cn.edu.thu.tsfiledb.sql.exec.query;

import cn.edu.thu.tsfile.common.constant.SystemConstant;
import cn.edu.thu.tsfile.timeseries.read.qp.Path;
import cn.edu.thu.tsfile.timeseries.read.query.QueryDataSet;
import cn.edu.thu.tsfile.timeseries.utils.StringContainer;
import cn.edu.thu.tsfiledb.qp.constant.SQLConstant;
import cn.edu.thu.tsfiledb.qp.exception.QueryProcessorException;
import cn.edu.thu.tsfiledb.qp.physical.PhysicalPlan;
import cn.edu.thu.tsfiledb.qp.QueryProcessor;
import cn.edu.thu.tsfiledb.sql.exec.utils.MemIntQpExecutor;
import org.junit.Before;
import org.junit.Test;

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

    private MemIntQpExecutor exec = new MemIntQpExecutor();

    @Before
    public void before() {
        Path path1 = new Path(new StringContainer(
                new String[]{"root", "laptop", "device_1", "sensor_1"},
                SystemConstant.PATH_SEPARATOR));
        Path path2 = new Path(new StringContainer(
                new String[]{"root", "laptop", "device_1", "sensor_2"},
                SystemConstant.PATH_SEPARATOR));
        for (int i = 1; i <= 10; i++) {
            exec.insert(path1, i * 20, Integer.toString(i * 20 + 1));
            exec.insert(path2, i * 50, Integer.toString(i * 50 + 2));
        }
    }


    @Test
    public void testQueryBasic() throws QueryProcessorException {
        String sqlStr =
                "select device_1.sensor_1,device_1.sensor_2 " + "from root.laptop "
                        + "where time <= 51 or !(time != 100 and time < 460)";
        String[] expected = {"20, <root.laptop.device_1.sensor_1,21> <root.laptop.device_1.sensor_2,null> ",
                "40, <root.laptop.device_1.sensor_1,41> <root.laptop.device_1.sensor_2,null> ",
                "50, <root.laptop.device_1.sensor_1,null> <root.laptop.device_1.sensor_2,52> ",
                "100, <root.laptop.device_1.sensor_1,101> <root.laptop.device_1.sensor_2,102> ",
                "500, <root.laptop.device_1.sensor_1,null> <root.laptop.device_1.sensor_2,502> "};
        QueryProcessor parser = new QueryProcessor();
        PhysicalPlan physicalPlan = parser.parseSQLToPhysicalPlan(sqlStr, exec);
        if (!physicalPlan.isQuery())
            fail();
        Iterator<QueryDataSet> iter = parser.query(physicalPlan, exec);
        int i = 0;
        while (iter.hasNext()) {
            QueryDataSet set = iter.next();
            while (set.hasNextRecord()) {
                assertEquals(expected[i++], set.getNextRecord().toString());
            }
        }
    }

    @Test
    public void testAggregation() throws QueryProcessorException {
        String sqlStr =
                "select sum(device_1.sensor_1) " + "from root.laptop "
                        + "where time <= 51 or !(time != 100 and time < 460)";
        QueryProcessor parser = new QueryProcessor();
        PhysicalPlan plan = parser.parseSQLToPhysicalPlan(sqlStr, exec);
        if (!plan.isQuery())
            fail();
        assertEquals("sum", exec.getParameter(SQLConstant.IS_AGGREGATION));
    }
}
