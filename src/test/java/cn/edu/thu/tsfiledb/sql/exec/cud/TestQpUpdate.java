package cn.edu.thu.tsfiledb.sql.exec.cud;

import cn.edu.thu.tsfile.common.constant.SystemConstant;
import cn.edu.thu.tsfile.common.exception.ProcessorException;
import cn.edu.thu.tsfile.timeseries.read.qp.Path;
import cn.edu.thu.tsfile.timeseries.read.query.QueryDataSet;
import cn.edu.thu.tsfile.timeseries.utils.StringContainer;
import cn.edu.thu.tsfiledb.qp.exception.QueryProcessorException;
import cn.edu.thu.tsfiledb.qp.logical.operator.root.RootOperator;
import cn.edu.thu.tsfiledb.sql.exec.TSqlParserV2;
import cn.edu.thu.tsfiledb.sql.exec.utils.MemIntQpExecutor;
import org.antlr.runtime.RecognitionException;
import org.junit.Before;
import org.junit.Test;


import java.util.Iterator;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * test non-query operation, which includes insert/update/delete
 * 
 * @author kangrong
 *
 */
public class TestQpUpdate {
    private MemIntQpExecutor exec = new MemIntQpExecutor();

    @Before
    public void before() throws ProcessorException {
        Path path1 =
                new Path(new StringContainer(
                        new String[] {"root", "laptop", "device_1", "sensor_1"},
                        SystemConstant.PATH_SEPARATOR));
        Path path2 =
                new Path(new StringContainer(
                        new String[] {"root", "laptop", "device_1", "sensor_2"},
                        SystemConstant.PATH_SEPARATOR));
        exec.insert(path1, 10, "10");
        exec.insert(path2, 20, "20");
    }


    @Test
    public void testUpdate() throws QueryProcessorException, ProcessorException, RecognitionException {
        String sqlStr =
                "update root.laptop.device_1.sensor_1 set value = 33000 where time >= 10 and time <= 10";
        TSqlParserV2 parser = new TSqlParserV2();
        RootOperator update = parser.parseSQLToOperator(sqlStr);
        boolean upRet = parser.nonQuery(update, exec);

        assertTrue(upRet);
        // query to assert
        sqlStr = "select sensor_1,sensor_2 " + "from root.laptop.device_1";
        RootOperator root = parser.parseSQLToOperator(sqlStr);
        Iterator<QueryDataSet> iter = parser.query(root, exec);
        String[] expect =
                {"10, <root.laptop.device_1.sensor_1,33000> <root.laptop.device_1.sensor_2,null> ",
                        "20, <root.laptop.device_1.sensor_1,null> <root.laptop.device_1.sensor_2,20> "};
        int i = 0;
        while (iter.hasNext()) {
            QueryDataSet set = iter.next();
            while (set.hasNextRecord()) {
                assertEquals(set.getNextRecord().toString(), expect[i++]);
            }
        }
        assertEquals(expect.length, i);
    }

    @Test
    public void testDelete() throws QueryProcessorException, ProcessorException, RecognitionException {
        String sqlStr = "delete from root.laptop.device_1.sensor_1 where time < 15";
        TSqlParserV2 parser = new TSqlParserV2();
        RootOperator update = parser.parseSQLToOperator(sqlStr);
        boolean upRet = parser.nonQuery(update, exec);

        assertTrue(upRet);
        // query to assert
        sqlStr = "select sensor_1,sensor_2 " + "from root.laptop.device_1";
        RootOperator root = parser.parseSQLToOperator(sqlStr);
        Iterator<QueryDataSet> iter = parser.query(root, exec);

        String[] expect =
                {"20, <root.laptop.device_1.sensor_1,null> <root.laptop.device_1.sensor_2,20> "};
        int i = 0;
        while (iter.hasNext()) {
            QueryDataSet set = iter.next();
            while (set.hasNextRecord()) {
                assertEquals(set.getNextRecord().toString(), expect[i++]);
            }
        }
        assertEquals(expect.length, i);
    }

    @Test
    public void testInsert() throws QueryProcessorException, ProcessorException, RecognitionException {
        String sqlStr = "insert into root.laptop.device_1 (timestamp, sensor_1) values (30,30)";
        TSqlParserV2 parser = new TSqlParserV2();
        RootOperator update = parser.parseSQLToOperator(sqlStr);

        //execute insert
        boolean upRet = parser.nonQuery(update, exec);
        assertTrue(upRet);

        // query to assert
        sqlStr = "select sensor_1,sensor_2 " + "from root.laptop.device_1";
        RootOperator root = parser.parseSQLToOperator(sqlStr);
        Iterator<QueryDataSet> iter = parser.query(root, exec);

        String[] expect =
                {
                        "10, <root.laptop.device_1.sensor_1,10> <root.laptop.device_1.sensor_2,null> ",
                        "20, <root.laptop.device_1.sensor_1,null> <root.laptop.device_1.sensor_2,20> ",
                        "30, <root.laptop.device_1.sensor_1,30> <root.laptop.device_1.sensor_2,null> "};
        int i = 0;
        while (iter.hasNext()) {
            QueryDataSet set = iter.next();
            while (set.hasNextRecord()) {
                String result = set.getNextRecord().toString();
                assertEquals(expect[i++], result);
            }
        }
        assertEquals(expect.length - 1, i);
    }

}
