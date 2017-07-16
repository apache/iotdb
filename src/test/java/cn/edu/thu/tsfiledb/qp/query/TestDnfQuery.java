package cn.edu.thu.tsfiledb.qp.query;

import cn.edu.thu.tsfile.common.constant.SystemConstant;
import cn.edu.thu.tsfile.common.exception.ProcessorException;
import cn.edu.thu.tsfile.timeseries.read.qp.Path;
import cn.edu.thu.tsfile.timeseries.read.query.QueryDataSet;
import cn.edu.thu.tsfile.timeseries.utils.StringContainer;
import cn.edu.thu.tsfiledb.exception.ArgsErrorException;
import cn.edu.thu.tsfiledb.qp.QueryProcessor;
import cn.edu.thu.tsfiledb.qp.exception.QueryProcessorException;
import cn.edu.thu.tsfiledb.qp.executor.OverflowQPExecutor;
import cn.edu.thu.tsfiledb.qp.physical.PhysicalPlan;
import cn.edu.thu.tsfiledb.qp.utils.MemIntQpExecutor;
import org.antlr.runtime.RecognitionException;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

/**
 * @author CGF.
 */
public class TestDnfQuery {

    private static final Logger LOG = LoggerFactory.getLogger(TestQpQuery.class);
    private QueryProcessor processor = new QueryProcessor(new MemIntQpExecutor());

    // @Before
    public void before() throws ProcessorException {
//        Path path1 = new Path(new StringContainer(
//                new String[]{"root", "laptop", "device_1", "sensor_1"},
//                SystemConstant.PATH_SEPARATOR));
//        Path path2 = new Path(new StringContainer(
//                new String[]{"root", "laptop", "device_1", "sensor_2"},
//                SystemConstant.PATH_SEPARATOR));
//        for (int i = 1; i <= 10; i++) {
//            processor.getExecutor().insert(path1, i * 20, Integer.toString(i * 20 + 1));
//            processor.getExecutor().insert(path2, i * 50, Integer.toString(i * 50 + 2));
//        }

        Path path1 = new Path(new StringContainer(
                new String[]{"root", "vehicle", "d0", "s0"},
                SystemConstant.PATH_SEPARATOR));
        Path path2 = new Path(new StringContainer(
                new String[]{"root", "vehicle", "d0", "s1"},
                SystemConstant.PATH_SEPARATOR));
        processor.getExecutor().insert(path1, 1, Integer.toString(101));
        processor.getExecutor().insert(path1, 2, Integer.toString(198));
        processor.getExecutor().insert(path1, 100, Integer.toString(99));
        processor.getExecutor().insert(path1, 101, Integer.toString(99));
        processor.getExecutor().insert(path1, 102, Integer.toString(80));
        processor.getExecutor().insert(path1, 103, Integer.toString(99));
        processor.getExecutor().insert(path1, 104, Integer.toString(90));
        processor.getExecutor().insert(path1, 105, Integer.toString(99));
        processor.getExecutor().insert(path1, 106, Integer.toString(99));
        processor.getExecutor().insert(path1, 2, Integer.toString(10000));
        processor.getExecutor().insert(path1, 50, Integer.toString(10000));
        processor.getExecutor().insert(path1, 1000, Integer.toString(22222));
        processor.getExecutor().delete(path1, 104);
        processor.getExecutor().update(path1, 103, 106, Integer.toString(33333));

        processor.getExecutor().insert(path2, 1, Integer.toString(1101));
        processor.getExecutor().insert(path2, 2, Integer.toString(198));
        processor.getExecutor().insert(path2, 100, Integer.toString(199));
        processor.getExecutor().insert(path2, 101, Integer.toString(199));
        processor.getExecutor().insert(path2, 102, Integer.toString(180));
        processor.getExecutor().insert(path2, 103, Integer.toString(199));
        processor.getExecutor().insert(path2, 104, Integer.toString(190));
        processor.getExecutor().insert(path2, 105, Integer.toString(199));
        processor.getExecutor().insert(path2, 2, Integer.toString(40000));
        processor.getExecutor().insert(path2, 50, Integer.toString(50000));
        processor.getExecutor().insert(path2, 1000, Integer.toString(55555));


//        insert into root.vehicle.d0(timestamp,s1) values(1,1101);
//        insert into root.vehicle.d0(timestamp,s1) values(2,198);
//        insert into root.vehicle.d0(timestamp,s1) values(100,199);
//        insert into root.vehicle.d0(timestamp,s1) values(101,199);
//        insert into root.vehicle.d0(timestamp,s1) values(102,180);
//        insert into root.vehicle.d0(timestamp,s1) values(103,199);
//        insert into root.vehicle.d0(timestamp,s1) values(104,190);
//        insert into root.vehicle.d0(timestamp,s1) values(105,199);
//        insert into root.vehicle.d0(timestamp,s1) values(2,40000);
//        insert into root.vehicle.d0(timestamp,s1) values(50,50000);
//        insert into root.vehicle.d0(timestamp,s1) values(1000,55555);


    }

    // @Test
    public void testEqualsOperator() throws QueryProcessorException, ArgsErrorException {
        String str1 = "select s1 from root.vehicle.d0 where time <= 2";
        PhysicalPlan plan = processor.parseSQLToPhysicalPlan(str1);
        if (!plan.isQuery())
            fail();
        Iterator<QueryDataSet> iter = processor.getExecutor().processQuery(plan);
        LOG.info("query result:");
        String[] result = new String[]{"1, <root.vehicle.d0.s1,1101> ", "2, <root.vehicle.d0.s1,40000> "};
        int cnt = 0;
        while (iter.hasNext()) {
            QueryDataSet set = iter.next();
            while (set.hasNextRecord()) {
                String actual = set.getNextRecord().toString();
                assertEquals(actual, result[cnt++]);
                //System.out.println(actual);
            }
        }

        str1 = "select s1 from root.vehicle.d0 where time > 2";
        plan = processor.parseSQLToPhysicalPlan(str1);
        if (!plan.isQuery())
            fail();
        iter = processor.getExecutor().processQuery(plan);
        LOG.info("query result:");
        result = new String[]{"1, <root.vehicle.d0.s1,1101> ", "2, <root.vehicle.d0.s1,40000> "};
        cnt = 0;
        while (iter.hasNext()) {
            QueryDataSet set = iter.next();
            while (set.hasNextRecord()) {
                String actual = set.getNextRecord().toString();
                //assertEquals(actual, result[cnt++]);
                System.out.println(actual);
            }
        }
    }

    //@Test
    public void testQueryBasic() throws QueryProcessorException, RecognitionException, ArgsErrorException {
        String str1 = "select s0,s1 from root.vehicle.d0 where time <= 150";
        PhysicalPlan plan = processor.parseSQLToPhysicalPlan(str1);
        if (!plan.isQuery())
            fail();
        Iterator<QueryDataSet> iter = processor.getExecutor().processQuery(plan);
        LOG.info("query result:");
        while (iter.hasNext()) {
            QueryDataSet set = iter.next();
            while (set.hasNextRecord()) {
                String actual = set.getNextRecord().toString();
                System.out.println(actual);
            }
        }
    }

    //@Test
    public void testQueryDnf() throws QueryProcessorException, RecognitionException, ArgsErrorException {
        String str1 = "select s0,s1 from root.vehicle.d0 where time < 106 and (s0 >= 60 or s1 <= 200)";
        PhysicalPlan plan = processor.parseSQLToPhysicalPlan(str1);
        if (!plan.isQuery())
            fail();
        Iterator<QueryDataSet> iter = processor.getExecutor().processQuery(plan);
        LOG.info("query result:");
        while (iter.hasNext()) {
            QueryDataSet set = iter.next();
            while (set.hasNextRecord()) {
                String actual = set.getNextRecord().toString();
                System.out.println(actual);
            }
        }
        LOG.info("Query processing complete\n");
    }
}
/**
 *
 20, <root.laptop.device_1.sensor_1,21> <root.laptop.device_1.sensor_2,null>
 40, <root.laptop.device_1.sensor_1,41> <root.laptop.device_1.sensor_2,null>
 50, <root.laptop.device_1.sensor_1,null> <root.laptop.device_1.sensor_2,52>
 60, <root.laptop.device_1.sensor_1,61> <root.laptop.device_1.sensor_2,null>
 80, <root.laptop.device_1.sensor_1,81> <root.laptop.device_1.sensor_2,null>
 100, <root.laptop.device_1.sensor_1,101> <root.laptop.device_1.sensor_2,102>
 120, <root.laptop.device_1.sensor_1,121> <root.laptop.device_1.sensor_2,null>
 140, <root.laptop.device_1.sensor_1,141> <root.laptop.device_1.sensor_2,null>
 150, <root.laptop.device_1.sensor_1,null> <root.laptop.device_1.sensor_2,152>
 160, <root.laptop.device_1.sensor_1,161> <root.laptop.device_1.sensor_2,null>
 180, <root.laptop.device_1.sensor_1,181> <root.laptop.device_1.sensor_2,null>
 200, <root.laptop.device_1.sensor_1,201> <root.laptop.device_1.sensor_2,202>
 250, <root.laptop.device_1.sensor_1,null> <root.laptop.device_1.sensor_2,252>
 300, <root.laptop.device_1.sensor_1,null> <root.laptop.device_1.sensor_2,302>
 350, <root.laptop.device_1.sensor_1,null> <root.laptop.device_1.sensor_2,352>
 400, <root.laptop.device_1.sensor_1,null> <root.laptop.device_1.sensor_2,402>
 450, <root.laptop.device_1.sensor_1,null> <root.laptop.device_1.sensor_2,452>
 500, <root.laptop.device_1.sensor_1,null> <root.laptop.device_1.sensor_2,502>

 select s0,s1 from root.vehicle.d0 where time < 106 and (s0 >= 60 or s1 <= 200)
 */

