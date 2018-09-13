package cn.edu.tsinghua.iotdb.qp.plan;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import cn.edu.tsinghua.iotdb.exception.ArgsErrorException;
import cn.edu.tsinghua.iotdb.exception.FileNodeManagerException;
import cn.edu.tsinghua.iotdb.qp.utils.MemIntQpExecutor;
import cn.edu.tsinghua.tsfile.timeseries.readV2.query.QueryDataSet;
import org.junit.After;

import cn.edu.tsinghua.iotdb.qp.QueryProcessor;
import cn.edu.tsinghua.iotdb.qp.exception.QueryProcessorException;
import cn.edu.tsinghua.iotdb.qp.physical.PhysicalPlan;
import cn.edu.tsinghua.iotdb.query.reader.RecordReaderFactory;
import cn.edu.tsinghua.tsfile.common.exception.ProcessorException;
import org.junit.Test;

public class QPUpdateTest {
	private QueryProcessor processor;

	@After
	public void after() throws ProcessorException {
	}

	 @Test
	public void test() throws QueryProcessorException, ArgsErrorException, ProcessorException, IOException {
        init();
//        testUpdate();
        testUpdate2();
//        testDelete();
//        testInsert();
//        testDeletePaths();
	}

	private void init() throws QueryProcessorException, ArgsErrorException, ProcessorException{
        MemIntQpExecutor memProcessor = new MemIntQpExecutor();
        Map<String, List<String>> fakeAllPaths = new HashMap<String, List<String>>() {{
            put("root.laptop.d1.s1", new ArrayList<String>() {{
                add("root.laptop.d1.s1");
            }});
            put("root.laptop.d2.s1", new ArrayList<String>() {{
                add("root.laptop.d1.s1");
            }});
            put("root.laptop.d2.s2", new ArrayList<String>() {{
                add("root.laptop.d1.s2");
            }});
            put("root.laptop.*.s1", new ArrayList<String>() {{
                add("root.laptop.d1.s1");
                add("root.laptop.d2.s1");
            }});
        }};
        memProcessor.setFakeAllPaths(fakeAllPaths);
        processor = new QueryProcessor(memProcessor);
	}

	private void testUpdate2() throws ArgsErrorException, ProcessorException, IOException {
        PhysicalPlan plan = null;
//		String sql = "update root.qp_update_test.device_1.sensor_1 set value=100 where time>100 or (time<=50 and time>10)";
		String sql = "UPDATE root.laptop SET d1.s1 = -33000, d2.s1 = 'string' WHERE time < 100";
		try {
            plan = processor.parseSQLToPhysicalPlan(sql);
        } catch (QueryProcessorException e) {
            assertEquals("UPDATE clause doesn't support multi-update yet.",e.getMessage());
        }
        sql = "UPDATE root.laptop SET d1.s1 = -33000 WHERE time < 100";
        try {
            plan = processor.parseSQLToPhysicalPlan(sql);
        } catch (QueryProcessorException e) {
            assertTrue(false);
        }
        assertEquals(
                "UpdatePlan:  paths:  root.laptop.d1.s1\n" +
                "  value:-33000\n" +
                "  filter: \n" +
                "    199\n",
                plan.printQueryPlan());
//        System.out.println(plan.printQueryPlan());

	}

	private void testUpdate() throws QueryProcessorException, ArgsErrorException, ProcessorException, IOException, FileNodeManagerException {
		String sqlStr = "update root.qp_update_test.device_1.sensor_1 set value = 33000 where time >= 10 and time <= 10";
		PhysicalPlan plan1 = processor.parseSQLToPhysicalPlan(sqlStr);
		boolean upRet = processor.getExecutor().processNonQuery(plan1);

		assertTrue(upRet);
		// query to assert
		sqlStr = "select sensor_1,sensor_2 from root.qp_update_test.device_1";
		PhysicalPlan plan2 = processor.parseSQLToPhysicalPlan(sqlStr);
		QueryDataSet queryDataSet = processor.getExecutor().processQuery(plan2);
		String[] expect = { "10	33000	null",
				"20	null	10" };
		int i = 0;
		while (queryDataSet.hasNext()) {
			assertEquals(queryDataSet.next().toString(), expect[i++]);
		}
		assertEquals(expect.length, i);
	}
	
	private void testDeletePaths() throws QueryProcessorException, ProcessorException, ArgsErrorException, IOException, FileNodeManagerException {
		String sqlStr = "delete from root.qp_update_test.device_1 where time < 15";
		PhysicalPlan plan1 = processor.parseSQLToPhysicalPlan(sqlStr);
		boolean upRet = processor.getExecutor().processNonQuery(plan1);

		assertTrue(upRet);
		// query to assert
		sqlStr = "select sensor_1,sensor_2 from root.qp_update_test.device_1";
		PhysicalPlan plan2 = processor.parseSQLToPhysicalPlan(sqlStr);
		RecordReaderFactory.getInstance().removeRecordReader("root.qp_update_test.device_1", "sensor_1");
		RecordReaderFactory.getInstance().removeRecordReader("root.qp_update_test.device_1", "sensor_2");
		QueryDataSet queryDataSet = processor.getExecutor().processQuery(plan2);

		String[] expect = { "20	null	10" };
		int i = 0;
		while (queryDataSet.hasNext()) {
			assertEquals(queryDataSet.next().toString(), expect[i++]);
		}
		assertEquals(expect.length, i);
	}
	
	private void testDelete() throws QueryProcessorException, ProcessorException, ArgsErrorException, IOException, FileNodeManagerException {
		String sqlStr = "delete from root.qp_update_test.device_1.sensor_1 where time < 15";
		PhysicalPlan plan1 = processor.parseSQLToPhysicalPlan(sqlStr);
		boolean upRet = processor.getExecutor().processNonQuery(plan1);

		assertTrue(upRet);
		// query to assert
		sqlStr = "select sensor_1,sensor_2 from root.qp_update_test.device_1";
		PhysicalPlan plan2 = processor.parseSQLToPhysicalPlan(sqlStr);
		RecordReaderFactory.getInstance().removeRecordReader("root.qp_update_test.device_1", "sensor_1");
		RecordReaderFactory.getInstance().removeRecordReader("root.qp_update_test.device_1", "sensor_2");
		QueryDataSet queryDataSet = processor.getExecutor().processQuery(plan2);

		String[] expect = { "20	null	10" };
		int i = 0;
		while (queryDataSet.hasNext()) {
			assertEquals(queryDataSet.next().toString(), expect[i++]);
		}
		assertEquals(expect.length, i);
	}
	
	private void testInsert() throws QueryProcessorException, ProcessorException, ArgsErrorException, IOException, FileNodeManagerException {
		String sqlStr = "insert into root.qp_update_test.device_1 (timestamp, sensor_1, sensor_2) values (13, 50, 40)";
		PhysicalPlan plan1 = processor.parseSQLToPhysicalPlan(sqlStr);

		// execute insert
		boolean upRet = processor.getExecutor().processNonQuery(plan1);
		assertTrue(upRet);

		RecordReaderFactory.getInstance().removeRecordReader("root.qp_update_test.device_1", "sensor_1");
		RecordReaderFactory.getInstance().removeRecordReader("root.qp_update_test.device_1", "sensor_2");
		// query to assert
		sqlStr = "select sensor_1,sensor_2 from root.qp_update_test.device_1";
		PhysicalPlan plan2 = processor.parseSQLToPhysicalPlan(sqlStr);
		QueryDataSet queryDataSet = processor.getExecutor().processQuery(plan2);

		String[] expect = { 
				"13	50	40",
				"20	null	10" };
		int i = 0;
		while (queryDataSet.hasNext()) {
			assertEquals(queryDataSet.next().toString(), expect[i++]);
		}
		assertEquals(expect.length, i);
	}
}
