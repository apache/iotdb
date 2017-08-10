package cn.edu.thu.tsfiledb.qp.cud;

import static org.junit.Assert.*;

import java.io.File;
import java.io.IOException;
import java.util.Iterator;

import cn.edu.thu.tsfiledb.query.management.RecordReaderFactory;
import cn.edu.thu.tsfiledb.query.reader.RecordReader;
import org.apache.commons.io.FileUtils;
import org.junit.After;
import org.junit.Test;

import cn.edu.thu.tsfile.common.exception.ProcessorException;
import cn.edu.thu.tsfile.timeseries.read.query.QueryDataSet;
import cn.edu.thu.tsfiledb.conf.TsfileDBDescriptor;
import cn.edu.thu.tsfiledb.exception.ArgsErrorException;
import cn.edu.thu.tsfiledb.qp.QueryProcessor;
import cn.edu.thu.tsfiledb.qp.exception.QueryProcessorException;
import cn.edu.thu.tsfiledb.qp.executor.OverflowQPExecutor;
import cn.edu.thu.tsfiledb.qp.physical.PhysicalPlan;

public class QPUpdateTest {
	QueryProcessor processor = new QueryProcessor(new OverflowQPExecutor());
	@After
	public void after() throws ProcessorException {
//		try {
//			FileUtils.deleteDirectory(new File(TsfileDBDescriptor.getInstance().getConfig().metadataDir));
//			FileUtils.forceDelete(new File(TsfileDBDescriptor.getInstance().getConfig().overflowDataDir+File.separator+"root.qp_update_test"));
//			FileUtils.forceDelete(new File(TsfileDBDescriptor.getInstance().getConfig().bufferWriteDir+File.separator+"root.qp_update_test"));
//			FileUtils.forceDelete(new File(TsfileDBDescriptor.getInstance().getConfig().fileNodeDir+File.separator+"root.qp_update_test"));
//		} catch (IOException e) {
//			// TODO Auto-generated catch block
//			e.printStackTrace();
//		}
	}

	// @Test
	public void test() throws QueryProcessorException, ArgsErrorException, ProcessorException, IOException {
        init();
        testUpdate();
        testUpdate2();
        testDelete();
        testInsert();
        testDeletePaths();
	}
	
	private void init() throws QueryProcessorException, ArgsErrorException, ProcessorException{
		PhysicalPlan plan = processor.parseSQLToPhysicalPlan("CREATE TIMESERIES root.qp_update_test.device_1.sensor_1 WITH DATATYPE=INT32, ENCODING=RLE");
		processor.getExecutor().processNonQuery(plan);
		plan = processor.parseSQLToPhysicalPlan("CREATE TIMESERIES root.qp_update_test.device_1.sensor_2 WITH DATATYPE=INT32, ENCODING=RLE");
		processor.getExecutor().processNonQuery(plan);
				
		plan = processor.parseSQLToPhysicalPlan("SET STORAGE GROUP TO root.qp_update_test");
		processor.getExecutor().processNonQuery(plan);
		
		
		plan = processor.parseSQLToPhysicalPlan("insert into root.qp_update_test.device_1(timestamp,sensor_1) values(10,10)");
		processor.getExecutor().processNonQuery(plan);
		
		plan = processor.parseSQLToPhysicalPlan("insert into root.qp_update_test.device_1(timestamp,sensor_2) values(20,10)");
		processor.getExecutor().processNonQuery(plan);
	}
	
	private void testUpdate2() throws QueryProcessorException, ArgsErrorException, ProcessorException, IOException {
		String sql = "update root.qp_update_test.device_1.sensor_1 set value=100 where time>100 or (time<=50 and time>10)";
		PhysicalPlan plan = processor.parseSQLToPhysicalPlan(sql);
		boolean upRet = processor.getExecutor().processNonQuery(plan);
		assertTrue(upRet);
		sql = "update root.qp_update_test.device_1.sensor_1 set value=100 where time>100 and time<20";
		try {
			plan = processor.parseSQLToPhysicalPlan(sql);
		} catch (QueryProcessorException e) {
			assertEquals("For update command, time filter is invalid", e.getMessage());
		}
		sql = "update root.qp_update_test.device_1.sensor_1 set value=100 where time<-10 or time>1000";
		try {
			plan = processor.parseSQLToPhysicalPlan(sql);
		} catch (QueryProcessorException e) {
			assertEquals("update time must be greater than 0.", e.getMessage());
		}
		sql = "update root.qp_update_test.device_1.sensor_1 set value=100 where time<100";
		// remove read cache compulsory
		RecordReaderFactory.getInstance().removeRecordReader("root.qp_update_test.device_1", "sensor_1");
		try {
			plan = processor.parseSQLToPhysicalPlan(sql);
			processor.getExecutor().processNonQuery(plan);
			String sqlStr = "select sensor_1,sensor_2 from root.qp_update_test.device_1";
			PhysicalPlan plan2 = processor.parseSQLToPhysicalPlan(sqlStr);
			Iterator<QueryDataSet> iter = processor.getExecutor().processQuery(plan2);

			String[] expect = { 
					"10	100	null",
					"20	null	10" };
			int i = 0;
			while (iter.hasNext()) {
				QueryDataSet set = iter.next();
				while (set.hasNextRecord()) {
					String result = set.getNextRecord().toString();
					assertEquals(expect[i++], result);
				}
			}
			assertEquals(expect.length, i);
			
		} catch (QueryProcessorException e) {
			fail(e.getMessage());
		}
	}

	private void testUpdate() throws QueryProcessorException, ArgsErrorException, ProcessorException {
		String sqlStr = "update root.qp_update_test.device_1.sensor_1 set value = 33000 where time >= 10 and time <= 10";
		PhysicalPlan plan1 = processor.parseSQLToPhysicalPlan(sqlStr);
		boolean upRet = processor.getExecutor().processNonQuery(plan1);

		assertTrue(upRet);
		// query to assert
		sqlStr = "select sensor_1,sensor_2 from root.qp_update_test.device_1";
		PhysicalPlan plan2 = processor.parseSQLToPhysicalPlan(sqlStr);
		Iterator<QueryDataSet> iter = processor.getExecutor().processQuery(plan2);
		String[] expect = { "10	33000	null",
				"20	null	10" };
		int i = 0;
		while (iter.hasNext()) {
			QueryDataSet set = iter.next();
			while (set.hasNextRecord()) {
				assertEquals(set.getNextRecord().toString(), expect[i++]);
			}
		}
		assertEquals(expect.length, i);
	}
	
	private void testDeletePaths() throws QueryProcessorException, ProcessorException, ArgsErrorException, IOException {
		String sqlStr = "delete from root.qp_update_test.device_1 where time < 15";
		PhysicalPlan plan1 = processor.parseSQLToPhysicalPlan(sqlStr);
		boolean upRet = processor.getExecutor().processNonQuery(plan1);

		assertTrue(upRet);
		// query to assert
		sqlStr = "select sensor_1,sensor_2 from root.qp_update_test.device_1";
		PhysicalPlan plan2 = processor.parseSQLToPhysicalPlan(sqlStr);
		RecordReaderFactory.getInstance().removeRecordReader("root.qp_update_test.device_1", "sensor_1");
		RecordReaderFactory.getInstance().removeRecordReader("root.qp_update_test.device_1", "sensor_2");
		Iterator<QueryDataSet> iter = processor.getExecutor().processQuery(plan2);

		String[] expect = { "20	null	10" };
		int i = 0;
		while (iter.hasNext()) {
			QueryDataSet set = iter.next();
			while (set.hasNextRecord()) {
				assertEquals(expect[i++], set.getNextRecord().toString());
			}
		}
		assertEquals(expect.length, i);
	}
	
	private void testDelete() throws QueryProcessorException, ProcessorException, ArgsErrorException, IOException {
		String sqlStr = "delete from root.qp_update_test.device_1.sensor_1 where time < 15";
		PhysicalPlan plan1 = processor.parseSQLToPhysicalPlan(sqlStr);
		boolean upRet = processor.getExecutor().processNonQuery(plan1);

		assertTrue(upRet);
		// query to assert
		sqlStr = "select sensor_1,sensor_2 from root.qp_update_test.device_1";
		PhysicalPlan plan2 = processor.parseSQLToPhysicalPlan(sqlStr);
		RecordReaderFactory.getInstance().removeRecordReader("root.qp_update_test.device_1", "sensor_1");
		RecordReaderFactory.getInstance().removeRecordReader("root.qp_update_test.device_1", "sensor_2");
		Iterator<QueryDataSet> iter = processor.getExecutor().processQuery(plan2);

		String[] expect = { "20	null	10" };
		int i = 0;
		while (iter.hasNext()) {
			QueryDataSet set = iter.next();
			while (set.hasNextRecord()) {
				assertEquals(expect[i++], set.getNextRecord().toString());
			}
		}
		assertEquals(expect.length, i);
	}
	
	private void testInsert() throws QueryProcessorException, ProcessorException, ArgsErrorException, IOException {
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
		Iterator<QueryDataSet> iter = processor.getExecutor().processQuery(plan2);

		String[] expect = { 
				"13	50	40",
				"20	null	10" };
		int i = 0;
		while (iter.hasNext()) {
			QueryDataSet set = iter.next();
			while (set.hasNextRecord()) {
				String result = set.getNextRecord().toString();
				assertEquals(expect[i++], result);
			}
		}
		assertEquals(expect.length, i);
	}
}
