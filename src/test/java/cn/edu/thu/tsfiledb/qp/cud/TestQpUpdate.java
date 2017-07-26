package cn.edu.thu.tsfiledb.qp.cud;

import cn.edu.thu.tsfile.common.constant.SystemConstant;
import cn.edu.thu.tsfile.common.exception.ProcessorException;
import cn.edu.thu.tsfile.timeseries.read.qp.Path;
import cn.edu.thu.tsfile.timeseries.read.query.QueryDataSet;
import cn.edu.thu.tsfile.timeseries.utils.StringContainer;
import cn.edu.thu.tsfiledb.qp.exception.QueryProcessorException;
import cn.edu.thu.tsfiledb.qp.executor.OverflowQPExecutor;
import cn.edu.thu.tsfiledb.qp.physical.PhysicalPlan;
import cn.edu.thu.tsfiledb.conf.TsfileDBDescriptor;
import cn.edu.thu.tsfiledb.exception.ArgsErrorException;
import cn.edu.thu.tsfiledb.qp.QueryProcessor;
import cn.edu.thu.tsfiledb.qp.utils.MemIntQpExecutor;
import org.antlr.runtime.RecognitionException;
import org.apache.commons.io.FileUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.awt.geom.FlatteningPathIterator;
import java.io.File;
import java.io.IOException;
import java.util.Iterator;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * test non-query operation, which includes insert/update/delete
 * 
 * @author kangrong
 *
 */
public class TestQpUpdate {
	QueryProcessor processor = new QueryProcessor(new OverflowQPExecutor());

	@Before
	public void before() throws ProcessorException {
		Path path1 = new Path(new StringContainer(new String[] { "root", "laptop", "device_1", "sensor_1" },
				SystemConstant.PATH_SEPARATOR));
		Path path2 = new Path(new StringContainer(new String[] { "root", "laptop", "device_1", "sensor_2" },
				SystemConstant.PATH_SEPARATOR));

		try {
			PhysicalPlan plan1 = processor.parseSQLToPhysicalPlan(
					"CREATE TIMESERIES root.laptop.device_1.sensor_1 WITH DATATYPE=INT32, ENCODING=RLE");
			processor.getExecutor().processNonQuery(plan1);
			PhysicalPlan plan2 = processor.parseSQLToPhysicalPlan(
					"CREATE TIMESERIES root.laptop.device_1.sensor_2 WITH DATATYPE=INT32, ENCODING=RLE");
			processor.getExecutor().processNonQuery(plan2);

			processor.getExecutor().insert(path1, 10, "10");
			processor.getExecutor().insert(path2, 20, "20");
		} catch (QueryProcessorException | ArgsErrorException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	@After
	public void after() {
		try {
			FileUtils.deleteDirectory(new File(TsfileDBDescriptor.getInstance().getConfig().metadataDir));
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	@Test
	public void testUpdate2() throws QueryProcessorException, ArgsErrorException, ProcessorException {
		// update: just time>x or time<x
		String sql = "update root.laptop.device_1.sensor_1 set value=100 where time>100 or (time<=50 and time>10)";
		PhysicalPlan plan1 = processor.parseSQLToPhysicalPlan(sql);
		boolean upRet = processor.getExecutor().processNonQuery(plan1);
		assertTrue(upRet);
		sql = "update root.laptop.device_1.sensor_1 set value=100 where time>100 and time<20";
		try {
			plan1 = processor.parseSQLToPhysicalPlan(sql);
		} catch (QueryProcessorException e) {
			assertEquals("For update command, time filter is invalid", e.getMessage());
		}
		sql = "update root.laptop.device_1.sensor_1 set value=100 where time<-10 or time>1000";
		try {
			plan1 = processor.parseSQLToPhysicalPlan(sql);
		} catch (QueryProcessorException e) {
			assertEquals("update time must be greater than 0.", e.getMessage());
		}
		sql = "update root.laptop.device_1.sensor_1 set value=100 where time<100";
		try {
			plan1 = processor.parseSQLToPhysicalPlan(sql);
		} catch (QueryProcessorException e) {
			fail(e.getMessage());
		}
	}

	@Test
	public void testUpdate()
			throws QueryProcessorException, ProcessorException, RecognitionException, ArgsErrorException {
		String sqlStr = "update root.laptop.device_1.sensor_1 set value = 33000 where time >= 10 and time <= 10";
		PhysicalPlan plan1 = processor.parseSQLToPhysicalPlan(sqlStr);
		boolean upRet = processor.getExecutor().processNonQuery(plan1);

		assertTrue(upRet);
		// query to assert
		sqlStr = "select sensor_1,sensor_2 " + "from root.laptop.device_1";
		PhysicalPlan plan2 = processor.parseSQLToPhysicalPlan(sqlStr);
		Iterator<QueryDataSet> iter = processor.getExecutor().processQuery(plan2);
		String[] expect = { "10, <root.laptop.device_1.sensor_1,33000> <root.laptop.device_1.sensor_2,null> ",
				"20, <root.laptop.device_1.sensor_1,null> <root.laptop.device_1.sensor_2,20> " };
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
	public void testDelete()
			throws QueryProcessorException, ProcessorException, RecognitionException, ArgsErrorException {
		String sqlStr = "delete from root.laptop.device_1.sensor_1 where time < 15";
		PhysicalPlan plan1 = processor.parseSQLToPhysicalPlan(sqlStr);
		boolean upRet = processor.getExecutor().processNonQuery(plan1);

		assertTrue(upRet);
		// query to assert
		sqlStr = "select sensor_1,sensor_2 " + "from root.laptop.device_1";
		PhysicalPlan plan2 = processor.parseSQLToPhysicalPlan(sqlStr);
		Iterator<QueryDataSet> iter = processor.getExecutor().processQuery(plan2);

		String[] expect = { "20, <root.laptop.device_1.sensor_1,null> <root.laptop.device_1.sensor_2,20> " };
		int i = 0;
		while (iter.hasNext()) {
			QueryDataSet set = iter.next();
			while (set.hasNextRecord()) {
				assertEquals(expect[i++], set.getNextRecord().toString());
			}
		}
		assertEquals(expect.length, i);
	}

	@Test
	public void testDeletePaths()
			throws QueryProcessorException, ProcessorException, RecognitionException, ArgsErrorException {
		String sqlStr = "delete from root.laptop.device_1.sensor_1,root.laptop.device_1.sensor_2 where time < 15";
		PhysicalPlan plan1 = processor.parseSQLToPhysicalPlan(sqlStr);
		boolean upRet = processor.getExecutor().processNonQuery(plan1);

		assertTrue(upRet);
		// query to assert
		sqlStr = "select sensor_1,sensor_2 " + "from root.laptop.device_1";
		PhysicalPlan plan2 = processor.parseSQLToPhysicalPlan(sqlStr);
		Iterator<QueryDataSet> iter = processor.getExecutor().processQuery(plan2);

		String[] expect = { "20, <root.laptop.device_1.sensor_1,null> <root.laptop.device_1.sensor_2,20> " };
		int i = 0;
		while (iter.hasNext()) {
			QueryDataSet set = iter.next();
			while (set.hasNextRecord()) {
				assertEquals(expect[i++], set.getNextRecord().toString());
			}
		}
		assertEquals(expect.length, i);
	}

	@Test
	public void testInsert()
			throws QueryProcessorException, ProcessorException, RecognitionException, ArgsErrorException {
		String sqlStr = "insert into root.laptop.device_1 (timestamp, sensor_1) values (30,30)";
		PhysicalPlan plan1 = processor.parseSQLToPhysicalPlan(sqlStr);

		// execute insert
		boolean upRet = processor.getExecutor().processNonQuery(plan1);
		assertTrue(upRet);

		// query to assert
		sqlStr = "select sensor_1,sensor_2 " + "from root.laptop.device_1";
		PhysicalPlan plan2 = processor.parseSQLToPhysicalPlan(sqlStr);
		Iterator<QueryDataSet> iter = processor.getExecutor().processQuery(plan2);

		String[] expect = { "10, <root.laptop.device_1.sensor_1,10> <root.laptop.device_1.sensor_2,null> ",
				"20, <root.laptop.device_1.sensor_1,null> <root.laptop.device_1.sensor_2,20> ",
				"30, <root.laptop.device_1.sensor_1,30> <root.laptop.device_1.sensor_2,null> " };
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
