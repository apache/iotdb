package cn.edu.thu.tsfiledb.engine.filenode;

import static org.junit.Assert.*;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import cn.edu.thu.tsfile.common.conf.TSFileConfig;
import cn.edu.thu.tsfile.common.conf.TSFileDescriptor;
import cn.edu.thu.tsfile.common.utils.Pair;
import cn.edu.thu.tsfile.file.metadata.RowGroupMetaData;
import cn.edu.thu.tsfile.file.metadata.enums.TSDataType;
import cn.edu.thu.tsfile.timeseries.filter.definition.SingleSeriesFilterExpression;
import cn.edu.thu.tsfile.timeseries.read.query.DynamicOneColumnData;
import cn.edu.thu.tsfile.timeseries.write.record.DataPoint;
import cn.edu.thu.tsfile.timeseries.write.record.TSRecord;
import cn.edu.thu.tsfiledb.conf.TSFileDBConfig;
import cn.edu.thu.tsfiledb.conf.TSFileDBDescriptor;
import cn.edu.thu.tsfiledb.engine.exception.FileNodeManagerException;
import cn.edu.thu.tsfiledb.engine.lru.MetadataManagerHelper;
import cn.edu.thu.tsfiledb.engine.overflow.io.EngineTestHelper;

public class FileNodeManagerTest {

	private TSFileConfig tsconfig = TSFileDescriptor.getInstance().getConfig();
	private TSFileDBConfig tsdbconfig = TSFileDBDescriptor.getInstance().getConfig();
	

	private FileNodeManager fManager = null;

	private String deltaObjectId = "root.vehicle.d0";
	private String deltaObjectId2 = "root.vehicle.d1";
	private String measurementId = "s0";
	private TSDataType dataType = TSDataType.INT32;

	@Before
	public void setUp() throws Exception {
		tsdbconfig.FileNodeDir = "filenode" + File.separatorChar;
		tsdbconfig.BufferWriteDir = "bufferwrite";
		tsdbconfig.overflowDataDir = "overflow";

		// set rowgroupsize
		tsconfig.rowGroupSize = 2000;
		tsconfig.pageCheckSizeThreshold = 3;
		tsconfig.pageSize = 100;
		tsconfig.defaultMaxStringLength = 2;
		EngineTestHelper.delete(tsdbconfig.FileNodeDir);
		EngineTestHelper.delete(tsdbconfig.BufferWriteDir);
		EngineTestHelper.delete(tsdbconfig.overflowDataDir);
		MetadataManagerHelper.initMetadata();
	}

	@After
	public void tearDown() throws Exception {
		EngineTestHelper.delete(tsdbconfig.FileNodeDir);
		EngineTestHelper.delete(tsdbconfig.BufferWriteDir);
		EngineTestHelper.delete(tsdbconfig.overflowDataDir);
		MetadataManagerHelper.clearMetadata();
	}

	@Test
	public void testClose() {

	}

	@Test
	public void testOverflow() {

	}

	@Test
	public void testBufferwriteInsert() {

		List<Pair<Long, Long>> pairList = new ArrayList<>();
		pairList.add(new Pair<Long, Long>(100L, 200L));
		pairList.add(new Pair<Long, Long>(300L, 400L));
		pairList.add(new Pair<Long, Long>(500L, 600L));
		pairList.add(new Pair<Long, Long>(700L, 800L));
		createBufferwriteFiles(pairList, deltaObjectId);

		createBufferwriteInMemory(new Pair<Long, Long>(900L, 1000L));

		fManager = FileNodeManager.getInstance();
		try {
			int token = fManager.beginQuery(deltaObjectId);

			QueryStructure queryResult = fManager.query(deltaObjectId, measurementId, null, null, null);
			DynamicOneColumnData bufferwriteinmemory = queryResult.getBufferwriteDataInMemory();
			List<RowGroupMetaData> bufferwriteinDisk = queryResult.getBufferwriteDataInDisk();
			assertEquals(true, bufferwriteinmemory != null);
			assertEquals(true, bufferwriteinDisk != null);
			List<IntervalFileNode> newInterFiles = queryResult.getBufferwriteDataInFiles();
			assertEquals(5, newInterFiles.size());
			for (int i = 0; i < pairList.size(); i++) {
				IntervalFileNode temp = newInterFiles.get(i);
				Pair<Long, Long> time = pairList.get(i);
				assertEquals(time.left.longValue(), temp.startTime);
				assertEquals(time.right.longValue(), temp.endTime);
				System.out.println(time);
			}

			List<Object> overflowResult = queryResult.getAllOverflowData();
			assertEquals(null, overflowResult.get(0));
			assertEquals(null, overflowResult.get(1));
			assertEquals(null, overflowResult.get(2));
			assertEquals(null, overflowResult.get(3));
			fManager.endQuery(deltaObjectId, token);
			fManager.closeAll();
			// waitToSleep();
		} catch (FileNodeManagerException e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}

	@Test
	public void testOverflowInsert() {
		// create bufferwrite data
		List<Pair<Long, Long>> pairList = new ArrayList<>();
		pairList.add(new Pair<Long, Long>(100L, 200L));
		pairList.add(new Pair<Long, Long>(300L, 400L));
		pairList.add(new Pair<Long, Long>(500L, 600L));
		pairList.add(new Pair<Long, Long>(700L, 800L));
		createBufferwriteFiles(pairList, deltaObjectId);

		long[] overflowInsert1 = { 2, 4, 6, 8 };
		long[] overflowInsert2 = { 202, 204, 206, 208 };

		createOverflowInserts(overflowInsert1, deltaObjectId);
		try {
			int token = fManager.beginQuery(deltaObjectId);
			QueryStructure queryResult = fManager.query(deltaObjectId, measurementId, null, null, null);

			List<Object> overflowResult = queryResult.getAllOverflowData();
			DynamicOneColumnData insertData = (DynamicOneColumnData) overflowResult.get(0);
			assertEquals(overflowInsert1.length, insertData.length);
			for (int i = 0; i < overflowInsert1.length; i++) {
				assertEquals(overflowInsert1[i], insertData.getTime(i));
				assertEquals(overflowInsert1[i], insertData.getInt(i));
			}
			assertEquals(false, fManager.closeAll());
			fManager.endQuery(deltaObjectId, token);
			fManager.closeAll();
		} catch (FileNodeManagerException e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
		createOverflowInserts(overflowInsert2, deltaObjectId);

		try {
			int token = fManager.beginQuery(deltaObjectId);
			QueryStructure queryResult = fManager.query(deltaObjectId, measurementId, null, null, null);
			List<Object> overflowResult = queryResult.getAllOverflowData();
			DynamicOneColumnData insertData = (DynamicOneColumnData) overflowResult.get(0);
			assertEquals(overflowInsert1.length + overflowInsert2.length, insertData.length);
			for (int i = 0; i < overflowInsert1.length; i++) {
				assertEquals(overflowInsert1[i], insertData.getTime(i));
				assertEquals(overflowInsert1[i], insertData.getInt(i));
			}
			for (int i = overflowInsert1.length; i < overflowInsert1.length + overflowInsert2.length; i++) {
				assertEquals(overflowInsert2[i - overflowInsert1.length], insertData.getTime(i));
				assertEquals(overflowInsert2[i - overflowInsert1.length], insertData.getInt(i));
			}
			fManager.endQuery(deltaObjectId, token);
			fManager.closeAll();
		} catch (FileNodeManagerException e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}

	@Test
	public void testOverflowUpdate() {

		// create bufferwrite data
		List<Pair<Long, Long>> pairList = new ArrayList<>();
		pairList.add(new Pair<Long, Long>(100L, 200L));
		pairList.add(new Pair<Long, Long>(300L, 400L));
		pairList.add(new Pair<Long, Long>(500L, 600L));
		pairList.add(new Pair<Long, Long>(700L, 800L));
		createBufferwriteFiles(pairList, deltaObjectId);

		// overflow update
		List<Pair<Long, Long>> overflowUpdate1 = new ArrayList<>();
		overflowUpdate1.add(new Pair<Long, Long>(150L, 170L));
		createOverflowUpdates(overflowUpdate1, deltaObjectId);

		try {
			int token = fManager.beginQuery(deltaObjectId);
			QueryStructure queryResult = fManager.query(deltaObjectId, measurementId, null, null, null);
			List<Object> overflowResult = queryResult.getAllOverflowData();
			DynamicOneColumnData updateData = (DynamicOneColumnData) overflowResult.get(1);
			assertEquals(1, updateData.length);
			assertEquals(2, updateData.timeLength);
			assertEquals(150, updateData.getTime(0));
			assertEquals(170, updateData.getTime(1));
			assertEquals((150 + 170) / 2, updateData.getInt(0));
			fManager.endQuery(deltaObjectId, token);
			fManager.closeAll();
		} catch (FileNodeManagerException e) {
			e.printStackTrace();
			fail(e.getMessage());
		}

	}

	@Test
	public void testOverflowDelete() {
		// create bufferwrite data
		List<Pair<Long, Long>> pairList = new ArrayList<>();
		pairList.add(new Pair<Long, Long>(100L, 200L));
		pairList.add(new Pair<Long, Long>(300L, 400L));
		pairList.add(new Pair<Long, Long>(500L, 600L));
		pairList.add(new Pair<Long, Long>(700L, 800L));
		createBufferwriteFiles(pairList, deltaObjectId);

		long overflowDelete1 = 50;

		fManager = FileNodeManager.getInstance();
		try {
			fManager.delete(deltaObjectId, measurementId, overflowDelete1, dataType);
			QueryStructure queryResult = fManager.query(deltaObjectId, measurementId, null, null, null);
			List<Object> overflowResult = queryResult.getAllOverflowData();
			SingleSeriesFilterExpression timeFilter = (SingleSeriesFilterExpression) overflowResult.get(3);
			System.out.println(timeFilter);
			fManager.closeAll();

		} catch (FileNodeManagerException e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}

	@Test
	public void testQuery() {

	}

	@Test
	public void testMergeAll() {

		List<Pair<Long, Long>> pairList = new ArrayList<>();
		pairList.add(new Pair<Long, Long>(100L, 200L));
		pairList.add(new Pair<Long, Long>(300L, 400L));
		pairList.add(new Pair<Long, Long>(500L, 600L));
		pairList.add(new Pair<Long, Long>(700L, 800L));
		createBufferwriteFiles(pairList, deltaObjectId);
		createBufferwriteFiles(pairList,deltaObjectId2);
		long[] overflowInsert1 = { 2, 4, 6, 8 };
		long[] overflowInsert2 = { 202, 204, 206, 208 };
		// new file: 2-208 300-400 500-600 700-800

		// not close
		createOverflowInserts(overflowInsert1, deltaObjectId);
		createOverflowInserts(overflowInsert1,deltaObjectId2);
		// not close
		createOverflowInserts(overflowInsert2, deltaObjectId);
		createOverflowInserts(overflowInsert2, deltaObjectId2);
		

		fManager = FileNodeManager.getInstance();
		try {
			fManager.mergeAll();
			int token = fManager.beginQuery(deltaObjectId);
			QueryStructure queryResult = fManager.query(deltaObjectId, measurementId, null, null, null);
			fManager.endQuery(deltaObjectId, token);
			DynamicOneColumnData bufferwriteindex = queryResult.getBufferwriteDataInMemory();
			assertEquals(null, bufferwriteindex);
			List<RowGroupMetaData> bufferwriteindisk = queryResult.getBufferwriteDataInDisk();
			assertEquals(null, bufferwriteindisk);
			List<IntervalFileNode> bufferwriteFiles = queryResult.getBufferwriteDataInFiles();
			assertEquals(pairList.size(), bufferwriteFiles.size());
			IntervalFileNode temp = bufferwriteFiles.get(0);
			// range 1: 2-208
			assertEquals(100, temp.startTime);
			assertEquals(200, temp.endTime);
			// range 2: 202-400
			temp = bufferwriteFiles.get(1);
			assertEquals(300, temp.startTime);
			assertEquals(400, temp.endTime);
			// range 3: 500-600
			temp = bufferwriteFiles.get(2);
			assertEquals(500, temp.startTime);
			assertEquals(600, temp.endTime);
			// range 4: 700-800
			temp = bufferwriteFiles.get(3);
			assertEquals(700, temp.startTime);
			assertEquals(800, temp.endTime);
			List<Object> overflowData = queryResult.getAllOverflowData();
			assertEquals(true, overflowData.get(0) != null);
			assertEquals(true, overflowData.get(1) != null);
			assertEquals(true, overflowData.get(2) != null);
			assertEquals(true, overflowData.get(3) != null);

			// wait to merge over
			waitToSleep(1000);
			token = fManager.beginQuery(deltaObjectId);
			queryResult = fManager.query(deltaObjectId, measurementId, null, null, null);
			fManager.endQuery(deltaObjectId, token);
			bufferwriteindex = queryResult.getBufferwriteDataInMemory();
			assertEquals(null, bufferwriteindex);
			bufferwriteindisk = queryResult.getBufferwriteDataInDisk();
			assertEquals(null, bufferwriteindisk);
			bufferwriteFiles = queryResult.getBufferwriteDataInFiles();
			assertEquals(pairList.size(), bufferwriteFiles.size());
			temp = bufferwriteFiles.get(0);
			// range 1: 2-208
			assertEquals(2, temp.startTime);
			assertEquals(208, temp.endTime);
			// range 2: 202-400
			temp = bufferwriteFiles.get(1);
			assertEquals(300, temp.startTime);
			assertEquals(400, temp.endTime);
			// range 3: 500-600
			temp = bufferwriteFiles.get(2);
			assertEquals(500, temp.startTime);
			assertEquals(600, temp.endTime);
			// range 4: 700-800
			temp = bufferwriteFiles.get(3);
			assertEquals(700, temp.startTime);
			assertEquals(800, temp.endTime);
			overflowData = queryResult.getAllOverflowData();
			assertEquals(null, overflowData.get(0));
			assertEquals(null, overflowData.get(1));
			assertEquals(null, overflowData.get(2));
			assertEquals(null, overflowData.get(3));

			fManager.closeAll();

		} catch (FileNodeManagerException e) {
			e.printStackTrace();
			fail(e.getMessage());
		} 
	}

	private void waitToSleep(long waitTime) {

		try {
			Thread.sleep(waitTime);
		} catch (InterruptedException e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}

	private void createBufferwriteFiles(List<Pair<Long, Long>> pairList, String deltaObjectId) {
		for (Pair<Long, Long> timePair : pairList) {
			createBufferwriteFile(timePair, deltaObjectId);
		}
	}

	private void createBufferwriteFile(Pair<Long, Long> timePair, String deltaObjectId) {

		long startTime = timePair.left;
		long endTime = timePair.right;
		// create bufferwrite file
		fManager = FileNodeManager.getInstance();
		for (long i = startTime; i <= endTime; i++) {
			TSRecord record = new TSRecord(i, deltaObjectId);
			DataPoint dataPoint = DataPoint.getDataPoint(dataType, measurementId, String.valueOf(i));
			record.addTuple(dataPoint);
			try {
				fManager.insert(record);
			} catch (FileNodeManagerException e) {
				e.printStackTrace();
				fail(e.getMessage());
			}
		}
		try {
			// close
			fManager.closeAll();
		} catch (FileNodeManagerException e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}

	private void createBufferwriteInMemory(Pair<Long, Long> timePair) {
		long startTime = timePair.left;
		long endTime = timePair.right;
		// create bufferwrite file
		fManager = FileNodeManager.getInstance();
		for (long i = startTime; i <= endTime; i++) {
			TSRecord record = new TSRecord(i, deltaObjectId);
			DataPoint dataPoint = DataPoint.getDataPoint(dataType, measurementId, String.valueOf(i));
			record.addTuple(dataPoint);
			try {
				fManager.insert(record);
			} catch (FileNodeManagerException e) {
				e.printStackTrace();
				fail(e.getMessage());
			}
		}
	}

	private void createOverflowInserts(long[] times, String deltaObjectId) {
		fManager = FileNodeManager.getInstance();
		for (long time : times) {
			TSRecord record = new TSRecord(time, deltaObjectId);
			DataPoint dataPoint = DataPoint.getDataPoint(dataType, measurementId, String.valueOf((int) time));
			record.addTuple(dataPoint);
			try {
				fManager.insert(record);
			} catch (FileNodeManagerException e) {
				e.printStackTrace();
				fail(e.getMessage());
			}
		}
	}

	private void createOverflowUpdates(List<Pair<Long, Long>> timePairs, String deltaObjectId) {
		fManager = FileNodeManager.getInstance();
		for (Pair<Long, Long> time : timePairs) {
			try {
				fManager.update(deltaObjectId, measurementId, time.left, time.right, dataType,
						String.valueOf((time.left + time.right) / 2));
			} catch (FileNodeManagerException e) {
				e.printStackTrace();
				fail(e.getMessage());
			}
		}
	}
}
