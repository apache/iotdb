package cn.edu.tsinghua.iotdb.engine.filenode;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.List;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import cn.edu.tsinghua.iotdb.engine.MetadataManagerHelper;
import cn.edu.tsinghua.iotdb.exception.FileNodeManagerException;
import cn.edu.tsinghua.iotdb.exception.PathErrorException;
import cn.edu.tsinghua.iotdb.utils.EnvironmentUtils;
import cn.edu.tsinghua.tsfile.common.conf.TSFileConfig;
import cn.edu.tsinghua.tsfile.common.conf.TSFileDescriptor;
import cn.edu.tsinghua.tsfile.file.metadata.enums.TSDataType;
import cn.edu.tsinghua.tsfile.timeseries.read.query.DynamicOneColumnData;
import cn.edu.tsinghua.tsfile.timeseries.write.record.DataPoint;
import cn.edu.tsinghua.tsfile.timeseries.write.record.TSRecord;

/**
 * @author liukun
 *
 */
public class FileNodeManagerMulTest {

	private TSFileConfig tsconfig = TSFileDescriptor.getInstance().getConfig();

	private String deltaObjectId0 = "root.vehicle.d0";
	private String deltaObjectId2 = "root.vehicle.d2";

	private String deltaObjectId1 = "root.vehicle.d1";

	private String measurementId = "s0";

	private TSDataType dataType = TSDataType.INT32;

	private int rowGroupSize;
	private int pageCheckSizeThreshold;
	private int defaultMaxStringLength;
	private boolean cachePageData;
	private int pageSize;

	@Before
	public void setUp() throws Exception {
		EnvironmentUtils.closeStatMonitor();
		pageCheckSizeThreshold = tsconfig.pageCheckSizeThreshold;
		defaultMaxStringLength = tsconfig.maxStringLength;
		cachePageData = tsconfig.duplicateIncompletedPage;
		pageSize = tsconfig.pageSizeInByte;
		rowGroupSize = tsconfig.groupSizeInByte;
		// set rowgroupsize
		tsconfig.groupSizeInByte = 10000;
		tsconfig.pageCheckSizeThreshold = 3;
		tsconfig.pageSizeInByte = 100;
		tsconfig.maxStringLength = 2;
		tsconfig.duplicateIncompletedPage = true;
		MetadataManagerHelper.initMetadata2();
		EnvironmentUtils.envSetUp();
	}

	@After
	public void tearDown() throws Exception {
		EnvironmentUtils.cleanEnv();
		tsconfig.groupSizeInByte = rowGroupSize;
		tsconfig.pageCheckSizeThreshold = pageCheckSizeThreshold;
		tsconfig.pageSizeInByte = pageSize;
		tsconfig.maxStringLength = defaultMaxStringLength;
		tsconfig.duplicateIncompletedPage = cachePageData;
	}

	@Test
	public void testBufferAndQuery() {

		// write buffer file
		// file 0: d0[10,20]
		createBufferwriteFile(10, 20, deltaObjectId0);
		closeBufferwrite();
		// file 1: d1[10,20]
		createBufferwriteFile(10, 20, deltaObjectId1);
		closeBufferwrite();
		// file 2: d2[10,20]
		createBufferwriteFile(10, 20, deltaObjectId2);
		closeBufferwrite();
		// file 3: d0,d1[30,40]
		createBufferwriteFile(30, 40, deltaObjectId0, deltaObjectId1);
		closeBufferwrite();
		// file 4: d0,d1,d2[50,60]
		createBufferwriteFile(50, 60, deltaObjectId0, deltaObjectId1, deltaObjectId2);
		closeBufferwrite();
		// file 5: d0,d1,d2[70,..80)
		createBufferwriteFile(70, 80, deltaObjectId0, deltaObjectId1, deltaObjectId2);

		FileNodeManager fileNodeManager = FileNodeManager.getInstance();

		try {
			int token = fileNodeManager.beginQuery(deltaObjectId0);
			QueryStructure queryStructure = fileNodeManager.query(deltaObjectId0, measurementId, null, null, null);
			fileNodeManager.endQuery(deltaObjectId0, token);
			// assert query result
			DynamicOneColumnData pagecache = queryStructure.getCurrentPage();
			assertEquals(1, queryStructure.getPageList().left.size());
			for (ByteArrayInputStream stream : queryStructure.getPageList().left) {
				DynamicOneColumnData old = PageTestUtils.pageToDynamic(stream, queryStructure.getPageList().right,
						deltaObjectId0, measurementId);
				old.mergeRecord(pagecache);
				pagecache = old;
			}
			for (int i = 70; i <= 80; i++) {
				assertEquals(i, pagecache.getTime(i - 70));
				assertEquals(i, pagecache.getInt(i - 70));
			}
			assertEquals(6, queryStructure.getBufferwriteDataInFiles().size());
			List<IntervalFileNode> bufferwriteDataInFiles = queryStructure.getBufferwriteDataInFiles();
			IntervalFileNode temp = bufferwriteDataInFiles.get(0);
			assertEquals(1, temp.getStartTimeMap().size());
			for (String d : temp.getStartTimeMap().keySet()) {
				assertEquals(deltaObjectId0, d);
				assertEquals(10, temp.getStartTime(deltaObjectId0));
			}
			assertEquals(1, temp.getEndTimeMap().size());
			for (String d : temp.getEndTimeMap().keySet()) {
				assertEquals(deltaObjectId0, d);
				assertEquals(20, temp.getEndTime(deltaObjectId0));
			}
			assertEquals(OverflowChangeType.NO_CHANGE, temp.overflowChangeType);
			// second file
			temp = bufferwriteDataInFiles.get(1);
			assertEquals(1, temp.getStartTimeMap().size());
			for (String d : temp.getStartTimeMap().keySet()) {
				assertEquals(deltaObjectId1, d);
				assertEquals(10, temp.getStartTime(deltaObjectId1));
			}
			assertEquals(1, temp.getEndTimeMap().size());
			for (String d : temp.getEndTimeMap().keySet()) {
				assertEquals(deltaObjectId1, d);
				assertEquals(20, temp.getEndTime(deltaObjectId1));
			}
			assertEquals(OverflowChangeType.NO_CHANGE, temp.overflowChangeType);
			// third file
			temp = bufferwriteDataInFiles.get(2);
			assertEquals(1, temp.getStartTimeMap().size());
			for (String d : temp.getStartTimeMap().keySet()) {
				assertEquals(deltaObjectId2, d);
				assertEquals(10, temp.getStartTime(deltaObjectId2));
			}
			assertEquals(1, temp.getEndTimeMap().size());
			for (String d : temp.getEndTimeMap().keySet()) {
				assertEquals(deltaObjectId2, d);
				assertEquals(20, temp.getEndTime(deltaObjectId2));
			}
			assertEquals(OverflowChangeType.NO_CHANGE, temp.overflowChangeType);
			// fourth file
			temp = bufferwriteDataInFiles.get(3);
			assertEquals(2, temp.getStartTimeMap().size());
			assertEquals(2, temp.getEndTimeMap().size());
			assertEquals(30, temp.getStartTime(deltaObjectId0));
			assertEquals(30, temp.getStartTime(deltaObjectId1));
			assertEquals(40, temp.getEndTime(deltaObjectId0));
			assertEquals(40, temp.getEndTime(deltaObjectId1));
			assertEquals(OverflowChangeType.NO_CHANGE, temp.overflowChangeType);
			// fifth file
			temp = bufferwriteDataInFiles.get(4);
			assertEquals(3, temp.getStartTimeMap().size());
			assertEquals(3, temp.getEndTimeMap().size());
			assertEquals(50, temp.getStartTime(deltaObjectId0));
			assertEquals(50, temp.getStartTime(deltaObjectId1));
			assertEquals(50, temp.getStartTime(deltaObjectId2));
			assertEquals(60, temp.getEndTime(deltaObjectId0));
			assertEquals(60, temp.getEndTime(deltaObjectId1));
			assertEquals(60, temp.getEndTime(deltaObjectId2));
			assertEquals(OverflowChangeType.NO_CHANGE, temp.overflowChangeType);
			// sixth
			temp = bufferwriteDataInFiles.get(5);
			assertEquals(3, temp.getStartTimeMap().size());
			assertEquals(0, temp.getEndTimeMap().size());
			assertEquals(70, temp.getStartTime(deltaObjectId0));
			assertEquals(70, temp.getStartTime(deltaObjectId1));
			assertEquals(70, temp.getStartTime(deltaObjectId2));
			assertEquals(-1, temp.getEndTime(deltaObjectId0));
			assertEquals(-1, temp.getEndTime(deltaObjectId1));
			assertEquals(-1, temp.getEndTime(deltaObjectId2));
			assertEquals(OverflowChangeType.NO_CHANGE, temp.overflowChangeType);

			assertEquals(true, fileNodeManager.closeAll());
		} catch (FileNodeManagerException e) {
			e.printStackTrace();
			fail(e.getMessage());
		} catch (PathErrorException e) {
			e.printStackTrace();
			fail(e.getMessage());
		} catch (IOException e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}

	@Test
	public void testOverflowInsertQuery() {
		testBufferAndQuery();
		FileNodeManager fileNodeManager = FileNodeManager.getInstance();
		// oveflow data

		// insert d0 time = 5
		TSRecord record = new TSRecord(5, deltaObjectId0);
		DataPoint dataPoint = DataPoint.getDataPoint(dataType, measurementId, String.valueOf(5));
		record.addTuple(dataPoint);
		try {
			fileNodeManager.insert(record, false);
		} catch (FileNodeManagerException e) {
			fail(e.getMessage());
			e.printStackTrace();
		}
		// insert d2 time = 5
		record = new TSRecord(5, deltaObjectId2);
		record.addTuple(dataPoint);
		try {
			fileNodeManager.insert(record, false);
		} catch (FileNodeManagerException e) {
			e.printStackTrace();
			fail(e.getMessage());
		}

		// insert d1 time = 65
		record = new TSRecord(65, deltaObjectId1);
		record.addTuple(dataPoint);
		try {
			fileNodeManager.insert(record, false);
		} catch (FileNodeManagerException e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
		// query
		try {
			int token = fileNodeManager.beginQuery(deltaObjectId0);
			QueryStructure queryStructure = fileNodeManager.query(deltaObjectId0, measurementId, null, null, null);
			fileNodeManager.endQuery(deltaObjectId0, token);
			DynamicOneColumnData insert = (DynamicOneColumnData) queryStructure.getAllOverflowData().get(0);
			assertEquals(1, insert.valueLength);
			assertEquals(5, insert.getTime(0));
			assertEquals(5, insert.getInt(0));

			assertEquals(6, queryStructure.getBufferwriteDataInFiles().size());
			List<IntervalFileNode> bufferwriteDataInFiles = queryStructure.getBufferwriteDataInFiles();
			IntervalFileNode temp = bufferwriteDataInFiles.get(0);
			assertEquals(1, temp.getStartTimeMap().size());
			for (String d : temp.getStartTimeMap().keySet()) {
				assertEquals(deltaObjectId0, d);
				assertEquals(10, temp.getStartTime(deltaObjectId0));
			}
			assertEquals(1, temp.getEndTimeMap().size());
			for (String d : temp.getEndTimeMap().keySet()) {
				assertEquals(deltaObjectId0, d);
				assertEquals(20, temp.getEndTime(deltaObjectId0));
			}
			assertEquals(OverflowChangeType.CHANGED, temp.overflowChangeType);
			// second file
			temp = bufferwriteDataInFiles.get(1);
			assertEquals(1, temp.getStartTimeMap().size());
			for (String d : temp.getStartTimeMap().keySet()) {
				assertEquals(deltaObjectId1, d);
				assertEquals(10, temp.getStartTime(deltaObjectId1));
			}
			assertEquals(1, temp.getEndTimeMap().size());
			for (String d : temp.getEndTimeMap().keySet()) {
				assertEquals(deltaObjectId1, d);
				assertEquals(20, temp.getEndTime(deltaObjectId1));
			}
			assertEquals(OverflowChangeType.NO_CHANGE, temp.overflowChangeType);
			// third file
			temp = bufferwriteDataInFiles.get(2);
			assertEquals(1, temp.getStartTimeMap().size());
			for (String d : temp.getStartTimeMap().keySet()) {
				assertEquals(deltaObjectId2, d);
				assertEquals(10, temp.getStartTime(deltaObjectId2));
			}
			assertEquals(1, temp.getEndTimeMap().size());
			for (String d : temp.getEndTimeMap().keySet()) {
				assertEquals(deltaObjectId2, d);
				assertEquals(20, temp.getEndTime(deltaObjectId2));
			}
			assertEquals(OverflowChangeType.CHANGED, temp.overflowChangeType);
			// fourth file
			temp = bufferwriteDataInFiles.get(3);
			assertEquals(2, temp.getStartTimeMap().size());
			assertEquals(2, temp.getEndTimeMap().size());
			assertEquals(30, temp.getStartTime(deltaObjectId0));
			assertEquals(30, temp.getStartTime(deltaObjectId1));
			assertEquals(40, temp.getEndTime(deltaObjectId0));
			assertEquals(40, temp.getEndTime(deltaObjectId1));
			assertEquals(OverflowChangeType.NO_CHANGE, temp.overflowChangeType);
			// fifth file
			temp = bufferwriteDataInFiles.get(4);
			assertEquals(3, temp.getStartTimeMap().size());
			assertEquals(3, temp.getEndTimeMap().size());
			assertEquals(50, temp.getStartTime(deltaObjectId0));
			assertEquals(50, temp.getStartTime(deltaObjectId1));
			assertEquals(50, temp.getStartTime(deltaObjectId2));
			assertEquals(60, temp.getEndTime(deltaObjectId0));
			assertEquals(60, temp.getEndTime(deltaObjectId1));
			assertEquals(60, temp.getEndTime(deltaObjectId2));
			assertEquals(OverflowChangeType.CHANGED, temp.overflowChangeType);
			// sixth
			temp = bufferwriteDataInFiles.get(5);
			assertEquals(3, temp.getStartTimeMap().size());
			assertEquals(3, temp.getEndTimeMap().size());
			assertEquals(70, temp.getStartTime(deltaObjectId0));
			assertEquals(70, temp.getStartTime(deltaObjectId1));
			assertEquals(70, temp.getStartTime(deltaObjectId2));
			assertEquals(80, temp.getEndTime(deltaObjectId0));
			assertEquals(80, temp.getEndTime(deltaObjectId1));
			assertEquals(80, temp.getEndTime(deltaObjectId2));
			assertEquals(OverflowChangeType.NO_CHANGE, temp.overflowChangeType);

			assertEquals(true, fileNodeManager.closeAll());
		} catch (FileNodeManagerException e) {
			e.printStackTrace();
			fail(e.getMessage());
		}

	}

	/**
	 * No file when merge overflow
	 */
	@Test
	public void testOverflowEmptyAndMerge1() {

		FileNodeManager fileNodeManager = FileNodeManager.getInstance();
		createBufferwriteFile(100, 100, deltaObjectId0, deltaObjectId1, deltaObjectId2);
		closeBufferwrite();
		// query check
		try {
			QueryStructure queryStructure = fileNodeManager.query(deltaObjectId0, measurementId, null, null, null);
			assertEquals(1, queryStructure.getBufferwriteDataInFiles().size());
			IntervalFileNode temp = queryStructure.getBufferwriteDataInFiles().get(0);
			assertEquals(3, temp.getStartTimeMap().size());
			assertEquals(3, temp.getEndTimeMap().size());
			assertEquals(100, temp.getStartTime(deltaObjectId0));
			assertEquals(100, temp.getEndTime(deltaObjectId0));
			assertEquals(100, temp.getStartTime(deltaObjectId1));
			assertEquals(100, temp.getEndTime(deltaObjectId1));
			assertEquals(100, temp.getStartTime(deltaObjectId2));
			assertEquals(100, temp.getEndTime(deltaObjectId2));
			fileNodeManager.closeAll();
		} catch (FileNodeManagerException e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
		// delete
		try {
			fileNodeManager.delete(deltaObjectId0, measurementId, 101, dataType);
			fileNodeManager.delete(deltaObjectId1, measurementId, 101, dataType);
			fileNodeManager.delete(deltaObjectId2, measurementId, 101, dataType);
		} catch (FileNodeManagerException e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
		// merge
		try {
			fileNodeManager.mergeAll();
			while (!fileNodeManager.closeAll()) {
				Thread.sleep(1000);
			}
		} catch (FileNodeManagerException e) {
			e.printStackTrace();
			fail(e.getMessage());
		} catch (InterruptedException e) {
			e.printStackTrace();
			fail(e.getMessage());
		}

		try {
			Thread.sleep(2000);
		} catch (InterruptedException e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
		// query check
		try {
			QueryStructure queryStructure = fileNodeManager.query(deltaObjectId0, measurementId, null, null, null);
			assertEquals(0, queryStructure.getBufferwriteDataInFiles().size());
			assertEquals(null, queryStructure.getBufferwriteDataInDisk());
		} catch (FileNodeManagerException e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
		// overflow data
		TSRecord record = new TSRecord(5, deltaObjectId0);
		DataPoint dataPoint = DataPoint.getDataPoint(dataType, measurementId, String.valueOf(5));
		record.addTuple(dataPoint);
		try {
			fileNodeManager.insert(record, false);
			record = new TSRecord(5, deltaObjectId1);
			record.addTuple(dataPoint);
			fileNodeManager.insert(record, false);
			record = new TSRecord(10, deltaObjectId2);
			record.addTuple(dataPoint);
			fileNodeManager.insert(record, false);
			// query check
			QueryStructure queryStructure = fileNodeManager.query(deltaObjectId2, measurementId, null, null, null);
			DynamicOneColumnData insert = (DynamicOneColumnData) queryStructure.getAllOverflowData().get(0);
			assertEquals(1, insert.valueLength);
			assertEquals(10, insert.getTime(0));
			assertEquals(5, insert.getInt(0));
		} catch (FileNodeManagerException e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
		// merge
		try {
			fileNodeManager.mergeAll();
			Thread.sleep(5000);
			while (!fileNodeManager.closeAll()) {
				Thread.sleep(1000);
			}
			QueryStructure queryStructure = fileNodeManager.query(deltaObjectId0, measurementId, null, null, null);
			assertEquals(1, queryStructure.getBufferwriteDataInFiles().size());
			IntervalFileNode temp = queryStructure.getBufferwriteDataInFiles().get(0);
			assertEquals(5, temp.getStartTime(deltaObjectId0));
			assertEquals(5, temp.getEndTime(deltaObjectId0));
			assertEquals(5, temp.getStartTime(deltaObjectId1));
			assertEquals(5, temp.getEndTime(deltaObjectId1));
			assertEquals(10, temp.getStartTime(deltaObjectId2));
			assertEquals(10, temp.getEndTime(deltaObjectId2));
			assertEquals(true, fileNodeManager.closeAll());
		} catch (FileNodeManagerException e) {
			e.printStackTrace();
			fail(e.getMessage());
		} catch (InterruptedException e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}

	@Test
	public void testOverflowEmptyAndMerge2() {

		FileNodeManager fileNodeManager = FileNodeManager.getInstance();
		createBufferwriteFile(100, 100, deltaObjectId0, deltaObjectId1, deltaObjectId2);
		closeBufferwrite();
		// delete
		try {
			fileNodeManager.delete(deltaObjectId0, measurementId, 101, dataType);
			fileNodeManager.delete(deltaObjectId1, measurementId, 101, dataType);
			fileNodeManager.delete(deltaObjectId2, measurementId, 101, dataType);
		} catch (FileNodeManagerException e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
		// merge
		try {
			fileNodeManager.mergeAll();
			while (!fileNodeManager.closeAll()) {
				Thread.sleep(1000);
			}
		} catch (FileNodeManagerException e) {
			e.printStackTrace();
			fail(e.getMessage());
		} catch (InterruptedException e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
		try {
			Thread.sleep(2000);
		} catch (InterruptedException e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
		// overflow data
		TSRecord record = new TSRecord(5, deltaObjectId0);
		DataPoint dataPoint = DataPoint.getDataPoint(dataType, measurementId, String.valueOf(5));
		record.addTuple(dataPoint);
		try {
			fileNodeManager.insert(record, false);
			record = new TSRecord(5, deltaObjectId1);
			record.addTuple(dataPoint);
			fileNodeManager.insert(record, false);
			record = new TSRecord(10, deltaObjectId2);
			record.addTuple(dataPoint);
			fileNodeManager.insert(record, false);
			// query check
			QueryStructure queryStructure = fileNodeManager.query(deltaObjectId2, measurementId, null, null, null);
			DynamicOneColumnData insert = (DynamicOneColumnData) queryStructure.getAllOverflowData().get(0);
			assertEquals(1, insert.valueLength);
			assertEquals(10, insert.getTime(0));
			assertEquals(5, insert.getInt(0));
		} catch (FileNodeManagerException e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
		// buffer data
		record = new TSRecord(20, deltaObjectId2);
		record.addTuple(dataPoint);
		try {
			fileNodeManager.insert(record, false);
		} catch (FileNodeManagerException e1) {
			e1.printStackTrace();
			fail(e1.getMessage());
		}
		// merge
		try {
			fileNodeManager.mergeAll();
			while (!fileNodeManager.closeAll()) {
				Thread.sleep(3000);
			}
			QueryStructure queryStructure = fileNodeManager.query(deltaObjectId0, measurementId, null, null, null);
			assertEquals(1, queryStructure.getBufferwriteDataInFiles().size());
			IntervalFileNode temp = queryStructure.getBufferwriteDataInFiles().get(0);
			assertEquals(5, temp.getStartTime(deltaObjectId0));
			assertEquals(5, temp.getEndTime(deltaObjectId0));
			assertEquals(5, temp.getStartTime(deltaObjectId1));
			assertEquals(5, temp.getEndTime(deltaObjectId1));
			assertEquals(10, temp.getStartTime(deltaObjectId2));
			assertEquals(20, temp.getEndTime(deltaObjectId2));
			assertEquals(true, fileNodeManager.closeAll());
		} catch (FileNodeManagerException e) {
			e.printStackTrace();
			fail(e.getMessage());
		} catch (InterruptedException e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}

	@Deprecated
	public void testFileEmptyMergeAnaWriteDebug() {
		createBufferwriteFile(10, 20, deltaObjectId0);
		closeBufferwrite();
		createBufferwriteFile(10, 20, deltaObjectId1);
		closeBufferwrite();
		createBufferwriteFile(100, 100, deltaObjectId2);
		FileNodeManager fileNodeManager = FileNodeManager.getInstance();
		try {
			fileNodeManager.delete(deltaObjectId2, measurementId, 101, dataType);
		} catch (FileNodeManagerException e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
		try {
			fileNodeManager.mergeAll();
		} catch (FileNodeManagerException e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
		try {
			while (!fileNodeManager.closeAll()) {
				System.out.println("wait to merge");
				Thread.sleep(1000);
			}
		} catch (FileNodeManagerException | InterruptedException e1) {
			e1.printStackTrace();
			fail(e1.getMessage());
		}
		try {
			QueryStructure queryStructure = fileNodeManager.query(deltaObjectId0, measurementId, null, null, null);
			if (queryStructure.getBufferwriteDataInFiles().size() == 2) {
				assertEquals(2, queryStructure.getBufferwriteDataInFiles().size());
				IntervalFileNode temp = queryStructure.getBufferwriteDataInFiles().get(0);
				assertEquals(10, temp.getStartTime(deltaObjectId0));
				assertEquals(20, temp.getEndTime(deltaObjectId0));
				temp = queryStructure.getBufferwriteDataInFiles().get(1);
				assertEquals(10, temp.getStartTime(deltaObjectId1));
				assertEquals(20, temp.getEndTime(deltaObjectId1));
			} else {
				assertEquals(3, queryStructure.getBufferwriteDataInFiles().size());
				IntervalFileNode temp = queryStructure.getBufferwriteDataInFiles().get(0);
				assertEquals(1, temp.getStartTimeMap().size());
				assertEquals(10, temp.getStartTime(deltaObjectId0));
				assertEquals(20, temp.getEndTime(deltaObjectId0));
				temp = queryStructure.getBufferwriteDataInFiles().get(1);
				assertEquals(1, temp.getStartTimeMap().size());
				assertEquals(10, temp.getStartTime(deltaObjectId1));
				assertEquals(20, temp.getEndTime(deltaObjectId1));
				temp = queryStructure.getBufferwriteDataInFiles().get(2);
				assertEquals(1, temp.getStartTimeMap().size());
				assertEquals(200, temp.getStartTime(deltaObjectId2));
				assertEquals(-1, temp.getEndTime(deltaObjectId2));
			}
		} catch (FileNodeManagerException e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
		// add empty overflow data
		TSRecord record = new TSRecord(5, deltaObjectId2);
		DataPoint dataPoint = DataPoint.getDataPoint(dataType, measurementId, String.valueOf(5));
		record.addTuple(dataPoint);
		try {
			fileNodeManager.insert(record, false);
		} catch (FileNodeManagerException e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
		record = new TSRecord(200, deltaObjectId2);
		record.addTuple(dataPoint);
		TSRecord overflow = new TSRecord(2, deltaObjectId2);
		overflow.addTuple(dataPoint);
		// merge data
		try {
			fileNodeManager.mergeAll();
			fileNodeManager.insert(record, false);
			fileNodeManager.insert(overflow, false);
			// wait end of merge
			while (!fileNodeManager.closeAll()) {
				System.out.println("wait to merge end, 1000ms...");
				Thread.sleep(1000);
			}
			QueryStructure queryStructure = fileNodeManager.query(deltaObjectId2, measurementId, null, null, null);
			if (queryStructure.getBufferwriteDataInFiles().size() == 4) {
				if (queryStructure.getBufferwriteDataInFiles()
						.get(1).overflowChangeType == OverflowChangeType.CHANGED) {
					assertEquals(OverflowChangeType.NO_CHANGE,
							queryStructure.getBufferwriteDataInFiles().get(2).overflowChangeType);
				} else {
					fail("error");
				}
			} else if (queryStructure.getBufferwriteDataInFiles().size() == 3) {
				IntervalFileNode temp = queryStructure.getBufferwriteDataInFiles().get(2);
				if (temp.overflowChangeType == OverflowChangeType.NO_CHANGE) {
					assertEquals(2, temp.getStartTime(deltaObjectId2));
					assertEquals(200, temp.getEndTime(deltaObjectId2));
				} else {
					assertEquals(2, temp.getStartTime(deltaObjectId2));
					assertEquals(200, temp.getEndTime(deltaObjectId2));
					DynamicOneColumnData insert = (DynamicOneColumnData) queryStructure.getAllOverflowData().get(0 + 2);
					assertEquals(true, insert != null);
					assertEquals(1, insert.valueLength);
					assertEquals(2, insert.getTime(0));
				}
			} else {
				System.out.println(queryStructure);
				fail("Error");
			}
			fileNodeManager.mergeAll();
			while (!fileNodeManager.closeAll()) {
				System.out.println("Wait to merge.....");
				Thread.sleep(1000);
			}
			queryStructure = fileNodeManager.query(deltaObjectId2, measurementId, null, null, null);
			assertEquals(null, queryStructure.getAllOverflowData().get(0));
			if (queryStructure.getBufferwriteDataInFiles().size() == 4) {
				IntervalFileNode temp = queryStructure.getBufferwriteDataInFiles().get(2);
				assertEquals(2, temp.getStartTime(deltaObjectId2));
				assertEquals(5, temp.getEndTime(deltaObjectId2));

				temp = queryStructure.getBufferwriteDataInFiles().get(3);
				assertEquals(200, temp.getStartTime(deltaObjectId2));
				assertEquals(200, temp.getEndTime(deltaObjectId2));
			} else if (queryStructure.getBufferwriteDataInFiles().size() == 3) {
				IntervalFileNode temp = queryStructure.getBufferwriteDataInFiles().get(2);
				assertEquals(1, temp.getStartTimeMap().size());
				assertEquals(2, temp.getStartTime(deltaObjectId2));
				assertEquals(200, temp.getEndTime(deltaObjectId2));
			} else {
				fail("Error");
			}
			fileNodeManager.closeAll();
		} catch (FileNodeManagerException e) {
			e.printStackTrace();
			fail(e.getMessage());
		} catch (InterruptedException e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}

	@Test
	public void testFileEmptyMergeAnaWrite() {
		createBufferwriteFile(10, 20, deltaObjectId0);
		closeBufferwrite();
		createBufferwriteFile(10, 20, deltaObjectId1);
		closeBufferwrite();
		createBufferwriteFile(100, 100, deltaObjectId2);
		FileNodeManager fileNodeManager = FileNodeManager.getInstance();
		try {
			fileNodeManager.delete(deltaObjectId2, measurementId, 101, dataType);
		} catch (FileNodeManagerException e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
		try {
			fileNodeManager.mergeAll();
		} catch (FileNodeManagerException e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
		try {
			while (!fileNodeManager.closeAll()) {
				System.out.println("wait to merge");
				Thread.sleep(1000);
			}
		} catch (FileNodeManagerException | InterruptedException e1) {
			e1.printStackTrace();
			fail(e1.getMessage());
		}
		try {
			QueryStructure queryStructure = fileNodeManager.query(deltaObjectId0, measurementId, null, null, null);
			if (queryStructure.getBufferwriteDataInFiles().size() == 2) {
				assertEquals(2, queryStructure.getBufferwriteDataInFiles().size());
				IntervalFileNode temp = queryStructure.getBufferwriteDataInFiles().get(0);
				assertEquals(10, temp.getStartTime(deltaObjectId0));
				assertEquals(20, temp.getEndTime(deltaObjectId0));
				temp = queryStructure.getBufferwriteDataInFiles().get(1);
				assertEquals(10, temp.getStartTime(deltaObjectId1));
				assertEquals(20, temp.getEndTime(deltaObjectId1));
			} else {
				assertEquals(3, queryStructure.getBufferwriteDataInFiles().size());
				IntervalFileNode temp = queryStructure.getBufferwriteDataInFiles().get(0);
				assertEquals(1, temp.getStartTimeMap().size());
				assertEquals(10, temp.getStartTime(deltaObjectId0));
				assertEquals(20, temp.getEndTime(deltaObjectId0));
				temp = queryStructure.getBufferwriteDataInFiles().get(1);
				assertEquals(1, temp.getStartTimeMap().size());
				assertEquals(10, temp.getStartTime(deltaObjectId1));
				assertEquals(20, temp.getEndTime(deltaObjectId1));
				temp = queryStructure.getBufferwriteDataInFiles().get(2);
				assertEquals(1, temp.getStartTimeMap().size());
				assertEquals(200, temp.getStartTime(deltaObjectId2));
				assertEquals(-1, temp.getEndTime(deltaObjectId2));
			}
		} catch (FileNodeManagerException e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
		// add empty overflow data
		TSRecord record = new TSRecord(5, deltaObjectId2);
		DataPoint dataPoint = DataPoint.getDataPoint(dataType, measurementId, String.valueOf(5));
		record.addTuple(dataPoint);
		try {
			fileNodeManager.insert(record, false);
		} catch (FileNodeManagerException e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
		record = new TSRecord(200, deltaObjectId2);
		record.addTuple(dataPoint);
		TSRecord overflow = new TSRecord(2, deltaObjectId2);
		overflow.addTuple(dataPoint);
		// merge data
		try {
			fileNodeManager.mergeAll();
			// wait end of merge
			while (!fileNodeManager.closeAll()) {
				System.out.println("wait to merge end, 1000ms...");
				Thread.sleep(1000);
			}
			fileNodeManager.insert(record, false);
			fileNodeManager.insert(overflow, false);
			fileNodeManager.mergeAll();
			// wait end of merge
			while (!fileNodeManager.closeAll()) {
				System.out.println("wait to merge end, 1000ms...");
				Thread.sleep(1000);
			}
			QueryStructure queryStructure = fileNodeManager.query(deltaObjectId2, measurementId, null, null, null);
			if (queryStructure.getBufferwriteDataInFiles().size() == 3) {
				IntervalFileNode temp = queryStructure.getBufferwriteDataInFiles().get(2);
				if (temp.overflowChangeType == OverflowChangeType.NO_CHANGE) {
					assertEquals(200, temp.getStartTime(deltaObjectId2));
					assertEquals(200, temp.getEndTime(deltaObjectId2));
				} else {
					fail("Error");
				}
				temp = queryStructure.getBufferwriteDataInFiles().get(0);
				assertEquals(2, temp.getStartTime(deltaObjectId2));
				assertEquals(5, temp.getEndTime(deltaObjectId2));
			} else {
				System.out.println(queryStructure);
				fail("Error");
			}
			fileNodeManager.mergeAll();
			while (!fileNodeManager.closeAll()) {
				System.out.println("Wait to merge.....");
				Thread.sleep(1000);
			}
			queryStructure = fileNodeManager.query(deltaObjectId2, measurementId, null, null, null);
			assertEquals(null, queryStructure.getAllOverflowData().get(0));
			if (queryStructure.getBufferwriteDataInFiles().size() == 4) {
				IntervalFileNode temp = queryStructure.getBufferwriteDataInFiles().get(2);
				assertEquals(2, temp.getStartTime(deltaObjectId2));
				assertEquals(5, temp.getEndTime(deltaObjectId2));

				temp = queryStructure.getBufferwriteDataInFiles().get(3);
				assertEquals(200, temp.getStartTime(deltaObjectId2));
				assertEquals(200, temp.getEndTime(deltaObjectId2));
			} else if (queryStructure.getBufferwriteDataInFiles().size() == 3) {
				IntervalFileNode temp = queryStructure.getBufferwriteDataInFiles().get(2);
				assertEquals(1, temp.getStartTimeMap().size());
				assertEquals(200, temp.getStartTime(deltaObjectId2));
				assertEquals(200, temp.getEndTime(deltaObjectId2));
			} else {
				fail("Error");
			}
			assertEquals(true, fileNodeManager.closeAll());
		} catch (FileNodeManagerException e) {
			e.printStackTrace();
			fail(e.getMessage());
		} catch (InterruptedException e) {
			e.printStackTrace();
			fail(e.getMessage());
		}

	}

	@Test
	public void testWriteDeleteAndMerge() {
		// write tsfile
		testBufferAndQuery();
		FileNodeManager fileNodeManager = FileNodeManager.getInstance();
		try {
			fileNodeManager.delete(deltaObjectId2, measurementId, 25, dataType);
		} catch (FileNodeManagerException e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
		// merge
		try {
			Thread thread = new Thread(new Runnable() {
				@Override
				public void run() {
					FileNodeManager fileNodeManager = FileNodeManager.getInstance();
					int token = 0;
					try {
						token = fileNodeManager.beginQuery(deltaObjectId0);
						QueryStructure queryStructure = fileNodeManager.query(deltaObjectId0, measurementId, null, null,
								null);
						assertEquals(null, queryStructure.getPageList());
						assertEquals(null, queryStructure.getCurrentPage());
						assertEquals(null, queryStructure.getBufferwriteDataInDisk());
						assertEquals(6, queryStructure.getBufferwriteDataInFiles().size());
						assertEquals(OverflowChangeType.CHANGED,
								queryStructure.getBufferwriteDataInFiles().get(2).overflowChangeType);

					} catch (FileNodeManagerException e) {
						e.printStackTrace();
						fail(e.getMessage());
					} finally {
						try {
							fileNodeManager.endQuery(deltaObjectId0, token);
						} catch (FileNodeManagerException e) {
							e.printStackTrace();
							fail(e.getMessage());
						}
					}
				}
			});
			thread.start();
			// wait the thread start before the merge
			Thread.sleep(100);
			fileNodeManager.mergeAll();
			while (!fileNodeManager.closeAll()) {
				System.out.println("Wait to merge over");
				Thread.sleep(1000);
			}
			QueryStructure queryStructure = fileNodeManager.query(deltaObjectId0, measurementId, null, null, null);
			assertEquals(5, queryStructure.getBufferwriteDataInFiles().size());
			assertEquals(OverflowChangeType.NO_CHANGE,
					queryStructure.getBufferwriteDataInFiles().get(3).overflowChangeType);
			assertEquals(true, fileNodeManager.closeAll());
		} catch (FileNodeManagerException e) {
			e.printStackTrace();
			fail(e.getMessage());
		} catch (InterruptedException e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}

	@Deprecated
	public void testWriteDeleteAndMergeDebug() {
		// write tsfile
		testBufferAndQuery();
		FileNodeManager fileNodeManager = FileNodeManager.getInstance();
		try {
			fileNodeManager.delete(deltaObjectId2, measurementId, 25, dataType);
		} catch (FileNodeManagerException e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
		// merge
		try {
			Thread thread = new Thread(new Runnable() {
				@Override
				public void run() {
					FileNodeManager fileNodeManager = FileNodeManager.getInstance();
					int token = 0;
					try {
						token = fileNodeManager.beginQuery(deltaObjectId0);
						QueryStructure queryStructure = fileNodeManager.query(deltaObjectId0, measurementId, null, null,
								null);
						assertEquals(null, queryStructure.getPageList());
						assertEquals(null, queryStructure.getCurrentPage());
						assertEquals(null, queryStructure.getBufferwriteDataInDisk());
						assertEquals(6, queryStructure.getBufferwriteDataInFiles().size());
						assertEquals(OverflowChangeType.CHANGED,
								queryStructure.getBufferwriteDataInFiles().get(2).overflowChangeType);
						// insert when merge
						TSRecord record = new TSRecord(15, deltaObjectId0);
						DataPoint dataPoint = DataPoint.getDataPoint(dataType, measurementId, String.valueOf(15));
						record.addTuple(dataPoint);
						fileNodeManager.insert(record, false);
						queryStructure = fileNodeManager.query(deltaObjectId0, measurementId, null, null, null);
						assertEquals(6, queryStructure.getBufferwriteDataInFiles().size());
						assertEquals(OverflowChangeType.MERGING_CHANGE,
								queryStructure.getBufferwriteDataInFiles().get(2));
					} catch (FileNodeManagerException e) {
						e.printStackTrace();
						fail(e.getMessage());
					} finally {
						try {
							fileNodeManager.endQuery(deltaObjectId0, token);
						} catch (FileNodeManagerException e) {
							e.printStackTrace();
							fail(e.getMessage());
						}
					}
				}
			});
			thread.start();
			// wait the thread start before the merge
			Thread.sleep(100);
			fileNodeManager.mergeAll();
			while (!fileNodeManager.closeAll()) {
				System.out.println("Wait to merge over");
				Thread.sleep(1000);
			}
			QueryStructure queryStructure = fileNodeManager.query(deltaObjectId0, measurementId, null, null, null);
			assertEquals(5, queryStructure.getBufferwriteDataInFiles().size());
			assertEquals(OverflowChangeType.NO_CHANGE,
					queryStructure.getBufferwriteDataInFiles().get(3).overflowChangeType);
			fileNodeManager.closeAll();
		} catch (FileNodeManagerException e) {
			e.printStackTrace();
			fail(e.getMessage());
		} catch (InterruptedException e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}

	private void createBufferwriteFile(long begin, long end, String... deltaObjects) {
		FileNodeManager fileNodeManager = FileNodeManager.getInstance();
		for (String deltaObjectId : deltaObjects) {
			for (long i = begin; i <= end; i++) {
				TSRecord record = new TSRecord(i, deltaObjectId);
				DataPoint dataPoint = DataPoint.getDataPoint(dataType, measurementId, String.valueOf(i));
				record.addTuple(dataPoint);
				try {
					fileNodeManager.insert(record, false);
				} catch (FileNodeManagerException e) {
					e.printStackTrace();
					fail(e.getMessage());
				}
			}
		}
	}

	private void closeBufferwrite() {
		FileNodeManager fileNodeManager = FileNodeManager.getInstance();
		try {
			fileNodeManager.closeAll();
		} catch (FileNodeManagerException e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}
}
