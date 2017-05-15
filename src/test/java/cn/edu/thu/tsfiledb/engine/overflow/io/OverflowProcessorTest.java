package cn.edu.thu.tsfiledb.engine.overflow.io;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.io.File;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import cn.edu.thu.tsfile.common.conf.TSFileConfig;
import cn.edu.thu.tsfile.common.conf.TSFileDescriptor;
import cn.edu.thu.tsfile.common.utils.RandomAccessOutputStream;
import cn.edu.thu.tsfile.file.metadata.enums.TSDataType;
import cn.edu.thu.tsfile.timeseries.read.query.DynamicOneColumnData;
import cn.edu.thu.tsfiledb.conf.TSFileDBConfig;
import cn.edu.thu.tsfiledb.conf.TSFileDBDescriptor;
import cn.edu.thu.tsfiledb.engine.bufferwrite.Action;
import cn.edu.thu.tsfiledb.engine.bufferwrite.FileNodeConstants;
import cn.edu.thu.tsfiledb.engine.exception.OverflowProcessorException;
import cn.edu.thu.tsfiledb.engine.overflow.io.OverflowProcessor;

public class OverflowProcessorTest {

	private String nameSpacePath = "nsp";
	private String overflowfilePath = null;
	private String overflowrestorefilePath = null;
	private String overflowmergefilePath = null;
	private Map<String, Object> parameters = null;
	private OverflowProcessor ofprocessor = null;
	private TSFileDBConfig tsdbconfig = TSFileDBDescriptor.getInstance().getConfig();
	private TSFileConfig tsconfig = TSFileDescriptor.getInstance().getConfig();
	private String deltaObjectId = "root.vehicle.d0";
	private String[] measurementIds = { "s0", "s1", "s2", "s3", "s4", "s5" };
	private TSDataType[] dataTypes = { TSDataType.INT32, TSDataType.INT64, TSDataType.FLOAT, TSDataType.DOUBLE,
			TSDataType.BOOLEAN, TSDataType.BYTE_ARRAY };

	private Action overflowflushaction = new Action() {

		@Override
		public void act() throws Exception {
			System.out.println("overflow flush action");
		}
	};

	private Action filenodeflushaction = new Action() {

		@Override
		public void act() throws Exception {
			System.out.println("filenode flush action");
		}
	};

	private Action filenodemanagerbackupaction = new Action() {

		@Override
		public void act() throws Exception {
			System.out.println("filenode manager backup action");
		}
	};

	private Action filenodemanagerflushaction = new Action() {

		@Override
		public void act() throws Exception {
			System.out.println("filenode manager flush action");
		}
	};

	@Before
	public void setUp() throws Exception {
		EngineTestHelper.delete(nameSpacePath);
		parameters = new HashMap<String, Object>();
		parameters.put(FileNodeConstants.OVERFLOW_FLUSH_ACTION, overflowflushaction);
		parameters.put(FileNodeConstants.FILENODE_PROCESSOR_FLUSH_ACTION, filenodeflushaction);
		parameters.put(FileNodeConstants.OVERFLOW_BACKUP_MANAGER_ACTION, filenodemanagerbackupaction);
		parameters.put(FileNodeConstants.OVERFLOW_FLUSH_MANAGER_ACTION, filenodemanagerflushaction);

		// set overflow data dir is ""
		tsdbconfig.overflowDataDir = "";
		overflowfilePath = tsdbconfig.overflowDataDir + nameSpacePath + File.separatorChar + nameSpacePath
				+ ".overflow";
		overflowrestorefilePath = overflowfilePath + ".restore";
		overflowmergefilePath = overflowfilePath + ".merge";
	}

	@After
	public void tearDown() throws Exception {
		EngineTestHelper.delete(nameSpacePath);
	}

	@Test
	public void testClose() {

		try {
			ofprocessor = new OverflowProcessor(nameSpacePath, parameters);
			ofprocessor.close();
			// test file path
			assertEquals(true, new File(overflowfilePath).exists());
			assertEquals(false, new File(overflowrestorefilePath).exists());
		} catch (OverflowProcessorException e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
		try {
			ofprocessor = new OverflowProcessor(nameSpacePath, parameters);
			ofprocessor.insert(deltaObjectId, measurementIds[0], 1, dataTypes[0], Integer.toString(10));
			List<Object> result = ofprocessor.query(deltaObjectId, measurementIds[0], null, null, null);
			DynamicOneColumnData insertDate = (DynamicOneColumnData) result.get(0);
			assertEquals(1, insertDate.length);
			assertEquals(1, insertDate.getTime(0));
			assertEquals(10, insertDate.getInt(0));
			ofprocessor.close();
			// test file path
			assertEquals(true, new File(overflowfilePath).exists());
			assertEquals(true, new File(overflowrestorefilePath).exists());
			// add bytes in the tail of the file
			RandomAccessOutputStream ras = new RandomAccessOutputStream(new File(overflowfilePath));
			ras.seek(ras.getPos());
			ras.write(new byte[10]);
			ras.close();
			ofprocessor = new OverflowProcessor(nameSpacePath, parameters);
			result = ofprocessor.query(deltaObjectId, measurementIds[0], null, null, null);
			insertDate = (DynamicOneColumnData) result.get(0);
			assertEquals(1, insertDate.length);
			assertEquals(1, insertDate.getTime(0));
			assertEquals(10, insertDate.getInt(0));
		} catch (OverflowProcessorException e) {
			e.printStackTrace();
			fail(e.getMessage());
		} catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}

	@Test
	public void testInsert() {

		// insert one point: int
		try {
			ofprocessor = new OverflowProcessor(nameSpacePath, parameters);
			for (int i = 1; i < 11; i++) {
				ofprocessor.insert(deltaObjectId, measurementIds[0], i, dataTypes[0], Integer.toString(i));
			}
			List<Object> result = ofprocessor.query(deltaObjectId, measurementIds[0], null, null, null);
			DynamicOneColumnData insertDate = (DynamicOneColumnData) result.get(0);
			assertEquals(10, insertDate.length);
			assertEquals(10, insertDate.timeLength);
			for (int i = 1; i < 11; i++) {
				assertEquals(i, insertDate.getTime(i - 1));
				assertEquals(i, insertDate.getInt(i - 1));
			}
			ofprocessor.close();

			ofprocessor = new OverflowProcessor(nameSpacePath, parameters);
			result = ofprocessor.query(deltaObjectId, measurementIds[0], null, null, null);
			insertDate = (DynamicOneColumnData) result.get(0);
			assertEquals(10, insertDate.length);
			assertEquals(10, insertDate.timeLength);
			for (int i = 1; i < 11; i++) {
				assertEquals(i, insertDate.getTime(i - 1));
				assertEquals(i, insertDate.getInt(i - 1));
			}
			ofprocessor.close();
		} catch (OverflowProcessorException e) {
			e.printStackTrace();
			fail(e.getMessage());
		}

	}

	@Test
	public void testUpdate() {
		// update data range
		try {
			ofprocessor = new OverflowProcessor(nameSpacePath, parameters);
			for (int i = 1; i < 20; i = i + 2) {
				ofprocessor.update(deltaObjectId, measurementIds[1], i, i + 1, dataTypes[1], Long.toString(i));
			}
			List<Object> result = ofprocessor.query(deltaObjectId, measurementIds[1], null, null, null);
			DynamicOneColumnData updateData = (DynamicOneColumnData) result.get(1);
			assertEquals(10, updateData.length);
			assertEquals(20, updateData.timeLength);

			for (int i = 1; i < 20; i = i + 2) {
				assertEquals(i, updateData.getTime(i - 1));
				assertEquals(i + 1, updateData.getTime(i));
				assertEquals(i, updateData.getLong((i - 1) / 2));
			}
			ofprocessor.close();
		} catch (OverflowProcessorException e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}

	@Test
	public void testDelete() {
		try {
			ofprocessor = new OverflowProcessor(nameSpacePath, parameters);
			// insert data
			for (int i = 1; i < 11; i++) {
				ofprocessor.insert(deltaObjectId, measurementIds[0], i, dataTypes[0], String.valueOf(i));
			}
			// delete data
			// delete time<5
			ofprocessor.delete(deltaObjectId, measurementIds[0], 5, dataTypes[0]);
			List<Object> result = ofprocessor.query(deltaObjectId, measurementIds[0], null, null, null);
			DynamicOneColumnData insertDate = (DynamicOneColumnData) result.get(0);
			assertEquals(6, insertDate.length);
			for (int i = 5; i < 11; i++) {
				assertEquals(i, insertDate.getTime(i - 5));
				assertEquals(i, insertDate.getInt(i - 5));
			}
			ofprocessor.insert(deltaObjectId, measurementIds[0], 1, dataTypes[0], String.valueOf(1));
			result = ofprocessor.query(deltaObjectId, measurementIds[0], null, null, null);
			insertDate = (DynamicOneColumnData) result.get(0);
			assertEquals(7, insertDate.length);
			assertEquals(1, insertDate.getTime(0));
			assertEquals(1, insertDate.getInt(0));
			ofprocessor.close();
		} catch (OverflowProcessorException e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}

	@Test
	public void testFlush() {
		// set the tsfile tsdbconfig
		tsconfig.rowGroupSize = 500;
		try {
			ofprocessor = new OverflowProcessor(nameSpacePath, parameters);
			for (int i = 1; i < 1001; i++) {
				ofprocessor.insert(deltaObjectId, measurementIds[0], i, dataTypes[0], Integer.toString(i));
			}
			// wait to flush
			Thread.sleep(100);
			List<Object> result = ofprocessor.query(deltaObjectId, measurementIds[0], null, null, null);
			DynamicOneColumnData insertData = (DynamicOneColumnData) result.get(0);
			assertEquals(1000, insertData.length);
			for (int i = 1; i < 1001; i++) {
				assertEquals(i, insertData.getTime(i - 1));
				assertEquals(i, insertData.getInt(i - 1));
			}
			ofprocessor.close();
		} catch (OverflowProcessorException e) {
			e.printStackTrace();
			fail(e.getMessage());
		} catch (InterruptedException e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}

	@Test
	public void testMerge() {
		// insert data
		tsconfig.rowGroupSize = 500;
		try {
			ofprocessor = new OverflowProcessor(nameSpacePath, parameters);
			for (int i = 1; i < 1001; i++) {
				ofprocessor.insert(deltaObjectId, measurementIds[0], i, dataTypes[0], Integer.toString(i));
			}
			Thread.sleep(100);
			ofprocessor.close();
			assertEquals(true, new File(overflowrestorefilePath).exists());
			ofprocessor.switchWorkingToMerge();
			// check file
			assertEquals(true, new File(overflowmergefilePath).exists());
			assertEquals(true, new File(overflowfilePath).exists());
			assertEquals(0, new File(overflowfilePath).length());
			assertEquals(false, new File(overflowrestorefilePath).exists());
			// query data
			List<Object> result = ofprocessor.query(deltaObjectId, measurementIds[0], null, null, null);
			DynamicOneColumnData insertData = (DynamicOneColumnData) result.get(0);
			assertEquals(1000, insertData.length);
			for (int i = 1; i < 1001; i++) {
				assertEquals(i, insertData.getTime(i - 1));
				assertEquals(i, insertData.getInt(i - 1));
			}
			ofprocessor.switchMergeToWorking();
			result = ofprocessor.query(deltaObjectId, measurementIds[0], null, null, null);
			insertData = (DynamicOneColumnData) result.get(0);
			assertEquals(null, insertData);
			ofprocessor.insert(deltaObjectId, measurementIds[0], 1010, dataTypes[0], String.valueOf(1010));
			result = ofprocessor.query(deltaObjectId, measurementIds[0], null, null, null);
			insertData = (DynamicOneColumnData) result.get(0);
			assertEquals(1, insertData.length);
			assertEquals(1010, insertData.getTime(0));
			assertEquals(1010, insertData.getInt(0));
			ofprocessor.close();

			ofprocessor = new OverflowProcessor(nameSpacePath, parameters);
			result = ofprocessor.query(deltaObjectId, measurementIds[0], null, null, null);
			insertData = (DynamicOneColumnData) result.get(0);
			assertEquals(1, insertData.length);
			assertEquals(1010, insertData.getTime(0));
			assertEquals(1010, insertData.getInt(0));
			ofprocessor.close();
		} catch (OverflowProcessorException e) {
			e.printStackTrace();
			fail(e.getMessage());
		} catch (InterruptedException e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}

	@Test
	public void testRestoreFromMerge() {
		// write some rowgroup and close file

		// change the file name to merge and delete the restore file

		// write data in new file and close it or damage it

		// and restore file and to merge

		// check query

		fail("restore from merege");
	}

	@Test
	public void testFlushCloseAndMergeQuery() {

		// write oveflow data and close

		// work to merge

		// optional: write data in new file

		// query data and check data

		fail("merge and query");
	}
}
