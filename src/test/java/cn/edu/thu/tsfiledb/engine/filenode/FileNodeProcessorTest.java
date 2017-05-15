package cn.edu.thu.tsfiledb.engine.filenode;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import cn.edu.thu.tsfile.common.conf.TSFileConfig;
import cn.edu.thu.tsfile.common.conf.TSFileDescriptor;
import cn.edu.thu.tsfile.common.utils.Pair;
import cn.edu.thu.tsfile.file.metadata.RowGroupMetaData;
import cn.edu.thu.tsfile.file.metadata.enums.TSDataType;
import cn.edu.thu.tsfile.timeseries.read.query.DynamicOneColumnData;
import cn.edu.thu.tsfiledb.conf.TSFileDBConfig;
import cn.edu.thu.tsfiledb.conf.TSFileDBDescriptor;
import cn.edu.thu.tsfiledb.engine.bufferwrite.Action;
import cn.edu.thu.tsfiledb.engine.bufferwrite.BufferWriteProcessor;
import cn.edu.thu.tsfiledb.engine.bufferwrite.FileNodeConstants;
import cn.edu.thu.tsfiledb.engine.exception.BufferWriteProcessorException;
import cn.edu.thu.tsfiledb.engine.exception.FileNodeProcessorException;
import cn.edu.thu.tsfiledb.engine.exception.OverflowProcessorException;
import cn.edu.thu.tsfiledb.engine.lru.MetadataManagerHelper;
import cn.edu.thu.tsfiledb.engine.overflow.io.EngineTestHelper;
import cn.edu.thu.tsfiledb.engine.overflow.io.OverflowProcessor;

public class FileNodeProcessorTest {

	private TSFileDBConfig tsdbconfig = TSFileDBDescriptor.getInstance().getConfig();

	private TSFileConfig tsconfig = TSFileDescriptor.getInstance().getConfig();

	private FileNodeProcessor processor = null;

	private String deltaObjectId = "root.vehicle.d0";

	private String measurementId = "s0";

	private Map<String, Object> parameters = null;

	private FileNodeProcessorStore fileNodeProcessorStore;

	private Action overflowBackUpAction = new Action() {

		@Override
		public void act() throws Exception {
			System.out.println("Manager overflow backup");
		}
	};

	private Action overflowFlushAction = new Action() {

		@Override
		public void act() throws Exception {
			System.out.println("Manager overflow flush");
		}
	};

	@Before
	public void setUp() throws Exception {
		tsdbconfig.FileNodeDir = "filenode" + File.separatorChar;
		tsdbconfig.BufferWriteDir = "bufferwrite";
		tsdbconfig.overflowDataDir = "overflow";
		tsdbconfig.metadataDir = "metadata";
		// set rowgroupsize
		tsconfig.rowGroupSize = 2000;
		tsconfig.pageCheckSizeThreshold = 3;
		tsconfig.pageSize = 100;
		tsconfig.defaultMaxStringLength = 2;

		parameters = new HashMap<>();
		parameters.put(FileNodeConstants.OVERFLOW_BACKUP_MANAGER_ACTION, overflowBackUpAction);
		parameters.put(FileNodeConstants.OVERFLOW_FLUSH_MANAGER_ACTION, overflowFlushAction);
		EngineTestHelper.delete(tsdbconfig.FileNodeDir);
		EngineTestHelper.delete(tsdbconfig.BufferWriteDir);
		EngineTestHelper.delete(tsdbconfig.overflowDataDir);
		EngineTestHelper.delete(tsdbconfig.metadataDir);
		MetadataManagerHelper.initMetadata();
	}

	@After
	public void tearDown() throws Exception {
		EngineTestHelper.delete(tsdbconfig.FileNodeDir);
		EngineTestHelper.delete(tsdbconfig.BufferWriteDir);
		EngineTestHelper.delete(tsdbconfig.overflowDataDir);
		EngineTestHelper.delete(tsdbconfig.metadataDir);
		MetadataManagerHelper.clearMetadata();
	}

	@Test
	public void testGetAndCloseProcessor() {

		try {
			processor = new FileNodeProcessor(tsdbconfig.FileNodeDir, deltaObjectId, parameters);
			assertEquals(-1, processor.getLastUpdateTime());
			processor.setLastUpdateTime(20);
			assertEquals(20, processor.getLastUpdateTime());
			processor.setLastUpdateTime(-1);
			assertEquals(false, processor.hasBufferwriteProcessor());
			assertEquals(false, processor.hasOverflowProcessor());

			// get bufferwrite processor
			long lastUpdateTime = 10;
			BufferWriteProcessor bfprocessor = processor.getBufferWriteProcessor(deltaObjectId, lastUpdateTime);

			String filename = bfprocessor.getFileName();
			String bufferwritefilePath = tsdbconfig.BufferWriteDir + File.separatorChar + deltaObjectId
					+ File.separatorChar + filename;
			assertEquals(true, new File(bufferwritefilePath).exists());
			// add intervalFileNode
			processor.addIntervalFileNode(lastUpdateTime, filename);
			bfprocessor.write(deltaObjectId, measurementId, lastUpdateTime, TSDataType.INT32, String.valueOf(10));
			assertEquals(true, bfprocessor.isNewProcessor());
			bfprocessor.setNewProcessor(false);
			processor.setLastUpdateTime(lastUpdateTime);
			assertEquals(true, filename.startsWith(String.valueOf(lastUpdateTime)));
			assertEquals(true, bfprocessor.canBeClosed());
			assertEquals(bfprocessor, processor.getBufferWriteProcessor());

			// get overflow processor
			OverflowProcessor ofprocessor = processor.getOverflowProcessor(deltaObjectId, parameters);
			assertEquals(ofprocessor, processor.getOverflowProcessor());
			ofprocessor.insert(deltaObjectId, measurementId, 5, TSDataType.INT32, String.valueOf(5));
			String overflowfile = ofprocessor.getFileName();
			String overflowfilePath = tsdbconfig.overflowDataDir + File.separatorChar + deltaObjectId
					+ File.separatorChar + overflowfile;
			assertEquals(true, new File(overflowfilePath).exists());
			String overflowfileRestorePath = overflowfilePath + ".restore";
			assertEquals(false, new File(overflowfileRestorePath).exists());
			// close processor
			assertEquals(true, processor.canBeClosed());
			processor.close();
			assertEquals(true, new File(overflowfileRestorePath).exists());

		} catch (FileNodeProcessorException e) {
			e.printStackTrace();
			fail(e.getMessage());
		} catch (BufferWriteProcessorException e) {
			e.printStackTrace();
			fail(e.getMessage());
		} catch (OverflowProcessorException e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}

	@Deprecated
	public void testMerge() {

		List<Pair<Long, Long>> bufferwriteRanges = new ArrayList<>();
		bufferwriteRanges.add(new Pair<Long, Long>(5L, 20L));
		bufferwriteRanges.add(new Pair<Long, Long>(25L, 40L));
		bufferwriteRanges.add(new Pair<Long, Long>(45L, 60L));
		bufferwriteRanges.add(new Pair<Long, Long>(65L, 80L));

		createBufferwritedata(bufferwriteRanges);

		createOverflowdata();

		try {
			processor = new FileNodeProcessor(tsdbconfig.FileNodeDir, deltaObjectId, parameters);
			processor.writeLock();
			if (processor.hasBufferwriteProcessor()) {
				processor.getBufferWriteProcessor().close();
			}
			if (!processor.hasOverflowProcessor()) {
				processor.getOverflowProcessor(deltaObjectId, parameters);
			}
			processor.getOverflowProcessor().close();
			processor.merge();
			// check the result
			QueryStructure queryRestult = processor.query(deltaObjectId, measurementId, null, null, null);
			DynamicOneColumnData bufferwriteinindex = queryRestult.getBufferwriteDataInMemory();
			List<RowGroupMetaData> bufferwriterowgroups = queryRestult.getBufferwriteDataInDisk();
			List<IntervalFileNode> newInterFiles = queryRestult.getBufferwriteDataInFiles();
			List<Object> overflowResult = queryRestult.getAllOverflowData();
			assertEquals(null, bufferwriteinindex);
			assertEquals(null, bufferwriterowgroups);
			assertEquals(4, newInterFiles.size());
			// check bufferwrite data
			// 2-22 :bug
			IntervalFileNode bufferwriteFile = newInterFiles.get(0);
			assertEquals(0, bufferwriteFile.startTime);
			assertEquals(24, bufferwriteFile.endTime);
			assertEquals(OverflowChangeType.NO_CHANGE, bufferwriteFile.overflowChangeType);
			checkFile(bufferwriteFile.filePath);
			// 25-40 :bug
			bufferwriteFile = newInterFiles.get(1);
			assertEquals(25, bufferwriteFile.startTime);
			assertEquals(44, bufferwriteFile.endTime);
			assertEquals(OverflowChangeType.NO_CHANGE, bufferwriteFile.overflowChangeType);
			checkFile(bufferwriteFile.filePath);
			// 45-62 :bug
			bufferwriteFile = newInterFiles.get(2);
			assertEquals(45, bufferwriteFile.startTime);
			assertEquals(64, bufferwriteFile.endTime);
			assertEquals(OverflowChangeType.NO_CHANGE, bufferwriteFile.overflowChangeType);
			checkFile(bufferwriteFile.filePath);
			// 65-80 :bug
			bufferwriteFile = newInterFiles.get(3);
			assertEquals(65, bufferwriteFile.startTime);
			assertEquals(80, bufferwriteFile.endTime);
			assertEquals(OverflowChangeType.NO_CHANGE, bufferwriteFile.overflowChangeType);
			checkFile(bufferwriteFile.filePath);
			// check overflow data: no overflow data
			assertEquals(null, overflowResult.get(0));
			assertEquals(null, overflowResult.get(1));
			assertEquals(null, overflowResult.get(2));
			assertEquals(null, overflowResult.get(3));
			processor.close();

		} catch (FileNodeProcessorException e) {
			e.printStackTrace();
			fail(e.getMessage());
		} catch (BufferWriteProcessorException e) {
			e.printStackTrace();
			fail(e.getMessage());
		} catch (OverflowProcessorException e) {
			e.printStackTrace();
			fail(e.getMessage());
		}

	}

	@Test
	public void testQuery() {

		try {
			// test memory data in index
			processor = new FileNodeProcessor(tsdbconfig.FileNodeDir, deltaObjectId, parameters);
			BufferWriteProcessor bfprocessor = processor.getBufferWriteProcessor(deltaObjectId, 1);
			bfprocessor.setNewProcessor(false);
			processor.addIntervalFileNode(1, bfprocessor.getFileName());
			// write data into buffer write processor
			bfprocessor.write(deltaObjectId, measurementId, 1, TSDataType.INT32, String.valueOf(1));
			processor.setLastUpdateTime(1);
			for (int i = 2; i < 11; i++) {
				bfprocessor = processor.getBufferWriteProcessor(deltaObjectId, i);
				bfprocessor.write(deltaObjectId, measurementId, i, TSDataType.INT32, String.valueOf(i));
				processor.setLastUpdateTime(i);
			}
			if (!processor.hasOverflowProcessor()) {
				processor.getOverflowProcessor(deltaObjectId, parameters);
			}
			int token = processor.addMultiPassLock();
			QueryStructure queryResult = processor.query(deltaObjectId, measurementId, null, null, null);
			processor.removeMultiPassLock(token);
			DynamicOneColumnData bufferwritedataindex = queryResult.getBufferwriteDataInMemory();
			List<RowGroupMetaData> bufferwritedataindisk = queryResult.getBufferwriteDataInDisk();
			List<IntervalFileNode> bufferwritedatainfiles = queryResult.getBufferwriteDataInFiles();
			List<Object> overflowResult = queryResult.getAllOverflowData();
			assertEquals(10, bufferwritedataindex.length);
			for (int i = 1; i < 11; i++) {
				assertEquals(i, bufferwritedataindex.getTime(i - 1));
				assertEquals(i, bufferwritedataindex.getInt(i - 1));
			}
			assertEquals(0, bufferwritedataindisk.size());
			assertEquals(1, bufferwritedatainfiles.size());
			assertEquals(false, bufferwritedatainfiles.get(0).isClosed());
			assertEquals(null, overflowResult.get(0));
			assertEquals(null, overflowResult.get(1));
			assertEquals(null, overflowResult.get(2));
			assertEquals(null, overflowResult.get(3));

			// test memory data in unclosed buffer write file
			for (int i = 11; i < 1000; i++) {
				bfprocessor = processor.getBufferWriteProcessor(deltaObjectId, i);
				bfprocessor.write(deltaObjectId, measurementId, i, TSDataType.INT32, String.valueOf(i));
				processor.setLastUpdateTime(i);
				if (i == 400) {
					break;
				}
			}

			if (!processor.hasOverflowProcessor()) {
				processor.getOverflowProcessor(deltaObjectId, parameters);
			}
			Thread.sleep(100);// wait to flush end
			token = processor.addMultiPassLock();
			queryResult = processor.query(deltaObjectId, measurementId, null, null, null);
			processor.removeMultiPassLock(token);
			bufferwritedataindisk = queryResult.getBufferwriteDataInDisk();
			bufferwritedatainfiles = queryResult.getBufferwriteDataInFiles();
			overflowResult = queryResult.getAllOverflowData();
			assertEquals(4, bufferwritedataindisk.size());
			assertEquals(1, bufferwritedatainfiles.size());
			assertEquals(false, bufferwritedatainfiles.get(0).isClosed());
			assertEquals(null, overflowResult.get(0));
			assertEquals(null, overflowResult.get(1));
			assertEquals(null, overflowResult.get(2));
			assertEquals(null, overflowResult.get(3));
			processor.close();

			// test data in closed bufferwrite file
			processor = new FileNodeProcessor(tsdbconfig.FileNodeDir, deltaObjectId, parameters);
			bfprocessor = processor.getBufferWriteProcessor(deltaObjectId, 401);
			bfprocessor.setNewProcessor(false);
			processor.addIntervalFileNode(401, bfprocessor.getFileName());
			// write data into buffer write processor
			bfprocessor.write(deltaObjectId, measurementId, 401, TSDataType.INT32, String.valueOf(401));
			processor.setLastUpdateTime(401);
			for (int i = 402; i < 1000; i++) {
				bfprocessor = processor.getBufferWriteProcessor(deltaObjectId, i);
				bfprocessor.write(deltaObjectId, measurementId, i, TSDataType.INT32, String.valueOf(i));
				processor.setLastUpdateTime(i);
				if (i == 800) {
					break;
				}
			}
			if (!processor.hasOverflowProcessor()) {
				processor.getOverflowProcessor(deltaObjectId, parameters);
			}
			Thread.sleep(100);// wait to flush end
			token = processor.addMultiPassLock();
			queryResult = processor.query(deltaObjectId, measurementId, null, null, null);
			processor.removeMultiPassLock(token);
			bufferwritedataindisk = queryResult.getBufferwriteDataInDisk();
			bufferwritedatainfiles = queryResult.getBufferwriteDataInFiles();
			overflowResult = queryResult.getAllOverflowData();
			assertEquals(4, bufferwritedataindisk.size());
			assertEquals(2, bufferwritedatainfiles.size());
			IntervalFileNode intervalFileNode = bufferwritedatainfiles.get(0);
			assertEquals(true, intervalFileNode.isClosed());
			assertEquals(1, intervalFileNode.startTime);
			assertEquals(400, intervalFileNode.endTime);
			assertEquals(OverflowChangeType.NO_CHANGE, intervalFileNode.overflowChangeType);
			assertEquals(false, bufferwritedatainfiles.get(1).isClosed());
			assertEquals(401, bufferwritedatainfiles.get(1).startTime);
			assertEquals(-1, bufferwritedatainfiles.get(1).endTime);
			assertEquals(null, overflowResult.get(0));
			assertEquals(null, overflowResult.get(1));
			assertEquals(null, overflowResult.get(2));
			assertEquals(null, overflowResult.get(3));
			processor.close();

			processor = new FileNodeProcessor(tsdbconfig.FileNodeDir, deltaObjectId, parameters);
			bfprocessor = processor.getBufferWriteProcessor(deltaObjectId, 801);
			bfprocessor.setNewProcessor(false);
			processor.addIntervalFileNode(801, bfprocessor.getFileName());
			bfprocessor.write(deltaObjectId, measurementId, 801, TSDataType.INT32, String.valueOf(801));
			processor.setLastUpdateTime(801);
			for (int i = 802; i < 820; i++) {
				bfprocessor = processor.getBufferWriteProcessor(deltaObjectId, i);
				bfprocessor.write(deltaObjectId, measurementId, i, TSDataType.INT32, String.valueOf(i));
				processor.setLastUpdateTime(i);
			}
			processor.close();

			processor = new FileNodeProcessor(tsdbconfig.FileNodeDir, deltaObjectId, parameters);
			bfprocessor = processor.getBufferWriteProcessor(deltaObjectId, 820);
			bfprocessor.setNewProcessor(false);
			processor.addIntervalFileNode(820, bfprocessor.getFileName());
			bfprocessor.write(deltaObjectId, measurementId, 820, TSDataType.INT32, String.valueOf(820));
			processor.setLastUpdateTime(820);
			for (int i = 821; i < 840; i++) {
				bfprocessor = processor.getBufferWriteProcessor(deltaObjectId, i);
				bfprocessor.write(deltaObjectId, measurementId, i, TSDataType.INT32, String.valueOf(i));
				processor.setLastUpdateTime(i);
			}
			processor.close();
			// mkdir: test delete unused file in construct the filenode
			// processor
			// String tempFilePath = tsdbconfig.BufferWriteDir +
			// File.separatorChar + deltaObjectId + File.separatorChar
			// + "temp";
			// File tempFile = new File(tempFilePath);
			// assertEquals(true, tempFile.mkdir());
			// assertEquals(true, tempFile.exists());

			// file range: 1-400 401-800 801-819 820-839
			processor = new FileNodeProcessor(tsdbconfig.FileNodeDir, deltaObjectId, parameters);
			// assertEquals(false, tempFile.exists());

			// overflow data
			OverflowProcessor ofprocessor = processor.getOverflowProcessor(deltaObjectId, parameters);
			ofprocessor.update(deltaObjectId, measurementId, 2, 10, TSDataType.INT32, String.valueOf(1001));
			processor.changeTypeToChanged(2, 10);
			ofprocessor.update(deltaObjectId, measurementId, 802, 810, TSDataType.INT32, String.valueOf(2000));
			processor.changeTypeToChanged(802, 810);

			token = processor.addMultiPassLock();
			assertEquals(false, processor.canBeClosed());
			queryResult = processor.query(deltaObjectId, measurementId, null, null, null);
			bufferwritedataindex = queryResult.getBufferwriteDataInMemory();
			bufferwritedataindisk = queryResult.getBufferwriteDataInDisk();
			bufferwritedatainfiles = queryResult.getBufferwriteDataInFiles();
			overflowResult = queryResult.getAllOverflowData();
			assertEquals(null, bufferwritedataindex);
			assertEquals(null, bufferwritedataindisk);
			assertEquals(4, bufferwritedatainfiles.size());
			System.out.println(bufferwritedatainfiles);
			assertEquals(OverflowChangeType.CHANGED, bufferwritedatainfiles.get(0).overflowChangeType);
			assertEquals(OverflowChangeType.NO_CHANGE, bufferwritedatainfiles.get(1).overflowChangeType);
			assertEquals(OverflowChangeType.CHANGED, bufferwritedatainfiles.get(2).overflowChangeType);
			assertEquals(OverflowChangeType.NO_CHANGE, bufferwritedatainfiles.get(3).overflowChangeType);
			DynamicOneColumnData updateDate = (DynamicOneColumnData) overflowResult.get(1);
			assertEquals(2, updateDate.length);
			assertEquals(4, updateDate.timeLength);
			assertEquals(2, updateDate.getTime(0));
			assertEquals(10, updateDate.getTime(1));
			assertEquals(802, updateDate.getTime(2));
			assertEquals(810, updateDate.getTime(3));
			processor.removeMultiPassLock(token);
			assertEquals(true, processor.canBeClosed());
			processor.close();

		} catch (FileNodeProcessorException e) {
			e.printStackTrace();
			fail(e.getMessage());
		} catch (BufferWriteProcessorException e) {
			e.printStackTrace();
			fail(e.getMessage());
		} catch (InterruptedException e) {
			e.printStackTrace();
			fail(e.getMessage());
		} catch (OverflowProcessorException e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}

	@Test
	public void testRecoveryBufferFile() {

		try {
			processor = new FileNodeProcessor(tsdbconfig.FileNodeDir, deltaObjectId, parameters);
			BufferWriteProcessor bfprocessor = processor.getBufferWriteProcessor(deltaObjectId, 1);
			bfprocessor.setNewProcessor(false);
			processor.addIntervalFileNode(1, bfprocessor.getFileName());
			// write data into buffer write processor
			bfprocessor.write(deltaObjectId, measurementId, 1, TSDataType.INT32, String.valueOf(1));

			for (int i = 2; i < 1000; i++) {
				bfprocessor = processor.getBufferWriteProcessor(deltaObjectId, i);
				bfprocessor.write(deltaObjectId, measurementId, i, TSDataType.INT32, String.valueOf(i));
				processor.setLastUpdateTime(i);
				if (i == 400) {
					break;
				}
			}
			Thread.sleep(100);
			if (!processor.hasOverflowProcessor()) {
				processor.getOverflowProcessor(deltaObjectId, parameters);
			}
			QueryStructure queryResult = processor.query(deltaObjectId, measurementId, null, null, null);
			List<RowGroupMetaData> bufferwritedataindisk = queryResult.getBufferwriteDataInDisk();
			List<IntervalFileNode> bufferwritedatainfiles = queryResult.getBufferwriteDataInFiles();
			List<Object> overflowResult = queryResult.getAllOverflowData();

			assertEquals(4, bufferwritedataindisk.size());
			assertEquals(1, bufferwritedatainfiles.size());
			assertEquals(false, bufferwritedatainfiles.get(0).isClosed());
			assertEquals(null, overflowResult.get(0));
			assertEquals(null, overflowResult.get(1));
			assertEquals(null, overflowResult.get(2));
			assertEquals(null, overflowResult.get(3));
			// not close and restore the bufferwrite file
			processor = new FileNodeProcessor(tsdbconfig.FileNodeDir, deltaObjectId, parameters);
			assertEquals(true, processor.hasBufferwriteProcessor());
			assertEquals(true, processor.hasOverflowProcessor());
			queryResult = processor.query(deltaObjectId, measurementId, null, null, null);
			bufferwritedataindisk = queryResult.getBufferwriteDataInDisk();
			bufferwritedatainfiles = queryResult.getBufferwriteDataInFiles();
			overflowResult = queryResult.getAllOverflowData();
			assertEquals(4, bufferwritedataindisk.size());
			assertEquals(1, bufferwritedatainfiles.size());
			assertEquals(false, bufferwritedatainfiles.get(0).isClosed());
			assertEquals(null, overflowResult.get(0));
			assertEquals(null, overflowResult.get(1));
			assertEquals(null, overflowResult.get(2));
			assertEquals(null, overflowResult.get(3));
			processor.close();

		} catch (FileNodeProcessorException e) {
			e.printStackTrace();
			fail(e.getMessage());
		} catch (BufferWriteProcessorException e) {
			e.printStackTrace();
			fail(e.getMessage());
		} catch (InterruptedException e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}

	@Test
	public void testRecoveryWait() {

		// construct one FileNodeProcessorStore
		// the status of FileNodeProcessorStore is waiting
		// construct the bufferwrite data file
		IntervalFileNode emptyIntervalFileNode = new IntervalFileNode(0, OverflowChangeType.NO_CHANGE, null, null);
		List<IntervalFileNode> newFilenodes = new ArrayList<>();
		for (int i = 1; i <= 3; i++) {
			IntervalFileNode node = new IntervalFileNode(i * 100, i * 100 + 99, OverflowChangeType.NO_CHANGE,
					"bufferfiletest" + i, null);
			// create file
			createFile(node.filePath);
			checkFile(node.filePath);
			newFilenodes.add(node);
		}
		// create unused bufferfiles
		String unusedFilename = "bufferfileunsed";
		createFile(unusedFilename);
		checkFile(unusedFilename);
		FileNodeProcessorState fileNodeProcessorState = FileNodeProcessorState.WAITING;
		fileNodeProcessorStore = new FileNodeProcessorStore(500, emptyIntervalFileNode, newFilenodes,
				fileNodeProcessorState, 0);

		String filenodedirPath = tsdbconfig.FileNodeDir + deltaObjectId + File.separatorChar;
		File file = new File(filenodedirPath);
		if (!file.exists()) {
			file.mkdirs();
		}
		String filenodestorePath = filenodedirPath + deltaObjectId + ".restore";
		SerializeUtil<FileNodeProcessorStore> serializeUtil = new SerializeUtil<>();
		try {
			serializeUtil.serialize(fileNodeProcessorStore, filenodestorePath);
		} catch (IOException e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
		assertEquals(true, new File(filenodestorePath).exists());
		// construct overflow data files

		// test recovery from waiting
		try {
			processor = new FileNodeProcessor(tsdbconfig.FileNodeDir, deltaObjectId, parameters);
			assertEquals(fileNodeProcessorStore.getLastUpdateTime(), processor.getLastUpdateTime());
			processor.close();
			FileNodeProcessorStore store = serializeUtil.deserialize(filenodestorePath).orElse(null);
			assertEquals(fileNodeProcessorStore.getLastUpdateTime(), store.getLastUpdateTime());
			assertEquals(fileNodeProcessorStore.getEmptyIntervalFileNode(), store.getEmptyIntervalFileNode());
			assertEquals(fileNodeProcessorStore.getNewFileNodes(), store.getNewFileNodes());
			assertEquals(FileNodeProcessorState.NONE, store.getFileNodeProcessorState());
			assertEquals(0, store.getNumOfMergeFile());

			// check file
			for (IntervalFileNode node : store.getNewFileNodes()) {
				checkFile(node.filePath);
				EngineTestHelper.delete(node.filePath);
			}
			checkUnFile(unusedFilename);
			EngineTestHelper.delete(unusedFilename);
		} catch (FileNodeProcessorException e) {
			e.printStackTrace();
			fail(e.getMessage());
		} catch (IOException e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}

	@Test
	public void testRevoceryMerge() {

		// construct one FileNodeProcessorStore
		// the status of FileNodeProcessorStore is merging
		// construct the bufferwrite data file
		IntervalFileNode emptyIntervalFileNode = new IntervalFileNode(0, OverflowChangeType.NO_CHANGE, null, null);
		List<IntervalFileNode> newFilenodes = new ArrayList<>();
		for (int i = 1; i <= 3; i++) {
			IntervalFileNode node = new IntervalFileNode(i * 100, i * 100 + 99, OverflowChangeType.NO_CHANGE,
					"bufferfiletest" + i, null);
			// create file
			createFile(node.filePath);
			checkFile(node.filePath);
			newFilenodes.add(node);
		}
		// create unused bufferfiles
		String unusedFilename = "bufferfileunsed";
		createFile(unusedFilename);
		checkFile(unusedFilename);
		FileNodeProcessorState fileNodeProcessorState = FileNodeProcessorState.MERGING_WRITE;
		fileNodeProcessorStore = new FileNodeProcessorStore(500, emptyIntervalFileNode, newFilenodes,
				fileNodeProcessorState, 0);

		String filenodedirPath = tsdbconfig.FileNodeDir + deltaObjectId + File.separatorChar;
		File file = new File(filenodedirPath);
		if (!file.exists()) {
			file.mkdirs();
		}
		String filenodestorePath = filenodedirPath + deltaObjectId + ".restore";
		SerializeUtil<FileNodeProcessorStore> serializeUtil = new SerializeUtil<>();
		try {
			serializeUtil.serialize(fileNodeProcessorStore, filenodestorePath);
		} catch (IOException e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
		assertEquals(true, new File(filenodestorePath).exists());
		// construct overflow data files

		// test recovery from waiting
		try {
			processor = new FileNodeProcessor(tsdbconfig.FileNodeDir, deltaObjectId, parameters);
			assertEquals(fileNodeProcessorStore.getLastUpdateTime(), processor.getLastUpdateTime());
		} catch (FileNodeProcessorException e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}

	@Deprecated
	public void testRevoceryMerge2() {
		// create bufferwrite files
		List<Pair<Long, Long>> bufferwriteRanges = new ArrayList<>();
		bufferwriteRanges.add(new Pair<Long, Long>(5L, 20L));
		bufferwriteRanges.add(new Pair<Long, Long>(25L, 40L));
		bufferwriteRanges.add(new Pair<Long, Long>(45L, 60L));
		bufferwriteRanges.add(new Pair<Long, Long>(65L, 80L));

		createBufferwritedata(bufferwriteRanges);
		String unusedFilename = "bufferfileunsed";
		createFile(unusedFilename);
		checkFile(unusedFilename);
		try {
			// check the bufferwrite files
			processor = new FileNodeProcessor(tsdbconfig.FileNodeDir, deltaObjectId, parameters);
			processor.getOverflowProcessor(deltaObjectId, parameters);
			int token = processor.addMultiPassLock();
			QueryStructure queryResult = processor.query(deltaObjectId, measurementId, null, null, null);
			assertEquals(false, processor.canBeClosed());
			processor.removeMultiPassLock(token);
			List<IntervalFileNode> newInterFiles = queryResult.getBufferwriteDataInFiles();
			List<Object> overflowResult = queryResult.getAllOverflowData();
			assertEquals(null, overflowResult.get(0));
			assertEquals(null, overflowResult.get(1));
			assertEquals(null, overflowResult.get(2));
			assertEquals(null, overflowResult.get(3));
			int size = newInterFiles.size();
			for (int i = 0; i < size; i++) {
				IntervalFileNode node = (IntervalFileNode) newInterFiles.get(i);
				assertEquals(bufferwriteRanges.get(i).left.longValue(), node.startTime);
				assertEquals(bufferwriteRanges.get(i).right.longValue(), node.endTime);
			}
			processor.close();
		} catch (FileNodeProcessorException e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
		// create overflow file
		// Overflow range: insert:2,22,62(222) update:50-70 (222)
		createOverflowdata();
		// check the overflow
		List<IntervalFileNode> restoreNewFiles = new ArrayList<>();
		long lastUpdateTime = -1;
		try {
			processor = new FileNodeProcessor(tsdbconfig.FileNodeDir, deltaObjectId, parameters);
			lastUpdateTime = processor.getLastUpdateTime();
			processor.getOverflowProcessor(deltaObjectId, parameters);
			int token = processor.addMultiPassLock();
			QueryStructure queryResult = processor.query(deltaObjectId, measurementId, null, null, null);
			assertEquals(false, processor.canBeClosed());
			processor.removeMultiPassLock(token);
			DynamicOneColumnData bufferwriteindex = queryResult.getBufferwriteDataInMemory();
			List<RowGroupMetaData> bufferwriterowgroups = queryResult.getBufferwriteDataInDisk();
			List<IntervalFileNode> newInterFiles = queryResult.getBufferwriteDataInFiles();
			restoreNewFiles = newInterFiles;
			List<Object> overflowResult = queryResult.getAllOverflowData();
			// check bufferwrite data
			assertEquals(null, bufferwriteindex);
			assertEquals(null, bufferwriterowgroups);
			assertEquals(bufferwriteRanges.size(), newInterFiles.size());
			IntervalFileNode bufferwriteFile = newInterFiles.get(0);
			// check the intervalFile status
			assertEquals(OverflowChangeType.CHANGED, bufferwriteFile.overflowChangeType);
			bufferwriteFile = newInterFiles.get(2);
			assertEquals(OverflowChangeType.CHANGED, bufferwriteFile.overflowChangeType);
			bufferwriteFile = newInterFiles.get(3);
			assertEquals(OverflowChangeType.CHANGED, bufferwriteFile.overflowChangeType);
			// check overflow data
			// insert
			assertEquals(true, overflowResult.get(0) != null);
			DynamicOneColumnData insertData = (DynamicOneColumnData) overflowResult.get(0);
			assertEquals(3, insertData.length);
			assertEquals(2, insertData.getTime(0));
			assertEquals(222, insertData.getInt(0));
			assertEquals(22, insertData.getTime(1));
			assertEquals(222, insertData.getInt(1));
			assertEquals(62, insertData.getTime(2));
			assertEquals(333, insertData.getInt(2));
			// update
			assertEquals(true, overflowResult.get(1) != null);
			DynamicOneColumnData updateData = (DynamicOneColumnData) overflowResult.get(1);
			/*
			 * CGF?? The check range is right??
			 */
			assertEquals(2, updateData.length);
			assertEquals(4, updateData.timeLength);
			assertEquals(50, updateData.getTime(0));
			assertEquals(61, updateData.getTime(1));
			assertEquals(333, updateData.getInt(0));
			assertEquals(63, updateData.getTime(2));
			assertEquals(70, updateData.getTime(3));
			assertEquals(333, updateData.getInt(1));
			processor.close();
		} catch (FileNodeProcessorException e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
		// construct the status is merge in the restore file
		IntervalFileNode emptyIntervalFileNode = new IntervalFileNode(0, OverflowChangeType.NO_CHANGE, null, null);
		FileNodeProcessorState fileNodeProcessorState = FileNodeProcessorState.MERGING_WRITE;
		System.out.println(lastUpdateTime);
		System.out.println(restoreNewFiles);
		fileNodeProcessorStore = new FileNodeProcessorStore(lastUpdateTime, emptyIntervalFileNode, restoreNewFiles,
				fileNodeProcessorState, 0);

		String filenodedirPath = tsdbconfig.FileNodeDir + deltaObjectId + File.separatorChar;
		File file = new File(filenodedirPath);
		if (!file.exists()) {
			file.mkdirs();
		}
		String filenodestorePath = filenodedirPath + deltaObjectId + ".restore";
		assertEquals(true, new File(filenodestorePath).exists());
		SerializeUtil<FileNodeProcessorStore> serializeUtil = new SerializeUtil<>();
		try {
			serializeUtil.serialize(fileNodeProcessorStore, filenodestorePath);
		} catch (IOException e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
		assertEquals(true, new File(filenodestorePath).exists());

		// restore from merge
		try {
			processor = new FileNodeProcessor(tsdbconfig.FileNodeDir, deltaObjectId, parameters);
			// it will merge automatically
			QueryStructure queryRestult = processor.query(deltaObjectId, measurementId, null, null, null);
			DynamicOneColumnData bufferwriteinindex = queryRestult.getBufferwriteDataInMemory();
			List<RowGroupMetaData> bufferwriterowgroups = queryRestult.getBufferwriteDataInDisk();
			List<IntervalFileNode> newInterFiles = queryRestult.getBufferwriteDataInFiles();
			List<Object> overflowResult = queryRestult.getAllOverflowData();
			assertEquals(null, bufferwriteinindex);
			assertEquals(null, bufferwriterowgroups);
			assertEquals(4, newInterFiles.size());
			// check bufferwrite data
			// 2-22 :bug
			IntervalFileNode bufferwriteFile = newInterFiles.get(0);
			assertEquals(0, bufferwriteFile.startTime);
			assertEquals(24, bufferwriteFile.endTime);
			assertEquals(OverflowChangeType.NO_CHANGE, bufferwriteFile.overflowChangeType);
			checkFile(bufferwriteFile.filePath);
			// 25-40 :bug
			bufferwriteFile = newInterFiles.get(1);
			assertEquals(25, bufferwriteFile.startTime);
			assertEquals(44, bufferwriteFile.endTime);
			assertEquals(OverflowChangeType.NO_CHANGE, bufferwriteFile.overflowChangeType);
			checkFile(bufferwriteFile.filePath);
			// 45-62 :bug
			bufferwriteFile = newInterFiles.get(2);
			assertEquals(45, bufferwriteFile.startTime);
			assertEquals(64, bufferwriteFile.endTime);
			assertEquals(OverflowChangeType.NO_CHANGE, bufferwriteFile.overflowChangeType);
			checkFile(bufferwriteFile.filePath);
			// 65-80 :bug
			bufferwriteFile = newInterFiles.get(3);
			assertEquals(65, bufferwriteFile.startTime);
			assertEquals(80, bufferwriteFile.endTime);
			assertEquals(OverflowChangeType.NO_CHANGE, bufferwriteFile.overflowChangeType);
			checkFile(bufferwriteFile.filePath);
			// check overflow data: no overflow data
			assertEquals(null, overflowResult.get(0));
			assertEquals(null, overflowResult.get(1));
			assertEquals(null, overflowResult.get(2));
			assertEquals(null, overflowResult.get(3));
			processor.close();
		} catch (FileNodeProcessorException e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
		// check the merge result
		checkUnFile(unusedFilename);
	}

	@Test
	public void testMergeFromEmptyIntervalFile() {
		// test merege from Empty Interval File
		try {
			processor = new FileNodeProcessor(tsdbconfig.FileNodeDir, deltaObjectId, parameters);
			// set lastupdate time
			processor.setLastUpdateTime(100);
			processor.close();
			// There is no buffer write files
			// create overflow data
			// insert:2,22,62; update:50-70
			createOverflowdata();

			processor = new FileNodeProcessor(tsdbconfig.FileNodeDir, deltaObjectId, parameters);
			processor.getOverflowProcessor(deltaObjectId, parameters);
			// check overflow data
			QueryStructure queryResult = processor.query(deltaObjectId, measurementId, null, null, null);
			assertEquals(null, queryResult.getBufferwriteDataInMemory());
			assertEquals(null, queryResult.getBufferwriteDataInDisk());
			assertEquals(0, queryResult.getBufferwriteDataInFiles().size());
			List<Object> overflowResult = queryResult.getAllOverflowData();
			DynamicOneColumnData insertData = (DynamicOneColumnData) overflowResult.get(0);
			assertEquals(3, insertData.length);
			assertEquals(2, insertData.getTime(0));
			assertEquals(222, insertData.getInt(0));
			assertEquals(22, insertData.getTime(1));
			assertEquals(222, insertData.getInt(1));
			assertEquals(62, insertData.getTime(2));
			assertEquals(333, insertData.getInt(2));

			DynamicOneColumnData updateData = (DynamicOneColumnData) overflowResult.get(1);
			assertEquals(2, updateData.length);
			assertEquals(4, updateData.timeLength);
			assertEquals(50, updateData.getTime(0));
			assertEquals(61, updateData.getTime(1));
			assertEquals(63, updateData.getTime(2));
			assertEquals(70, updateData.getTime(3));
			assertEquals(333, updateData.getInt(0));
			assertEquals(333, updateData.getInt(1));
			// merge
			processor.close();

		} catch (FileNodeProcessorException e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}

	private void createFile(String filename) {

		File file = new File(filename);
		if (!file.exists()) {
			file.mkdir();
		}
	}

	private void checkFile(String filename) {

		File file = new File(filename);
		assertEquals(true, file.exists());
	}

	private void checkUnFile(String filename) {

		String filePath = tsdbconfig.BufferWriteDir + File.separatorChar + deltaObjectId;
		File dataDir = new File(filePath);
		if (!dataDir.exists()) {
			dataDir.mkdirs();
		}
		File file = new File(dataDir, filename);
		assertEquals(false, file.exists());
	}

	private void createBufferwritedata(List<Pair<Long, Long>> bufferwriteRanges) {
		for (Pair<Long, Long> timePair : bufferwriteRanges) {
			createBufferwriteFile(timePair.left, timePair.right);
			try {
				Thread.sleep(10);
			} catch (InterruptedException e) {
				e.printStackTrace();
				fail(e.getMessage());
			}
		}
	}

	/**
	 * create bufferwrite file, time from begin to end
	 * 
	 * @param begin
	 * @param end
	 */
	private void createBufferwriteFile(long begin, long end) {
		try {
			processor = new FileNodeProcessor(tsdbconfig.FileNodeDir, deltaObjectId, parameters);
			BufferWriteProcessor bfProcessor = processor.getBufferWriteProcessor(deltaObjectId, begin);
			assertEquals(true, bfProcessor.isNewProcessor());
			bfProcessor.write(measurementId, measurementId, begin, TSDataType.INT32, String.valueOf(begin));
			processor.setLastUpdateTime(begin);
			bfProcessor.setNewProcessor(false);
			processor.addIntervalFileNode(begin, bfProcessor.getFileName());
			for (long i = begin + 1; i <= end; i++) {
				bfProcessor = processor.getBufferWriteProcessor(deltaObjectId, i);
				bfProcessor.write(deltaObjectId, measurementId, i, TSDataType.INT32, String.valueOf(i));
				processor.setLastUpdateTime(i);
			}
			processor.close();
		} catch (FileNodeProcessorException e) {
			e.printStackTrace();
			fail(e.getMessage());
		} catch (BufferWriteProcessorException e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}

	/**
	 * Overflow range: insert:2,22,62;update:50-70
	 */
	private void createOverflowdata() {

		// insert: 2
		createOverflowInsert(2, 222);
		// insert: 22
		createOverflowInsert(22, 222);
		// insert: 62
		createOverflowInsert(62, 222);
		// update: 50-70
		createOverflowUpdate(50, 70, 333);
	}

	private void createOverflowInsert(long time, int value) {
		try {
			processor = new FileNodeProcessor(tsdbconfig.FileNodeDir, deltaObjectId, parameters);
			OverflowProcessor ofProcessor = processor.getOverflowProcessor(deltaObjectId, parameters);
			ofProcessor.insert(deltaObjectId, measurementId, time, TSDataType.INT32, String.valueOf(value));
			processor.changeTypeToChanged(time);
			processor.close();
			Thread.sleep(10);
		} catch (FileNodeProcessorException e) {
			e.printStackTrace();
			fail(e.getMessage());
		} catch (OverflowProcessorException e) {
			e.printStackTrace();
			fail(e.getMessage());
		} catch (InterruptedException e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}

	private void createOverflowUpdate(long begin, long end, int value) {
		try {
			processor = new FileNodeProcessor(tsdbconfig.FileNodeDir, deltaObjectId, parameters);
			OverflowProcessor ofProcessor = processor.getOverflowProcessor(deltaObjectId, parameters);
			ofProcessor.update(deltaObjectId, measurementId, begin, end, TSDataType.INT32, String.valueOf(value));
			processor.changeTypeToChanged(begin, end);
			processor.close();
			Thread.sleep(10);
		} catch (FileNodeProcessorException e) {
			e.printStackTrace();
			fail(e.getMessage());
		} catch (OverflowProcessorException e) {
			e.printStackTrace();
			fail(e.getMessage());
		} catch (InterruptedException e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}
}
