package cn.edu.thu.tsfiledb.engine.filenode;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.io.ByteArrayInputStream;
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
import cn.edu.thu.tsfile.file.metadata.enums.CompressionTypeName;
import cn.edu.thu.tsfile.file.metadata.enums.TSDataType;
import cn.edu.thu.tsfile.timeseries.read.query.DynamicOneColumnData;
import cn.edu.thu.tsfiledb.conf.TsfileDBConfig;
import cn.edu.thu.tsfiledb.conf.TsfileDBDescriptor;
import cn.edu.thu.tsfiledb.engine.bufferwrite.Action;
import cn.edu.thu.tsfiledb.engine.bufferwrite.BufferWriteProcessor;
import cn.edu.thu.tsfiledb.engine.bufferwrite.FileNodeConstants;
import cn.edu.thu.tsfiledb.engine.exception.BufferWriteProcessorException;
import cn.edu.thu.tsfiledb.engine.exception.FileNodeProcessorException;
import cn.edu.thu.tsfiledb.engine.exception.OverflowProcessorException;
import cn.edu.thu.tsfiledb.engine.lru.MetadataManagerHelper;
import cn.edu.thu.tsfiledb.engine.overflow.io.EngineTestHelper;
import cn.edu.thu.tsfiledb.engine.overflow.io.OverflowProcessor;
import cn.edu.thu.tsfiledb.exception.PathErrorException;
import cn.edu.thu.tsfiledb.metadata.MManager;
import cn.edu.thu.tsfiledb.sys.writelog.WriteLogManager;

/**
 * @author liukun
 *
 */
public class FileNodeProcessorTest {

	private TsfileDBConfig tsdbconfig = TsfileDBDescriptor.getInstance().getConfig();

	private TSFileConfig tsconfig = TSFileDescriptor.getInstance().getConfig();

	private FileNodeProcessor processor = null;

	private String deltaObjectId = "root.vehicle.d0";

	private String deltaObjectId2 = "root.vehicle.d2";

	private String measurementId = "s0";

	private Map<String, Object> parameters = null;

	private FileNodeProcessorStore fileNodeProcessorStore;

	private String nameSpacePath = null;

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

	private String FileNodeDir;
	private String BufferWriteDir;
	private String overflowDataDir;
	private int rowGroupSize;
	private int pageCheckSizeThreshold = tsconfig.pageCheckSizeThreshold;
	private int defaultMaxStringLength = tsconfig.maxStringLength;
	private boolean cachePageData = tsconfig.duplicateIncompletedPage;
	private int pageSize = tsconfig.pageSizeInByte;
	@Before
	public void setUp() throws Exception {
		FileNodeDir = tsdbconfig.fileNodeDir;
		BufferWriteDir = tsdbconfig.bufferWriteDir;
		overflowDataDir = tsdbconfig.overflowDataDir;
		
		tsdbconfig.fileNodeDir = "filenode" + File.separatorChar;
		tsdbconfig.bufferWriteDir = "bufferwrite";
		tsdbconfig.overflowDataDir = "overflow";
		tsdbconfig.metadataDir = "metadata";
		// set rowgroupsize
		rowGroupSize = tsconfig.groupSizeInByte;
		tsconfig.groupSizeInByte = 2000;
		tsconfig.pageCheckSizeThreshold = 3;
		tsconfig.pageSizeInByte = 100;
		tsconfig.maxStringLength = 2;
		tsconfig.duplicateIncompletedPage = true;

		parameters = new HashMap<>();
		parameters.put(FileNodeConstants.OVERFLOW_BACKUP_MANAGER_ACTION, overflowBackUpAction);
		parameters.put(FileNodeConstants.OVERFLOW_FLUSH_MANAGER_ACTION, overflowFlushAction);
		EngineTestHelper.delete(tsdbconfig.fileNodeDir);
		EngineTestHelper.delete(tsdbconfig.bufferWriteDir);
		EngineTestHelper.delete(tsdbconfig.overflowDataDir);
		EngineTestHelper.delete(tsdbconfig.metadataDir);
		EngineTestHelper.delete(tsdbconfig.walFolder);
		MetadataManagerHelper.initMetadata();

		nameSpacePath = MManager.getInstance().getFileNameByPath(deltaObjectId);
		WriteLogManager.getInstance().close();
	}

	@After
	public void tearDown() throws Exception {
		WriteLogManager.getInstance().close();
		MManager.getInstance().flushObjectToFile();
		EngineTestHelper.delete(tsdbconfig.fileNodeDir);
		EngineTestHelper.delete(tsdbconfig.bufferWriteDir);
		EngineTestHelper.delete(tsdbconfig.overflowDataDir);
		EngineTestHelper.delete(tsdbconfig.metadataDir);
		EngineTestHelper.delete(tsdbconfig.walFolder);
		
		tsdbconfig.fileNodeDir = FileNodeDir;
		tsdbconfig.overflowDataDir = overflowDataDir;
		tsdbconfig.bufferWriteDir = BufferWriteDir;
		
		tsconfig.groupSizeInByte = rowGroupSize;
		tsconfig.pageCheckSizeThreshold = pageCheckSizeThreshold;
		tsconfig.pageSizeInByte = pageSize;
		tsconfig.maxStringLength = defaultMaxStringLength;
		tsconfig.duplicateIncompletedPage = cachePageData;
		

	}

	@Test
	public void testGetAndCloseProcessor() throws Exception {

		try {
			// nameSpacePath
			processor = new FileNodeProcessor(tsdbconfig.fileNodeDir, nameSpacePath, parameters);
			assertEquals(-1, processor.getLastUpdateTime(deltaObjectId));
			assertEquals(-1, processor.getLastUpdateTime(deltaObjectId2));
			processor.setLastUpdateTime(deltaObjectId, 20);
			processor.setLastUpdateTime(deltaObjectId2, 21);
			assertEquals(20, processor.getLastUpdateTime(deltaObjectId));
			assertEquals(21, processor.getLastUpdateTime(deltaObjectId2));
			processor.setLastUpdateTime(deltaObjectId, -1);
			processor.setLastUpdateTime(deltaObjectId2, -1);
			assertEquals(false, processor.hasBufferwriteProcessor());
			assertEquals(false, processor.hasOverflowProcessor());

			// get bufferwrite processor
			long lastUpdateTime = 10;
			BufferWriteProcessor bfprocessor = processor.getBufferWriteProcessor(nameSpacePath, lastUpdateTime);

			String filename = bfprocessor.getFileName();
			System.out.println(bfprocessor.getFileAbsolutePath());
			String bufferwritefilePath = tsdbconfig.bufferWriteDir + File.separatorChar + deltaObjectId
					+ File.separatorChar + filename;
			assertEquals(true, new File(bufferwritefilePath).exists());
			// add intervalFileNode
			processor.addIntervalFileNode(lastUpdateTime, filename);
			bfprocessor.write(deltaObjectId, measurementId, lastUpdateTime, TSDataType.INT32, String.valueOf(10));
			assertEquals(true, bfprocessor.isNewProcessor());
			bfprocessor.setNewProcessor(false);
			processor.setIntervalFileNodeStartTime(deltaObjectId, lastUpdateTime);
			processor.setLastUpdateTime(deltaObjectId, lastUpdateTime);
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

	@Test
	public void testQuery() throws Exception {

		try {
			// test memory data in index
			processor = new FileNodeProcessor(tsdbconfig.fileNodeDir, nameSpacePath, parameters);
			BufferWriteProcessor bfprocessor = processor.getBufferWriteProcessor(deltaObjectId, 1);
			bfprocessor.setNewProcessor(false);
			processor.addIntervalFileNode(1, bfprocessor.getFileAbsolutePath());
			// write data into buffer write processor
			bfprocessor.write(deltaObjectId, measurementId, 1, TSDataType.INT32, String.valueOf(1));
			processor.setIntervalFileNodeStartTime(deltaObjectId, 1);
			processor.setLastUpdateTime(deltaObjectId, 1);
			for (int i = 2; i < 11; i++) {
				bfprocessor = processor.getBufferWriteProcessor(deltaObjectId, i);
				bfprocessor.write(deltaObjectId, measurementId, i, TSDataType.INT32, String.valueOf(i));
				processor.setIntervalFileNodeStartTime(deltaObjectId, i);
				processor.setLastUpdateTime(deltaObjectId, i);
			}
			if (!processor.hasOverflowProcessor()) {
				processor.getOverflowProcessor(deltaObjectId, parameters);
			}
			int token = processor.addMultiPassLock();
			QueryStructure queryResult = processor.query(deltaObjectId, measurementId, null, null, null);
			processor.removeMultiPassLock(token);
			DynamicOneColumnData bufferwritedataindex = queryResult.getCurrentPage();
			Pair<List<ByteArrayInputStream>, CompressionTypeName> right = queryResult.getPageList();
			List<RowGroupMetaData> bufferwritedataindisk = queryResult.getBufferwriteDataInDisk();
			List<IntervalFileNode> bufferwritedatainfiles = queryResult.getBufferwriteDataInFiles();
			List<Object> overflowResult = queryResult.getAllOverflowData();
			assertEquals(true, bufferwritedataindex != null);
			assertEquals(true, right != null);
			assertEquals(1, right.left.size());
			for (ByteArrayInputStream stream : right.left) {
				DynamicOneColumnData columnData = PageTestUtils.pageToDynamic(stream, right.right, deltaObjectId,
						measurementId);
				columnData.mergeRecord(bufferwritedataindex);
				bufferwritedataindex = columnData;
			}

			for (int i = 1; i < 11; i++) {
				assertEquals(i, bufferwritedataindex.getTime(i - 1));
				assertEquals(i, bufferwritedataindex.getInt(i - 1));
			}
			assertEquals(0, bufferwritedataindisk.size());
			assertEquals(1, bufferwritedatainfiles.size());
			IntervalFileNode temp = bufferwritedatainfiles.get(0);
			assertEquals(1, temp.getStartTime(deltaObjectId));
			assertEquals(-1, temp.getEndTime(deltaObjectId));
			assertEquals(false, bufferwritedatainfiles.get(0).isClosed());
			assertEquals(null, overflowResult.get(0));
			assertEquals(null, overflowResult.get(1));
			assertEquals(null, overflowResult.get(2));
			assertEquals(null, overflowResult.get(3));

			// test memory data in unclosed buffer write file
			for (int i = 11; i < 1000; i++) {
				bfprocessor = processor.getBufferWriteProcessor(deltaObjectId, i);
				bfprocessor.write(deltaObjectId, measurementId, i, TSDataType.INT32, String.valueOf(i));
				processor.setIntervalFileNodeStartTime(deltaObjectId, i);
				processor.setLastUpdateTime(deltaObjectId, i);
				if (i == 400) {
					break;
				}
			}

			if (!processor.hasOverflowProcessor()) {
				processor.getOverflowProcessor(deltaObjectId, parameters);
			}
			Thread.sleep(100);// wait to flush end
			assertEquals(400, processor.getLastUpdateTime(deltaObjectId));
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
			processor = new FileNodeProcessor(tsdbconfig.fileNodeDir, deltaObjectId, parameters);
			bfprocessor = processor.getBufferWriteProcessor(deltaObjectId, 401);
			bfprocessor.setNewProcessor(false);
			processor.addIntervalFileNode(401, bfprocessor.getFileAbsolutePath());
			// write data into buffer write processor
			bfprocessor.write(deltaObjectId, measurementId, 401, TSDataType.INT32, String.valueOf(401));
			processor.setIntervalFileNodeStartTime(deltaObjectId, 401);
			processor.setLastUpdateTime(deltaObjectId, 401);
			for (int i = 402; i < 1000; i++) {
				bfprocessor = processor.getBufferWriteProcessor(deltaObjectId, i);
				bfprocessor.write(deltaObjectId, measurementId, i, TSDataType.INT32, String.valueOf(i));
				processor.setIntervalFileNodeStartTime(deltaObjectId, i);
				processor.setLastUpdateTime(deltaObjectId, i);
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
			assertEquals(1, intervalFileNode.getStartTime(deltaObjectId));
			assertEquals(400, intervalFileNode.getEndTime(deltaObjectId));
			assertEquals(OverflowChangeType.NO_CHANGE, intervalFileNode.overflowChangeType);
			assertEquals(false, bufferwritedatainfiles.get(1).isClosed());
			assertEquals(401, bufferwritedatainfiles.get(1).getStartTime(deltaObjectId));
			assertEquals(-1, bufferwritedatainfiles.get(1).getEndTime(deltaObjectId));
			assertEquals(null, overflowResult.get(0));
			assertEquals(null, overflowResult.get(1));
			assertEquals(null, overflowResult.get(2));
			assertEquals(null, overflowResult.get(3));
			processor.close();

			processor = new FileNodeProcessor(tsdbconfig.fileNodeDir, deltaObjectId, parameters);
			bfprocessor = processor.getBufferWriteProcessor(deltaObjectId, 801);
			bfprocessor.setNewProcessor(false);
			processor.addIntervalFileNode(801, bfprocessor.getFileAbsolutePath());
			bfprocessor.write(deltaObjectId, measurementId, 801, TSDataType.INT32, String.valueOf(801));
			processor.setLastUpdateTime(deltaObjectId, 801);
			for (int i = 802; i < 820; i++) {
				bfprocessor = processor.getBufferWriteProcessor(deltaObjectId, i);
				bfprocessor.write(deltaObjectId, measurementId, i, TSDataType.INT32, String.valueOf(i));
				processor.setIntervalFileNodeStartTime(deltaObjectId, i);
				processor.setLastUpdateTime(deltaObjectId, i);
			}
			processor.close();

			processor = new FileNodeProcessor(tsdbconfig.fileNodeDir, deltaObjectId, parameters);
			bfprocessor = processor.getBufferWriteProcessor(deltaObjectId, 820);
			bfprocessor.setNewProcessor(false);
			processor.addIntervalFileNode(820, bfprocessor.getFileAbsolutePath());
			bfprocessor.write(deltaObjectId, measurementId, 820, TSDataType.INT32, String.valueOf(820));
			processor.setLastUpdateTime(deltaObjectId, 820);
			for (int i = 821; i < 840; i++) {
				bfprocessor = processor.getBufferWriteProcessor(deltaObjectId, i);
				bfprocessor.write(deltaObjectId, measurementId, i, TSDataType.INT32, String.valueOf(i));
				processor.setIntervalFileNodeStartTime(deltaObjectId, i);
				processor.setLastUpdateTime(deltaObjectId, i);
			}
			processor.close();

			// file range: 1-400 401-800 801-819 820-839
			processor = new FileNodeProcessor(tsdbconfig.fileNodeDir, deltaObjectId, parameters);
			// assertEquals(false, tempFile.exists());

			// overflow data
			OverflowProcessor ofprocessor = processor.getOverflowProcessor(deltaObjectId, parameters);
			ofprocessor.update(deltaObjectId, measurementId, 2, 10, TSDataType.INT32, String.valueOf(1001));
			processor.changeTypeToChanged(deltaObjectId, 2, 10);
			ofprocessor.update(deltaObjectId, measurementId, 802, 810, TSDataType.INT32, String.valueOf(2000));
			processor.changeTypeToChanged(deltaObjectId, 802, 810);

			token = processor.addMultiPassLock();
			assertEquals(false, processor.canBeClosed());
			queryResult = processor.query(deltaObjectId, measurementId, null, null, null);
			bufferwritedataindex = queryResult.getCurrentPage();
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
			assertEquals(2, updateDate.valueLength);
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
		} catch (PathErrorException e) {
			e.printStackTrace();
			fail(e.getMessage());
		} catch (IOException e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}

	@Test
	public void testQueryToken() {
		try {
			processor = new FileNodeProcessor(tsdbconfig.fileNodeDir, deltaObjectId, parameters);
			processor.writeLock();
			int token = processor.addMultiPassLock();
			assertEquals(0, token);
			assertEquals(false, processor.canBeClosed());
			processor.removeMultiPassLock(token);
			assertEquals(true, processor.canBeClosed());

			token = processor.addMultiPassLock();
			assertEquals(0, token);
			int token2 = processor.addMultiPassLock();
			assertEquals(1, token2);
			processor.removeMultiPassLock(token2);
			assertEquals(false, processor.canBeClosed());
			processor.removeMultiPassLock(token);
			processor.close();
		} catch (FileNodeProcessorException e) {
			e.printStackTrace();
			fail(e.getMessage());
		}

	}

	@Test
	public void testRecoveryBufferFile() throws Exception {

		FileNodeProcessor fileNodeProcessor = null;
		try {
			fileNodeProcessor = new FileNodeProcessor(tsdbconfig.fileNodeDir, deltaObjectId, parameters);
			BufferWriteProcessor bfprocessor = fileNodeProcessor.getBufferWriteProcessor(deltaObjectId, 1);
			bfprocessor.setNewProcessor(false);
			fileNodeProcessor.addIntervalFileNode(1, bfprocessor.getFileAbsolutePath());
			// write data into buffer write processor
			bfprocessor.write(deltaObjectId, measurementId, 1, TSDataType.INT32, String.valueOf(1));
			fileNodeProcessor.setIntervalFileNodeStartTime(deltaObjectId, 1);
			fileNodeProcessor.setLastUpdateTime(deltaObjectId, 1);
			for (int i = 2; i < 1000; i++) {
				bfprocessor = fileNodeProcessor.getBufferWriteProcessor(deltaObjectId, i);
				bfprocessor.write(deltaObjectId, measurementId, i, TSDataType.INT32, String.valueOf(i));
				fileNodeProcessor.setIntervalFileNodeStartTime(deltaObjectId, i);
				fileNodeProcessor.setLastUpdateTime(deltaObjectId, i);
				if (i == 400) {
					break;
				}
			}
			Thread.sleep(100);
			if (!fileNodeProcessor.hasOverflowProcessor()) {
				fileNodeProcessor.getOverflowProcessor(deltaObjectId, parameters);
			}
			QueryStructure queryResult = fileNodeProcessor.query(deltaObjectId, measurementId, null, null, null);
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
			processor = new FileNodeProcessor(tsdbconfig.fileNodeDir, deltaObjectId, parameters);
			processor.writeLock();
			assertEquals(true, processor.shouldRecovery());
			processor.fileNodeRecovery();
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
			fileNodeProcessor.close();
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
		IntervalFileNode emptyIntervalFileNode = new IntervalFileNode(OverflowChangeType.NO_CHANGE, null);
		List<IntervalFileNode> newFilenodes = new ArrayList<>();
		for (int i = 1; i <= 3; i++) {
			// i * 100, i * 100 + 99
			IntervalFileNode node = new IntervalFileNode(OverflowChangeType.NO_CHANGE, "bufferfiletest" + i);
			node.setStartTime(deltaObjectId, i * 100);
			node.setEndTime(deltaObjectId, i * 100 + 99);
			// create file
			createFile(node.filePath);
			checkFile(node.filePath);
			newFilenodes.add(node);
		}
		// create unused bufferfiles
		String unusedFilename = "bufferfileunsed";
		createFile(unusedFilename);
		checkFile(unusedFilename);
		FileNodeProcessorStatus fileNodeProcessorState = FileNodeProcessorStatus.WAITING;
		Map<String, Long> lastUpdateTimeMap = new HashMap<>();
		lastUpdateTimeMap.put(deltaObjectId, (long) 500);
		fileNodeProcessorStore = new FileNodeProcessorStore(lastUpdateTimeMap, emptyIntervalFileNode, newFilenodes,
				fileNodeProcessorState, 0);

		String filenodedirPath = tsdbconfig.fileNodeDir + deltaObjectId + File.separatorChar;
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

		// test recovery from waiting
		try {
			processor = new FileNodeProcessor(tsdbconfig.fileNodeDir, deltaObjectId, parameters);
			processor.writeLock();
			assertEquals(true, processor.shouldRecovery());
			// recovery
			processor.fileNodeRecovery();
			assertEquals(fileNodeProcessorStore.getLastUpdateTimeMap(), processor.getLastUpdateTimeMap());
			processor.close();
			FileNodeProcessorStore store = serializeUtil.deserialize(filenodestorePath).orElse(null);
			assertEquals(fileNodeProcessorStore.getLastUpdateTimeMap(), store.getLastUpdateTimeMap());
			assertEquals(fileNodeProcessorStore.getEmptyIntervalFileNode(), store.getEmptyIntervalFileNode());
			assertEquals(fileNodeProcessorStore.getNewFileNodes(), store.getNewFileNodes());
			assertEquals(FileNodeProcessorStatus.NONE, store.getFileNodeProcessorState());
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

		String filePath = tsdbconfig.bufferWriteDir + File.separatorChar + deltaObjectId;
		File dataDir = new File(filePath);
		if (!dataDir.exists()) {
			dataDir.mkdirs();
		}
		File file = new File(dataDir, filename);
		assertEquals(false, file.exists());
	}

	private void createBufferwritedata(List<Pair<Long, Long>> bufferwriteRanges) throws Exception {
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
	 * @throws Exception 
	 */
	private void createBufferwriteFile(long begin, long end) throws Exception {
		try {
			processor = new FileNodeProcessor(tsdbconfig.fileNodeDir, deltaObjectId, parameters);
			BufferWriteProcessor bfProcessor = processor.getBufferWriteProcessor(deltaObjectId, begin);
			assertEquals(true, bfProcessor.isNewProcessor());
			bfProcessor.write(measurementId, measurementId, begin, TSDataType.INT32, String.valueOf(begin));
			processor.setLastUpdateTime(deltaObjectId, begin);
			bfProcessor.setNewProcessor(false);
			processor.addIntervalFileNode(begin, bfProcessor.getFileAbsolutePath());
			for (long i = begin; i <= end; i++) {
				bfProcessor = processor.getBufferWriteProcessor(deltaObjectId, i);
				bfProcessor.write(deltaObjectId, measurementId, i, TSDataType.INT32, String.valueOf(i));
				processor.setIntervalFileNodeStartTime(deltaObjectId, i);
				processor.setLastUpdateTime(deltaObjectId, i);
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
			processor = new FileNodeProcessor(tsdbconfig.fileNodeDir, deltaObjectId, parameters);
			OverflowProcessor ofProcessor = processor.getOverflowProcessor(deltaObjectId, parameters);
			ofProcessor.insert(deltaObjectId, measurementId, time, TSDataType.INT32, String.valueOf(value));
			processor.changeTypeToChanged(deltaObjectId, time);
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
			processor = new FileNodeProcessor(tsdbconfig.fileNodeDir, deltaObjectId, parameters);
			OverflowProcessor ofProcessor = processor.getOverflowProcessor(deltaObjectId, parameters);
			ofProcessor.update(deltaObjectId, measurementId, begin, end, TSDataType.INT32, String.valueOf(value));
			processor.changeTypeToChanged(deltaObjectId, begin, end);
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
