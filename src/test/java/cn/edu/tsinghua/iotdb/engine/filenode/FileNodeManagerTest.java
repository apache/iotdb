package cn.edu.tsinghua.iotdb.engine.filenode;

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

import cn.edu.tsinghua.iotdb.engine.MetadataManagerHelper;
import cn.edu.tsinghua.iotdb.engine.PathUtils;
import cn.edu.tsinghua.iotdb.exception.FileNodeManagerException;
import cn.edu.tsinghua.iotdb.exception.MetadataArgsErrorException;
import cn.edu.tsinghua.iotdb.exception.PathErrorException;
import cn.edu.tsinghua.iotdb.metadata.MManager;
import cn.edu.tsinghua.iotdb.utils.EnvironmentUtils;
import cn.edu.tsinghua.tsfile.common.conf.TSFileConfig;
import cn.edu.tsinghua.tsfile.common.conf.TSFileDescriptor;
import cn.edu.tsinghua.tsfile.common.utils.Pair;
import cn.edu.tsinghua.tsfile.file.metadata.RowGroupMetaData;
import cn.edu.tsinghua.tsfile.file.metadata.enums.TSDataType;
import cn.edu.tsinghua.tsfile.timeseries.filter.definition.SingleSeriesFilterExpression;
import cn.edu.tsinghua.tsfile.timeseries.read.query.DynamicOneColumnData;
import cn.edu.tsinghua.tsfile.timeseries.write.record.DataPoint;
import cn.edu.tsinghua.tsfile.timeseries.write.record.TSRecord;

/**
 * @author liukun
 *
 */
public class FileNodeManagerTest {

	private TSFileConfig tsconfig = TSFileDescriptor.getInstance().getConfig();

	private FileNodeManager fManager = null;

	private String deltaObjectId = "root.vehicle.d0";
	private String deltaObjectId2 = "root.vehicle.d1";
	private String measurementId = "s0";
	private String measurementId6 = "s6";
	private TSDataType dataType = TSDataType.INT32;

	private int rowGroupSize;
	private int pageCheckSizeThreshold;
	private int defaultMaxStringLength;
	private boolean cachePageData;
	private int pageSize;

	@Before
	public void setUp() throws Exception {
		// origin value
		EnvironmentUtils.closeStatMonitor();
		rowGroupSize = tsconfig.groupSizeInByte;
		pageCheckSizeThreshold = tsconfig.pageCheckSizeThreshold;
		cachePageData = tsconfig.duplicateIncompletedPage;
		defaultMaxStringLength = tsconfig.maxStringLength;
		pageSize = tsconfig.pageSizeInByte;
		// new value
		tsconfig.duplicateIncompletedPage = true;
		tsconfig.groupSizeInByte = 2000;
		tsconfig.pageCheckSizeThreshold = 3;
		tsconfig.pageSizeInByte = 100;
		tsconfig.maxStringLength = 2;
		MetadataManagerHelper.initMetadata();
	}

	@After
	public void tearDown() throws Exception {
		EnvironmentUtils.cleanEnv();
		// recovery value
		tsconfig.groupSizeInByte = rowGroupSize;
		tsconfig.pageCheckSizeThreshold = pageCheckSizeThreshold;
		tsconfig.pageSizeInByte = pageSize;
		tsconfig.maxStringLength = defaultMaxStringLength;
		tsconfig.duplicateIncompletedPage = cachePageData;
	}

	//@Test
	public void testBufferwriteAndAddMetadata() {
		createBufferwriteInMemory(new Pair<Long, Long>(1000L, 1001L), measurementId);
		fManager = FileNodeManager.getInstance();

		// add metadata
		MManager mManager = MManager.getInstance();
		assertEquals(false, mManager.pathExist(deltaObjectId + "." + measurementId6));
		try {
			mManager.addPathToMTree(deltaObjectId + "." + measurementId6, "INT32", "RLE", new String[0]);
		} catch (PathErrorException | MetadataArgsErrorException | IOException e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
		assertEquals(true, mManager.pathExist(deltaObjectId + "." + measurementId6));
		// check level
		String nsp = null;
		try {
			nsp = mManager.getFileNameByPath(deltaObjectId + "." + measurementId6);
		} catch (PathErrorException e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
		try {
			fManager.closeOneFileNode(nsp);
		} catch (FileNodeManagerException e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
		createBufferwriteInMemory(new Pair<Long, Long>(200L, 302L), measurementId6);
		// write data
		try {
			fManager.closeAll();
		} catch (FileNodeManagerException e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}

	@Test
	public void testBufferwriteInsert() {

		List<Pair<Long, Long>> pairList = new ArrayList<>();
		pairList.add(new Pair<Long, Long>(100L, 200L));
		pairList.add(new Pair<Long, Long>(300L, 400L));
		pairList.add(new Pair<Long, Long>(500L, 600L));
		pairList.add(new Pair<Long, Long>(700L, 800L));
		createBufferwriteFiles(pairList, deltaObjectId);

		createBufferwriteInMemory(new Pair<Long, Long>(900L, 1000L), measurementId);

		fManager = FileNodeManager.getInstance();
		try {
			int token = fManager.beginQuery(deltaObjectId);
			QueryStructure queryResult = fManager.query(deltaObjectId, measurementId, null, null, null);
			DynamicOneColumnData bufferwriteinmemory = queryResult.getCurrentPage();
			List<RowGroupMetaData> bufferwriteinDisk = queryResult.getBufferwriteDataInDisk();
			assertEquals(true, bufferwriteinmemory != null);
			assertEquals(true, bufferwriteinDisk != null);
			List<IntervalFileNode> newInterFiles = queryResult.getBufferwriteDataInFiles();
			assertEquals(5, newInterFiles.size());
			for (int i = 0; i < pairList.size(); i++) {
				IntervalFileNode temp = newInterFiles.get(i);
				Pair<Long, Long> time = pairList.get(i);
				assertEquals(time.left.longValue(), temp.getStartTime(deltaObjectId));
				assertEquals(time.right.longValue(), temp.getEndTime(deltaObjectId));
				System.out.println(time);
			}
			IntervalFileNode temp = newInterFiles.get(4);
			assertEquals(900, temp.getStartTime(deltaObjectId));
			assertEquals(-1, temp.getEndTime(deltaObjectId));

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
			assertEquals(overflowInsert1.length, insertData.valueLength);
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
			assertEquals(overflowInsert1.length + overflowInsert2.length, insertData.valueLength);
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
			assertEquals(1, updateData.valueLength);
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
	public void testRecoveryWait() {
		File dir = PathUtils.getBufferWriteDir("");
		if (!dir.exists()) {
			dir.mkdirs();
		}
		// create file
		// file: file0
		String file0Name = "file0";
		File file0 = new File(dir, file0Name);
		file0.mkdir();
		// file: file1
		String file1Name = "file1";
		File file1 = new File(dir, file1Name);
		file1.mkdir();
		// file: file2
		String file2Name = "file2";
		File file2 = new File(dir, file2Name);
		file2.mkdir();
		// construct the file node store file
		Map<String, Long> lastUpdateTimeMap = new HashMap<>();
		lastUpdateTimeMap.put(deltaObjectId, 1000L);
		IntervalFileNode emptyIntervalFileNode = new IntervalFileNode(OverflowChangeType.NO_CHANGE, null);
		List<IntervalFileNode> newFileNodes = new ArrayList<>();
		Map<String, Long> startTimeMap = new HashMap<>();
		Map<String, Long> endTimeMap = new HashMap<>();
		startTimeMap.put(deltaObjectId, 10L);
		endTimeMap.put(deltaObjectId, 20L);
		IntervalFileNode fileNode = new IntervalFileNode(startTimeMap, endTimeMap, OverflowChangeType.NO_CHANGE,
				file0.getAbsolutePath());

		startTimeMap = new HashMap<>();
		startTimeMap.put(deltaObjectId, 30L);
		endTimeMap = new HashMap<>();
		endTimeMap.put(deltaObjectId, 40L);
		newFileNodes.add(fileNode);
		fileNode = new IntervalFileNode(startTimeMap, endTimeMap, OverflowChangeType.NO_CHANGE,
				file1.getAbsolutePath());
		newFileNodes.add(fileNode);
		startTimeMap = new HashMap<>();
		startTimeMap.put(deltaObjectId, 50L);
		endTimeMap = new HashMap<>();
		endTimeMap.put(deltaObjectId, 60L);
		fileNode = new IntervalFileNode(startTimeMap, endTimeMap, OverflowChangeType.NO_CHANGE,
				file2.getAbsolutePath());
		newFileNodes.add(fileNode);
		FileNodeProcessorStore fileNodeProcessorStore = new FileNodeProcessorStore(lastUpdateTimeMap,
				emptyIntervalFileNode, newFileNodes, FileNodeProcessorStatus.NONE, 0);
		File fileNodeDir = PathUtils.getFileNodeDir(deltaObjectId);
		if (!fileNodeDir.exists()) {
			fileNodeDir.mkdirs();
		}
		File fileNodeStatus = new File(fileNodeDir, deltaObjectId + ".restore");
		String fileNodeStatusPath = fileNodeStatus.getAbsolutePath();
		SerializeUtil<FileNodeProcessorStore> serializeUtil = new SerializeUtil<>();
		try {
			serializeUtil.serialize(fileNodeProcessorStore, fileNodeStatusPath);
		} catch (IOException e) {
			e.printStackTrace();
			fail(e.getMessage());
		}

		FileNodeManager fileNodeManager = FileNodeManager.getInstance();
		fileNodeManager.recovery();
		try {
			QueryStructure queryStructure = fileNodeManager.query(deltaObjectId, measurementId, null, null, null);
			assertEquals(null, queryStructure.getCurrentPage());
			assertEquals(null, queryStructure.getPageList());
			assertEquals(null, queryStructure.getBufferwriteDataInDisk());
			assertEquals(3, queryStructure.getBufferwriteDataInFiles().size());
			IntervalFileNode temp = queryStructure.getBufferwriteDataInFiles().get(0);
			assertEquals(10, temp.getStartTime(deltaObjectId));
			assertEquals(20, temp.getEndTime(deltaObjectId));

			temp = queryStructure.getBufferwriteDataInFiles().get(1);
			assertEquals(30, temp.getStartTime(deltaObjectId));
			assertEquals(40, temp.getEndTime(deltaObjectId));

			temp = queryStructure.getBufferwriteDataInFiles().get(2);
			assertEquals(50, temp.getStartTime(deltaObjectId));
			assertEquals(60, temp.getEndTime(deltaObjectId));

		} catch (FileNodeManagerException e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
		try {
			fileNodeManager.closeAll();
		} catch (FileNodeManagerException e) {
			e.printStackTrace();
			fail(e.getMessage());
		}

		// construct the wait
		String file3Name = "file3";
		File file3 = new File(dir, file3Name);
		file0.mkdir();
		fileNode = new IntervalFileNode(startTimeMap, endTimeMap, OverflowChangeType.NO_CHANGE,
				file3Name);
		newFileNodes.clear();
		newFileNodes.add(fileNode);
		fileNodeProcessorStore = new FileNodeProcessorStore(lastUpdateTimeMap, emptyIntervalFileNode, newFileNodes,
				FileNodeProcessorStatus.WAITING, 0);
		try {
			serializeUtil.serialize(fileNodeProcessorStore, fileNodeStatusPath);
		} catch (IOException e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
		fileNodeManager.recovery();
		try {
			QueryStructure queryStructure = fileNodeManager.query(deltaObjectId, measurementId, null, null, null);
			assertEquals(null, queryStructure.getCurrentPage());
			assertEquals(null, queryStructure.getPageList());
			assertEquals(null, queryStructure.getBufferwriteDataInDisk());
			assertEquals(1, queryStructure.getBufferwriteDataInFiles().size());
			IntervalFileNode temp = queryStructure.getBufferwriteDataInFiles().get(0);
			assertEquals(file3.getPath(), temp.getFilePath());
			fileNodeManager.closeAll();
		} catch (FileNodeManagerException e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}

	@Test
	public void testRecoveryMerge() {

		List<Pair<Long, Long>> pairList = new ArrayList<>();
		pairList.add(new Pair<Long, Long>(100L, 200L));
		pairList.add(new Pair<Long, Long>(300L, 400L));
		pairList.add(new Pair<Long, Long>(500L, 600L));
		pairList.add(new Pair<Long, Long>(700L, 800L));
		createBufferwriteFiles(pairList, deltaObjectId);
		long[] overflowInsert1 = { 2, 4, 6, 8 };
		long[] overflowInsert2 = { 202, 204, 206, 208 };
		createOverflowInserts(overflowInsert1, deltaObjectId);
		createOverflowInserts(overflowInsert2, deltaObjectId);

		FileNodeManager fileNodeManager = FileNodeManager.getInstance();
		QueryStructure queryStructure = null;
		try {
			queryStructure = fileNodeManager.query(deltaObjectId, measurementId, null, null, null);
			DynamicOneColumnData bufferwriteindex = queryStructure.getCurrentPage();
			assertEquals(null, bufferwriteindex);
			List<RowGroupMetaData> bufferwriteindisk = queryStructure.getBufferwriteDataInDisk();
			assertEquals(null, bufferwriteindisk);
			List<IntervalFileNode> bufferwriteFiles = queryStructure.getBufferwriteDataInFiles();
			assertEquals(pairList.size(), bufferwriteFiles.size());
			IntervalFileNode temp = bufferwriteFiles.get(0);
			// range 1: 2-208
			assertEquals(OverflowChangeType.CHANGED, temp.overflowChangeType);
			assertEquals(100, temp.getStartTime(deltaObjectId));
			assertEquals(-1, temp.getStartTime(deltaObjectId2));
			assertEquals(200, temp.getEndTime(deltaObjectId));
			// range 2: 202-400
			temp = bufferwriteFiles.get(1);
			assertEquals(OverflowChangeType.NO_CHANGE, temp.overflowChangeType);
			assertEquals(300, temp.getStartTime(deltaObjectId));
			assertEquals(400, temp.getEndTime(deltaObjectId));
			// range 3: 500-600
			temp = bufferwriteFiles.get(2);
			assertEquals(OverflowChangeType.NO_CHANGE, temp.overflowChangeType);
			assertEquals(500, temp.getStartTime(deltaObjectId));
			assertEquals(600, temp.getEndTime(deltaObjectId));
			// range 4: 700-800
			temp = bufferwriteFiles.get(3);
			assertEquals(OverflowChangeType.NO_CHANGE, temp.overflowChangeType);
			assertEquals(700, temp.getStartTime(deltaObjectId));
			assertEquals(800, temp.getEndTime(deltaObjectId));
			List<Object> overflowData = queryStructure.getAllOverflowData();
			assertEquals(true, overflowData.get(0) != null);
			assertEquals(true, overflowData.get(1) != null);
			assertEquals(true, overflowData.get(2) != null);
			assertEquals(true, overflowData.get(3) != null);
			fileNodeManager.closeAll();
		} catch (FileNodeManagerException e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
		File fileNodeDir = PathUtils.getFileNodeDir(deltaObjectId);
		if (!fileNodeDir.exists()) {
			fileNodeDir.mkdirs();
		}
		File fileNodeStatus = new File(fileNodeDir, deltaObjectId + ".restore");
		String fileNodeStatusPath = fileNodeStatus.getAbsolutePath();
		SerializeUtil<FileNodeProcessorStore> serializeUtil = new SerializeUtil<>();
		Map<String, Long> lastUpdateTimeMap = new HashMap<>();
		lastUpdateTimeMap.put(deltaObjectId, 1000L);
		IntervalFileNode emptyIntervalFileNode = new IntervalFileNode(OverflowChangeType.NO_CHANGE, null);
		List<IntervalFileNode> newFileNodes = queryStructure.getBufferwriteDataInFiles();
		FileNodeProcessorStore fileNodeProcessorStore = new FileNodeProcessorStore(lastUpdateTimeMap,
				emptyIntervalFileNode, newFileNodes, FileNodeProcessorStatus.MERGING_WRITE, 0);
		try {
			serializeUtil.serialize(fileNodeProcessorStore, fileNodeStatusPath);
		} catch (IOException e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
		fileNodeManager.recovery();
		try {
			Thread.sleep(2000);
		} catch (InterruptedException e1) {
			e1.printStackTrace();
			fail(e1.getMessage());
		}
		try {
			queryStructure = fileNodeManager.query(deltaObjectId, measurementId, null, null, null);
			DynamicOneColumnData bufferwriteindex = queryStructure.getCurrentPage();
			assertEquals(null, bufferwriteindex);
			List<RowGroupMetaData> bufferwriteindisk = queryStructure.getBufferwriteDataInDisk();
			assertEquals(null, bufferwriteindisk);
			List<IntervalFileNode> bufferwriteFiles = queryStructure.getBufferwriteDataInFiles();
			assertEquals(pairList.size(), bufferwriteFiles.size());
			IntervalFileNode temp = bufferwriteFiles.get(0);
			// range 1: 2-208
			assertEquals(OverflowChangeType.NO_CHANGE, temp.overflowChangeType);
			assertEquals(2, temp.getStartTime(deltaObjectId));
			assertEquals(208, temp.getEndTime(deltaObjectId));
			// range 2: 202-400
			temp = bufferwriteFiles.get(1);
			assertEquals(OverflowChangeType.NO_CHANGE, temp.overflowChangeType);
			assertEquals(300, temp.getStartTime(deltaObjectId));
			assertEquals(400, temp.getEndTime(deltaObjectId));
			// range 3: 500-600
			temp = bufferwriteFiles.get(2);
			assertEquals(OverflowChangeType.NO_CHANGE, temp.overflowChangeType);
			assertEquals(500, temp.getStartTime(deltaObjectId));
			assertEquals(600, temp.getEndTime(deltaObjectId));
			// range 4: 700-800
			temp = bufferwriteFiles.get(3);
			assertEquals(OverflowChangeType.NO_CHANGE, temp.overflowChangeType);
			assertEquals(700, temp.getStartTime(deltaObjectId));
			assertEquals(800, temp.getEndTime(deltaObjectId));
			List<Object> overflowData = queryStructure.getAllOverflowData();
			assertEquals(null, overflowData.get(0));
			assertEquals(null, overflowData.get(1));
			assertEquals(null, overflowData.get(2));
			assertEquals(null, overflowData.get(3));
			fileNodeManager.closeAll();
		} catch (FileNodeManagerException e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}

	@Test
	public void testMergeAll() {

		List<Pair<Long, Long>> pairList = new ArrayList<>();
		pairList.add(new Pair<Long, Long>(100L, 200L));
		pairList.add(new Pair<Long, Long>(300L, 400L));
		pairList.add(new Pair<Long, Long>(500L, 600L));
		pairList.add(new Pair<Long, Long>(700L, 800L));
		createBufferwriteFiles(pairList, deltaObjectId);
		createBufferwriteFiles(pairList, deltaObjectId2);
		long[] overflowInsert1 = { 2, 4, 6, 8 };
		long[] overflowInsert2 = { 202, 204, 206, 208 };
		// new file: 2-208 300-400 500-600 700-800

		// not close
		createOverflowInserts(overflowInsert1, deltaObjectId);
		createOverflowInserts(overflowInsert1, deltaObjectId2);
		// not close
		createOverflowInserts(overflowInsert2, deltaObjectId);
		createOverflowInserts(overflowInsert2, deltaObjectId2);

		fManager = FileNodeManager.getInstance();
		try {
			fManager.mergeAll();
			// query old file node
			int token = fManager.beginQuery(deltaObjectId);
			QueryStructure queryResult = fManager.query(deltaObjectId, measurementId, null, null, null);
			fManager.endQuery(deltaObjectId, token);
			System.out.println(queryResult);
			DynamicOneColumnData bufferwriteindex = queryResult.getCurrentPage();
			assertEquals(null, bufferwriteindex);
			List<RowGroupMetaData> bufferwriteindisk = queryResult.getBufferwriteDataInDisk();
			assertEquals(null, bufferwriteindisk);
			List<IntervalFileNode> bufferwriteFiles = queryResult.getBufferwriteDataInFiles();
			assertEquals(pairList.size(), bufferwriteFiles.size());
			IntervalFileNode temp = bufferwriteFiles.get(0);
			// range 1: 2-208
			assertEquals(OverflowChangeType.CHANGED, temp.overflowChangeType);
			assertEquals(100, temp.getStartTime(deltaObjectId));
			assertEquals(-1, temp.getStartTime(deltaObjectId2));
			assertEquals(200, temp.getEndTime(deltaObjectId));
			// range 2: 202-400
			temp = bufferwriteFiles.get(1);
			assertEquals(OverflowChangeType.NO_CHANGE, temp.overflowChangeType);
			assertEquals(300, temp.getStartTime(deltaObjectId));
			assertEquals(400, temp.getEndTime(deltaObjectId));
			// range 3: 500-600
			temp = bufferwriteFiles.get(2);
			assertEquals(OverflowChangeType.NO_CHANGE, temp.overflowChangeType);
			assertEquals(500, temp.getStartTime(deltaObjectId));
			assertEquals(600, temp.getEndTime(deltaObjectId));
			// range 4: 700-800
			temp = bufferwriteFiles.get(3);
			assertEquals(OverflowChangeType.NO_CHANGE, temp.overflowChangeType);
			assertEquals(700, temp.getStartTime(deltaObjectId));
			assertEquals(800, temp.getEndTime(deltaObjectId));
			List<Object> overflowData = queryResult.getAllOverflowData();
			assertEquals(true, overflowData.get(0) != null);
			assertEquals(true, overflowData.get(1) != null);
			assertEquals(true, overflowData.get(2) != null);
			assertEquals(true, overflowData.get(3) != null);

			// wait to merge over
			waitToSleep(2000);
			// query new file and overflow data
			token = fManager.beginQuery(deltaObjectId);
			queryResult = fManager.query(deltaObjectId, measurementId, null, null, null);
			fManager.endQuery(deltaObjectId, token);
			bufferwriteindex = queryResult.getCurrentPage();
			assertEquals(null, bufferwriteindex);
			bufferwriteindisk = queryResult.getBufferwriteDataInDisk();
			assertEquals(null, bufferwriteindisk);
			bufferwriteFiles = queryResult.getBufferwriteDataInFiles();
			assertEquals(pairList.size(), bufferwriteFiles.size());
			temp = bufferwriteFiles.get(0);
			// range 1: 2-208
			assertEquals(OverflowChangeType.NO_CHANGE, temp.overflowChangeType);
			assertEquals(2, temp.getStartTime(deltaObjectId));
			assertEquals(208, temp.getEndTime(deltaObjectId));
			// range 2: 202-400
			temp = bufferwriteFiles.get(1);
			assertEquals(OverflowChangeType.NO_CHANGE, temp.overflowChangeType);
			assertEquals(300, temp.getStartTime(deltaObjectId));
			assertEquals(400, temp.getEndTime(deltaObjectId));
			// range 3: 500-600
			temp = bufferwriteFiles.get(2);
			assertEquals(OverflowChangeType.NO_CHANGE, temp.overflowChangeType);
			assertEquals(500, temp.getStartTime(deltaObjectId));
			assertEquals(600, temp.getEndTime(deltaObjectId));
			// range 4: 700-800
			temp = bufferwriteFiles.get(3);
			assertEquals(OverflowChangeType.NO_CHANGE, temp.overflowChangeType);
			assertEquals(700, temp.getStartTime(deltaObjectId));
			assertEquals(800, temp.getEndTime(deltaObjectId));
			overflowData = queryResult.getAllOverflowData();
			assertEquals(null, overflowData.get(0));
			assertEquals(null, overflowData.get(1));
			assertEquals(null, overflowData.get(2));
			assertEquals(null, overflowData.get(3));

			waitToSleep(2000);
			fManager.closeAll();
		} catch (FileNodeManagerException e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}

	@Test
	public void testOverflowEmptyAndMerge() {

		// write tsfile data
		createBufferwriteFile(new Pair<Long, Long>(100L, 100L), deltaObjectId);
		// delete tsfile data
		FileNodeManager fileNodeManager = FileNodeManager.getInstance();
		try {
			fileNodeManager.delete(deltaObjectId, measurementId, 101, dataType);
		} catch (FileNodeManagerException e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
		// check query before merge
		QueryStructure queryStructure;
		String originFilePath = null;
		try {
			int token = fileNodeManager.beginQuery(deltaObjectId);
			queryStructure = fileNodeManager.query(deltaObjectId, measurementId, null, null, null);
			fileNodeManager.endQuery(deltaObjectId, token);
			assertEquals(1, queryStructure.getBufferwriteDataInFiles().size());
			originFilePath = queryStructure.getBufferwriteDataInFiles().get(0).getFilePath();
			IntervalFileNode temp = queryStructure.getBufferwriteDataInFiles().get(0);
			assertEquals(OverflowChangeType.CHANGED, temp.overflowChangeType);
			assertEquals(100, temp.getStartTime(deltaObjectId));
			assertEquals(100, temp.getEndTime(deltaObjectId));
			assertEquals(1, temp.getStartTimeMap().size());
		} catch (FileNodeManagerException e1) {
			e1.printStackTrace();
			fail(e1.getMessage());
		}
		File fileBeforeMerge = new File(originFilePath);
		assertEquals(true, fileBeforeMerge.exists());
		// merge
		try {
			fileNodeManager.mergeAll();
		} catch (FileNodeManagerException e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
		// wait to end of merge
		waitToSleep(3000);
		// check query after merge: no tsfile data and no overflow data
		fileBeforeMerge = new File(originFilePath);
		assertEquals(false, fileBeforeMerge.exists());
		try {
			int token = fileNodeManager.beginQuery(deltaObjectId);
			queryStructure = fileNodeManager.query(deltaObjectId, measurementId, null, null, null);
			fileNodeManager.endQuery(deltaObjectId, token);
			assertEquals(null, queryStructure.getCurrentPage());
			assertEquals(null, queryStructure.getPageList());
			assertEquals(null, queryStructure.getBufferwriteDataInDisk());
			assertEquals(null, queryStructure.getAllOverflowData().get(0));
			assertEquals(null, queryStructure.getAllOverflowData().get(1));
			assertEquals(null, queryStructure.getAllOverflowData().get(2));
			assertEquals(0, queryStructure.getBufferwriteDataInFiles().size());
		} catch (FileNodeManagerException e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
		// add overflow data: time = 5,10 insert operation
		TSRecord record = new TSRecord(5, deltaObjectId);
		DataPoint dataPoint = DataPoint.getDataPoint(dataType, measurementId, String.valueOf(5));
		record.addTuple(dataPoint);
		try {
			fileNodeManager.insert(record);
			record = new TSRecord(10, deltaObjectId);
			dataPoint = DataPoint.getDataPoint(dataType, measurementId, String.valueOf(10));
			record.addTuple(dataPoint);
			fileNodeManager.insert(record);
		} catch (FileNodeManagerException e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
		// check query: only overflow insert data
		try {
			int token = fileNodeManager.beginQuery(deltaObjectId);
			queryStructure = fileNodeManager.query(deltaObjectId, measurementId, null, null, null);
			fileNodeManager.endQuery(deltaObjectId, token);
			assertEquals(null, queryStructure.getPageList());
			assertEquals(null, queryStructure.getCurrentPage());
			assertEquals(null, queryStructure.getBufferwriteDataInDisk());
			assertEquals(0, queryStructure.getBufferwriteDataInFiles().size());
			DynamicOneColumnData insert = (DynamicOneColumnData) queryStructure.getAllOverflowData().get(0);
			assertEquals(2, insert.valueLength);
			assertEquals(5, insert.getTime(0));
			assertEquals(10, insert.getTime(1));
			assertEquals(5, insert.getInt(0));
			assertEquals(10, insert.getInt(1));
		} catch (FileNodeManagerException e) {
			e.printStackTrace();
			fail(e.getMessage());
		}

		// merge: only overflow data
		try {
			fileNodeManager.mergeAll();
			// query check
			Thread thread = new Thread(new Runnable() {

				@Override
				public void run() {
					try {
						Thread.sleep(1000);
						QueryStructure structure = fileNodeManager.query(deltaObjectId, measurementId, null, null,
								null);
						assertEquals(null, structure.getCurrentPage());
						assertEquals(null, structure.getPageList());
						assertEquals(null, structure.getBufferwriteDataInDisk());
						assertEquals(null, structure.getAllOverflowData().get(0));
						assertEquals(1, structure.getBufferwriteDataInFiles().size());
						IntervalFileNode temp = structure.getBufferwriteDataInFiles().get(0);
						assertEquals(5, temp.getStartTime(deltaObjectId));
						assertEquals(10, temp.getEndTime(deltaObjectId));
					} catch (FileNodeManagerException | InterruptedException e) {
						e.printStackTrace();
						fail(e.getMessage());
					}
				}
			});
			thread.start();

		} catch (FileNodeManagerException e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
		// close all
		try {
			Thread.sleep(3000);
		} catch (InterruptedException e1) {
			e1.printStackTrace();
			fail(e1.getMessage());
		}
		try {
			assertEquals(true, fileNodeManager.closeAll());
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
			if (fManager.closeAll()) {

			} else {
				fail("Can't close the filenode manager");
			}
		} catch (FileNodeManagerException e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}

	private void createBufferwriteInMemory(Pair<Long, Long> timePair, String measurementId) {
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
