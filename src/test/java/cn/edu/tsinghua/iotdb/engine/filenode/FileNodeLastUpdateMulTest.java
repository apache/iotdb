package cn.edu.tsinghua.iotdb.engine.filenode;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import cn.edu.tsinghua.iotdb.conf.TsfileDBConfig;
import cn.edu.tsinghua.iotdb.conf.TsfileDBDescriptor;
import cn.edu.tsinghua.iotdb.engine.bufferwrite.Action;
import cn.edu.tsinghua.iotdb.engine.bufferwrite.BufferWriteProcessor;
import cn.edu.tsinghua.iotdb.engine.bufferwrite.FileNodeConstants;
import cn.edu.tsinghua.iotdb.engine.lru.MetadataManagerHelper;
import cn.edu.tsinghua.iotdb.engine.overflow.io.EngineTestHelper;
import cn.edu.tsinghua.iotdb.engine.overflow.io.OverflowProcessor;
import cn.edu.tsinghua.iotdb.exception.BufferWriteProcessorException;
import cn.edu.tsinghua.iotdb.exception.FileNodeProcessorException;
import cn.edu.tsinghua.iotdb.exception.OverflowProcessorException;
import cn.edu.tsinghua.iotdb.exception.PathErrorException;
import cn.edu.tsinghua.iotdb.metadata.MManager;
import cn.edu.tsinghua.iotdb.sys.writelog.WriteLogManager;
import cn.edu.tsinghua.tsfile.common.conf.TSFileConfig;
import cn.edu.tsinghua.tsfile.common.conf.TSFileDescriptor;
import cn.edu.tsinghua.tsfile.common.utils.Pair;
import cn.edu.tsinghua.tsfile.file.metadata.RowGroupMetaData;
import cn.edu.tsinghua.tsfile.file.metadata.enums.CompressionTypeName;
import cn.edu.tsinghua.tsfile.file.metadata.enums.TSDataType;
import cn.edu.tsinghua.tsfile.timeseries.read.query.DynamicOneColumnData;

/**
 * @author liukun
 *
 */
public class FileNodeLastUpdateMulTest {

	private TsfileDBConfig tsdbconfig = TsfileDBDescriptor.getInstance().getConfig();

	private TSFileConfig tsconfig = TSFileDescriptor.getInstance().getConfig();

	private FileNodeProcessor processor = null;

	private String deltaObjectId0 = "root.vehicle.d0";

	private String deltaObjectId2 = "root.vehicle.d2";

	private String deltaObjectId1 = "root.vehicle.d1";

	private String measurementId = "s0";

	private Map<String, Object> parameters = null;

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
		tsconfig.groupSizeInByte = 10000;
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
		MetadataManagerHelper.initMetadata2();
		nameSpacePath = MManager.getInstance().getFileNameByPath(deltaObjectId0);
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
	public void WriteCloseAndQueryTest() throws Exception {

		// write file
		// file1: d0[10,20]
		createBufferwriteFile(10, 20, deltaObjectId0);
		closeBufferWrite();
		// file2: d1[10,20]
		createBufferwriteFile(10, 20, deltaObjectId1);
		closeBufferWrite();
		// file3: d2[10,20]
		createBufferwriteFile(10, 20, deltaObjectId2);
		closeBufferWrite();
		// file4: d0,d1 [30,40]
		createBufferwriteFile(30, 40, deltaObjectId0, deltaObjectId1);
		closeBufferWrite();
		// file5: d0,d1,d2 [50,60]
		createBufferwriteFile(50, 60, deltaObjectId0, deltaObjectId1, deltaObjectId2);
		closeBufferWrite();
		// file6: d0,d1,d2 [70,80....
		createBufferwriteFile(70, 80, deltaObjectId0, deltaObjectId1, deltaObjectId2);

		// query
		try {
			// add overflow
			processor.getOverflowProcessor(nameSpacePath, parameters);
			QueryStructure queryStructure = processor.query(deltaObjectId0, measurementId, null, null, null);

			DynamicOneColumnData CachePage = queryStructure.getCurrentPage();
			Pair<List<ByteArrayInputStream>, CompressionTypeName> pageList = queryStructure.getPageList();
			List<RowGroupMetaData> bufferwriteDataInDisk = queryStructure.getBufferwriteDataInDisk();
			List<IntervalFileNode> bufferwriteDataInFiles = queryStructure.getBufferwriteDataInFiles();
			List<Object> allOverflowData = queryStructure.getAllOverflowData();

			// check overflow
			assertEquals(4, allOverflowData.size());
			assertEquals(null, allOverflowData.get(0));
			assertEquals(null, allOverflowData.get(1));
			assertEquals(null, allOverflowData.get(2));
			assertEquals(null, allOverflowData.get(3));

			// check memory data
			if (CachePage != null) {
				for (ByteArrayInputStream stream : pageList.left) {
					DynamicOneColumnData pagedata = PageTestUtils.pageToDynamic(stream, pageList.right, deltaObjectId0,
							measurementId);
					CachePage.mergeRecord(pagedata);
				}
				assertEquals(11, CachePage.valueLength);
				assertEquals(0, bufferwriteDataInDisk.size());
			} else {
				assertEquals(1, bufferwriteDataInDisk.size());
			}
			/**
			 * check file
			 */
			assertEquals(6, bufferwriteDataInFiles.size());
			// first file
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
			processor.close();
		} catch (FileNodeProcessorException e) {
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
	public void WriteOverflowAndQueryTest() throws Exception {
		WriteCloseAndQueryTest();
		try {
			processor = new FileNodeProcessor(tsdbconfig.fileNodeDir, nameSpacePath, parameters);
			processor.getOverflowProcessor(nameSpacePath, parameters);
			QueryStructure queryStructure = processor.query(deltaObjectId0, measurementId, null, null, null);
			// test origin
			DynamicOneColumnData CachePage = queryStructure.getCurrentPage();
			Pair<List<ByteArrayInputStream>, CompressionTypeName> pageList = queryStructure.getPageList();
			List<IntervalFileNode> bufferwriteDataInFiles = queryStructure.getBufferwriteDataInFiles();
			List<Object> allOverflowData = queryStructure.getAllOverflowData();
			assertEquals(null, CachePage);
			assertEquals(null, pageList);
			IntervalFileNode temp = bufferwriteDataInFiles.get(5);
			assertEquals(3, temp.getStartTimeMap().size());
			assertEquals(3, temp.getEndTimeMap().size());
			assertEquals(70, temp.getStartTime(deltaObjectId0));
			assertEquals(70, temp.getStartTime(deltaObjectId1));
			assertEquals(70, temp.getStartTime(deltaObjectId2));
			assertEquals(80, temp.getEndTime(deltaObjectId0));
			assertEquals(80, temp.getEndTime(deltaObjectId1));
			assertEquals(80, temp.getEndTime(deltaObjectId2));

			// overflow data
			OverflowProcessor ofProcessor = processor.getOverflowProcessor(nameSpacePath, parameters);
			// file 0
			ofProcessor.insert(deltaObjectId0, measurementId, 5, TSDataType.INT32, String.valueOf(5));
			processor.changeTypeToChanged(deltaObjectId0, 5);
			// file 2
			ofProcessor.insert(deltaObjectId2, measurementId, 5, TSDataType.INT32, String.valueOf(5));
			processor.changeTypeToChanged(deltaObjectId2, 5);
			// file 4
			ofProcessor.insert(deltaObjectId1, measurementId, 65, TSDataType.INT32, String.valueOf(65));
			processor.changeTypeToChanged(deltaObjectId1, 65);

			queryStructure = processor.query(deltaObjectId0, measurementId, null, null, null);
			bufferwriteDataInFiles = queryStructure.getBufferwriteDataInFiles();
			allOverflowData = queryStructure.getAllOverflowData();
			DynamicOneColumnData insert = (DynamicOneColumnData) allOverflowData.get(0);
			assertEquals(1, insert.valueLength);
			assertEquals(5, insert.getTime(0));
			assertEquals(5, insert.getInt(0));

			temp = bufferwriteDataInFiles.get(0);
			assertEquals(OverflowChangeType.CHANGED, temp.overflowChangeType);

			temp = bufferwriteDataInFiles.get(2);
			assertEquals(OverflowChangeType.CHANGED, temp.overflowChangeType);

			temp = bufferwriteDataInFiles.get(4);
			assertEquals(OverflowChangeType.CHANGED, temp.overflowChangeType);

			processor.close();
		} catch (FileNodeProcessorException e) {
			e.printStackTrace();
			fail(e.getMessage());
		} catch (OverflowProcessorException e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}

	@Deprecated
	public void FileEmptyMergeAnaWrite() throws Exception {

		createBufferwriteFile(10, 20, deltaObjectId0);
		closeBufferWrite();
		createBufferwriteFile(10, 20, deltaObjectId1);
		closeBufferWrite();
		try {
			processor = new FileNodeProcessor(tsdbconfig.fileNodeDir, nameSpacePath, parameters);
			// deltaObjectId2 empty overflow
			processor.setLastUpdateTime(deltaObjectId2, 100);
			OverflowProcessor ofProcessor = processor.getOverflowProcessor(nameSpacePath, parameters);
			ofProcessor.insert(deltaObjectId2, measurementId, 5, TSDataType.INT32, String.valueOf(5));
			processor.changeTypeToChanged(deltaObjectId2, 5);

			processor.writeLock();
			Thread thread = new Thread(new Runnable() {

				@Override
				public void run() {

					try {
						processor.writeLock();
						QueryStructure queryStructure = processor.query(deltaObjectId0, measurementId, null, null,
								null);
						assertEquals(2, queryStructure.getBufferwriteDataInFiles().size());
						IntervalFileNode temp = queryStructure.getBufferwriteDataInFiles().get(0);
						assertEquals(OverflowChangeType.CHANGED, temp.overflowChangeType);
						assertEquals(10, temp.getStartTime(deltaObjectId0));
						assertEquals(20, temp.getEndTime(deltaObjectId0));
						assertEquals(0, temp.getStartTime(deltaObjectId2));
						assertEquals(100, temp.getEndTime(deltaObjectId2));

						// write bufferwrite data
						// file 2: [200,400...)
						BufferWriteProcessor bfProcessor = processor.getBufferWriteProcessor(nameSpacePath, 200);
						bfProcessor.write(deltaObjectId2, measurementId, 200, TSDataType.INT32, String.valueOf(200));
						processor.addIntervalFileNode(200, bfProcessor.getFileAbsolutePath());
						processor.setIntervalFileNodeStartTime(deltaObjectId2, 200);
						processor.setLastUpdateTime(deltaObjectId2, 200);
						bfProcessor.write(deltaObjectId2, measurementId, 400, TSDataType.INT32, String.valueOf(400));
						processor.setLastUpdateTime(deltaObjectId2, 400);
						// add overflow
						queryStructure = processor.query(deltaObjectId2, measurementId, null, null, null);
						assertEquals(3, queryStructure.getBufferwriteDataInFiles().size());
						temp = queryStructure.getBufferwriteDataInFiles().get(2);
						assertEquals(OverflowChangeType.NO_CHANGE, temp.overflowChangeType);
						assertEquals(200, temp.getStartTime(deltaObjectId2));
						assertEquals(-1, temp.getEndTime(deltaObjectId2));
						OverflowProcessor ofProcessor = processor.getOverflowProcessor(nameSpacePath, parameters);
						ofProcessor.insert(deltaObjectId2, measurementId, 100, TSDataType.INT32, String.valueOf(100));
						processor.changeTypeToChanged(deltaObjectId2, 100);
						// merge changed
						queryStructure = processor.query(deltaObjectId2, measurementId, null, null, null);
						assertEquals(3, queryStructure.getBufferwriteDataInFiles().size());
						temp = queryStructure.getBufferwriteDataInFiles().get(2);
						assertEquals(OverflowChangeType.MERGING_CHANGE, temp.overflowChangeType);

					} catch (FileNodeProcessorException e) {
						e.printStackTrace();
						fail(e.getMessage());
					} catch (BufferWriteProcessorException e) {
						e.printStackTrace();
						fail(e.getMessage());
					} catch (OverflowProcessorException e) {
						e.printStackTrace();
						fail(e.getMessage());
					} catch (Exception e) {
						e.printStackTrace();
						fail(e.getMessage());
					} finally {
						processor.writeUnlock();
					}
				}
			});

			thread.start();
			Thread.sleep(1000);
			processor.merge();

			//
			// query data
			//
			QueryStructure queryStructure = processor.query(deltaObjectId2, measurementId, null, null, null);
			assertEquals(3, queryStructure.getBufferwriteDataInFiles().size());
			IntervalFileNode temp = queryStructure.getBufferwriteDataInFiles().get(1);
			assertEquals(OverflowChangeType.NO_CHANGE, temp.overflowChangeType);
			assertEquals(10, temp.getStartTime(deltaObjectId1));
			assertEquals(20, temp.getEndTime(deltaObjectId1));
			temp = queryStructure.getBufferwriteDataInFiles().get(2);
			assertEquals(OverflowChangeType.CHANGED, temp.overflowChangeType);
			assertEquals(200, temp.getStartTime(deltaObjectId2));
			assertEquals(-1, temp.getEndTime(deltaObjectId2));

			temp = queryStructure.getBufferwriteDataInFiles().get(0);
			assertEquals(OverflowChangeType.CHANGED, temp.overflowChangeType);
			assertEquals(10, temp.getStartTime(deltaObjectId0));
			assertEquals(20, temp.getEndTime(deltaObjectId0));
			assertEquals(5, temp.getStartTime(deltaObjectId2));
			assertEquals(5, temp.getEndTime(deltaObjectId2));

			processor.close();

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

	/**
	 * Write bufferwrite data and close
	 * 
	 * @param begin
	 * @param end
	 * @throws Exception 
	 */
	private void createBufferwriteFile(long begin, long end, String... deltaObjectIds) throws Exception {

		for (int i = 0; i < deltaObjectIds.length; i++) {
			String deltaObjectId = deltaObjectIds[i];
			if (i == 0) {
				try {
					processor = new FileNodeProcessor(tsdbconfig.fileNodeDir, nameSpacePath, parameters);
					BufferWriteProcessor bfProcessor = processor.getBufferWriteProcessor(nameSpacePath, begin);
					assertEquals(true, bfProcessor.isNewProcessor());
					bfProcessor.write(deltaObjectId, measurementId, begin, TSDataType.INT32, String.valueOf(begin));
					bfProcessor.setNewProcessor(false);
					processor.addIntervalFileNode(begin, bfProcessor.getFileAbsolutePath());
					processor.setIntervalFileNodeStartTime(deltaObjectId, begin);
					processor.setLastUpdateTime(deltaObjectId, begin);
				} catch (FileNodeProcessorException e) {
					e.printStackTrace();
					fail(e.getMessage());
				} catch (BufferWriteProcessorException e) {
					e.printStackTrace();
					fail(e.getMessage());
				}
			} else {
				try {
					BufferWriteProcessor bfProcessor = processor.getBufferWriteProcessor(nameSpacePath, begin);
					bfProcessor.write(deltaObjectId, measurementId, begin, TSDataType.INT32, String.valueOf(begin));
					processor.setIntervalFileNodeStartTime(deltaObjectId, begin);
					processor.setLastUpdateTime(deltaObjectId, begin);
				} catch (FileNodeProcessorException e) {
					e.printStackTrace();
					fail(e.getMessage());
				} catch (BufferWriteProcessorException e) {
					e.printStackTrace();
					fail(e.getMessage());
				}
			}
		}

		for (long i = begin + 1; i <= end; i++) {
			for (String deltaObjectId : deltaObjectIds) {
				try {
					BufferWriteProcessor bfProcessor = processor.getBufferWriteProcessor(nameSpacePath, i);
					bfProcessor.write(deltaObjectId, measurementId, i, TSDataType.INT32, String.valueOf(i));
					processor.setIntervalFileNodeStartTime(deltaObjectId, i);
					processor.setLastUpdateTime(deltaObjectId, i);
				} catch (FileNodeProcessorException e) {
					e.printStackTrace();
					fail(e.getMessage());
				} catch (BufferWriteProcessorException e) {
					e.printStackTrace();
					fail(e.getMessage());
				}
			}
		}
	}

	/**
	 * Close the filenode processor
	 */
	private void closeBufferWrite() {

		try {
			processor.close();
		} catch (FileNodeProcessorException e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}
}
