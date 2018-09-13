package cn.edu.tsinghua.iotdb.engine.filenodeV2;

import static org.junit.Assert.*;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import cn.edu.tsinghua.iotdb.conf.directories.Directories;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import cn.edu.tsinghua.iotdb.conf.TsfileDBConfig;
import cn.edu.tsinghua.iotdb.conf.TsfileDBDescriptor;
import cn.edu.tsinghua.iotdb.engine.MetadataManagerHelper;
import cn.edu.tsinghua.iotdb.engine.bufferwrite.BufferWriteProcessor;
import cn.edu.tsinghua.iotdb.engine.filenode.FileNodeProcessor;
import cn.edu.tsinghua.iotdb.engine.overflow.ioV2.OverflowProcessor;
import cn.edu.tsinghua.iotdb.engine.querycontext.GlobalSortedSeriesDataSource;
import cn.edu.tsinghua.iotdb.engine.querycontext.OverflowSeriesDataSource;
import cn.edu.tsinghua.iotdb.engine.querycontext.QueryDataSource;
import cn.edu.tsinghua.iotdb.engine.querycontext.RawSeriesChunk;
import cn.edu.tsinghua.iotdb.engine.querycontext.UpdateDeleteInfoOfOneSeries;
import cn.edu.tsinghua.iotdb.exception.FileNodeProcessorException;
import cn.edu.tsinghua.iotdb.utils.EnvironmentUtils;
import cn.edu.tsinghua.tsfile.common.conf.TSFileConfig;
import cn.edu.tsinghua.tsfile.common.conf.TSFileDescriptor;
import cn.edu.tsinghua.tsfile.file.metadata.enums.TSDataType;
import cn.edu.tsinghua.tsfile.timeseries.readV2.datatype.TimeValuePair;
import cn.edu.tsinghua.tsfile.timeseries.write.record.DataPoint;
import cn.edu.tsinghua.tsfile.timeseries.write.record.TSRecord;

public class FileNodeProcessorTest {

	private FileNodeProcessor fileNodeProcessor;
	private String processorName = "root.vehicle.d0";
	private String measurementId = "s0";
	private String measurementId1 = "s1";
	private TSDataType dataType = TSDataType.INT32;
	private TSDataType daType1 = TSDataType.INT64;
	private Map<String, Object> parameters = null;
	private int groupThreshold;
	private TSFileConfig config = TSFileDescriptor.getInstance().getConfig();
	private TsfileDBConfig dbconfig = TsfileDBDescriptor.getInstance().getConfig();

	@Before
	public void setUp() throws Exception {
		groupThreshold = config.groupSizeInByte;
		config.groupSizeInByte = 1024;
		parameters = new HashMap<>();
		MetadataManagerHelper.initMetadata();
		EnvironmentUtils.envSetUp();
	}

	@After
	public void tearDown() throws Exception {
		config.groupSizeInByte = groupThreshold;
		fileNodeProcessor.delete();
		EnvironmentUtils.cleanEnv();
	}

	@Test
	public void testInsertAndQuery() throws Exception {
		fileNodeProcessor = new FileNodeProcessor(dbconfig.fileNodeDir, processorName);
		fileNodeProcessor.close();
		assertEquals(false, fileNodeProcessor.shouldRecovery());
		assertEquals(false, fileNodeProcessor.isOverflowed());
		assertEquals(-1, fileNodeProcessor.getLastUpdateTime(processorName));
		for (int i = 1; i <= 100; i++) {
			long flushLastUpdateTime = fileNodeProcessor.getFlushLastUpdateTime(processorName);
			if (i < flushLastUpdateTime) {
				// overflow
				fail("overflow data");
			} else {
				// bufferwrite
				BufferWriteProcessor bufferWriteProcessor = fileNodeProcessor.getBufferWriteProcessor(processorName,
						i + System.currentTimeMillis());
				if (bufferWriteProcessor.isNewProcessor()) {
					bufferWriteProcessor.setNewProcessor(false);
					String bufferwriteRelativePath = bufferWriteProcessor.getFileRelativePath();
					fileNodeProcessor.addIntervalFileNode(i, Directories.getInstance().getFolderForTest(), bufferwriteRelativePath);
				}
				bufferWriteProcessor.write(processorName, measurementId, i, dataType, String.valueOf(i));
				fileNodeProcessor.setIntervalFileNodeStartTime(processorName);
				fileNodeProcessor.setLastUpdateTime(processorName, i);
				assertEquals(true, fileNodeProcessor.hasBufferwriteProcessor());
			}
			if (i == 85) {
				// all buffer-write data in memory
			} else if (i == 87) {
				// the groupSize is 1024Bytes. The size of one INT32 data-point
				// is 12Bytes.
				// the flush will be triggered when the number of insert data
				// reaches 86(1024/12=85.33).
				// waiting for the end of asynchronous flush.
				TimeUnit.SECONDS.sleep(2);
				// query result contains the flushed result.
				fileNodeProcessor.getOverflowProcessor(processorName);
				assertEquals(true, fileNodeProcessor.hasBufferwriteProcessor());
				QueryDataSource dataSource = fileNodeProcessor.query(processorName, measurementId, null);
				// overflow data | no overflow data
				OverflowSeriesDataSource overflowSeriesDataSource = dataSource.getOverflowSeriesDataSource();
				assertEquals(processorName + "." + measurementId,
						overflowSeriesDataSource.getSeriesPath().getFullPath());
				assertEquals(dataType, overflowSeriesDataSource.getDataType());
				assertEquals(1, overflowSeriesDataSource.getOverflowInsertFileList().size());
				assertEquals(0, overflowSeriesDataSource.getOverflowInsertFileList().get(0)
						.getTimeSeriesChunkMetaDatas().size());
				assertEquals(true, overflowSeriesDataSource.getRawSeriesChunk().isEmpty());
				UpdateDeleteInfoOfOneSeries deleteInfoOfOneSeries = overflowSeriesDataSource
						.getUpdateDeleteInfoOfOneSeries();
				assertEquals(dataType, deleteInfoOfOneSeries.getDataType());
				assertEquals(null, deleteInfoOfOneSeries.getOverflowUpdateInMem());
				assertEquals(1, deleteInfoOfOneSeries.getOverflowUpdateFileList().size());
				assertEquals(0, deleteInfoOfOneSeries.getOverflowUpdateFileList().get(0)
						.getTimeSeriesChunkMetaDataList().size());
				// bufferwrite data | sorted tsfile data
				GlobalSortedSeriesDataSource globalSortedSeriesDataSource = dataSource.getSeriesDataSource();
				assertEquals(processorName + "." + measurementId,
						globalSortedSeriesDataSource.getSeriesPath().toString());
				assertEquals(0, globalSortedSeriesDataSource.getSealedTsFiles().size());
				assertEquals(1, globalSortedSeriesDataSource.getUnsealedTsFile().getTimeSeriesChunkMetaDatas().size());
				assertEquals(false, globalSortedSeriesDataSource.getRawSeriesChunk().isEmpty());
				assertEquals(87, globalSortedSeriesDataSource.getRawSeriesChunk().getMaxTimestamp());
				assertEquals(87, globalSortedSeriesDataSource.getRawSeriesChunk().getMinTimestamp());
				assertEquals(87, globalSortedSeriesDataSource.getRawSeriesChunk().getMaxValue().getInt());
			}
		}
		// the flush last update time is 87
		// insert overflow data, whose time range is from 0 to 86.
		for (int i = 1; i <= 100; i++) {
			// System.out.println(i);
			long flushLastUpdateTime = fileNodeProcessor.getFlushLastUpdateTime(processorName);
			if (i <= 85) {
				if (i == 66) {
					// waiting the end of flush overflow data.
					TimeUnit.SECONDS.sleep(2);
				}
				assertEquals(86, flushLastUpdateTime);
				if (i < flushLastUpdateTime) {
					// insert value whose data-type is INT64.
					OverflowProcessor overflowProcessor = fileNodeProcessor.getOverflowProcessor(processorName);
					TSRecord tsRecord = new TSRecord(i, processorName);
					tsRecord.addTuple(DataPoint.getDataPoint(daType1, measurementId1, String.valueOf(i)));
					overflowProcessor.insert(tsRecord);
					fileNodeProcessor.changeTypeToChanged(processorName, i);
					fileNodeProcessor.setOverflowed(true);
				} else {
					fail("bufferwrite data");
				}
			} else {
				// i>=86
				if (i == 86) {
					if (i >= flushLastUpdateTime) {
						// query and insert time = 86
						QueryDataSource dataSource = fileNodeProcessor.query(processorName, measurementId1, null);
						// insert overflow data
						RawSeriesChunk rawSeriesChunk = dataSource.getOverflowSeriesDataSource().getRawSeriesChunk();
						assertEquals(false, rawSeriesChunk.isEmpty());
						assertEquals(daType1, rawSeriesChunk.getDataType());
						assertEquals(66, rawSeriesChunk.getMinTimestamp());
						assertEquals(85, rawSeriesChunk.getMaxTimestamp());
						Iterator<TimeValuePair> iterator = rawSeriesChunk.getIterator();
						for (int j = 66; j <= 85; j++) {
							iterator.hasNext();
							TimeValuePair pair = iterator.next();
							assertEquals(j, pair.getTimestamp());
							assertEquals(j, pair.getValue().getLong());
						}
						// insert time = 86 overflow data
						OverflowProcessor overflowProcessor = fileNodeProcessor.getOverflowProcessor(processorName);
						TSRecord tsRecord = new TSRecord(i, processorName);
						tsRecord.addTuple(DataPoint.getDataPoint(daType1, measurementId1, String.valueOf(i)));
						overflowProcessor.insert(tsRecord);
						fileNodeProcessor.changeTypeToChanged(processorName, i);
						fileNodeProcessor.setOverflowed(true);
					} else {
						fail("bufferwrite data" + i + "flushLastUpdateTime" + flushLastUpdateTime);
					}
				} else {
					// i>=87
					if (i < flushLastUpdateTime) {
						fail("overflow data" + i + "flushLastUpdateTime" + flushLastUpdateTime);
					} else {
						// waiting the end of overflow flush.
						// TimeUnit.SECONDS.sleep(2);
						// query data
						QueryDataSource dataSource = fileNodeProcessor.query(processorName, measurementId1, null);
						RawSeriesChunk rawSeriesChunk = dataSource.getOverflowSeriesDataSource().getRawSeriesChunk();
						assertEquals(false, rawSeriesChunk.isEmpty());
						assertEquals(1, dataSource.getOverflowSeriesDataSource().getOverflowInsertFileList().size());
						assertEquals(1, dataSource.getOverflowSeriesDataSource().getOverflowInsertFileList().get(0)
								.getTimeSeriesChunkMetaDatas().size());
						// bufferwrite data
						dataSource = fileNodeProcessor.query(processorName, measurementId, null);
						rawSeriesChunk = dataSource.getSeriesDataSource().getRawSeriesChunk();
						assertEquals(false, rawSeriesChunk.isEmpty());
						Iterator<TimeValuePair> iterator = rawSeriesChunk.getIterator();
						for (int j = 87; j <= 100; j++) {
							iterator.hasNext();
							TimeValuePair timeValuePair = iterator.next();
							assertEquals(j, timeValuePair.getTimestamp());
							assertEquals(j, timeValuePair.getValue().getInt());
						}
					}
				}

			}
		}
		QueryDataSource dataSource = fileNodeProcessor.query(processorName, measurementId1, null);
		GlobalSortedSeriesDataSource globalSortedSeriesDataSource = dataSource.getSeriesDataSource();
		RawSeriesChunk rawSeriesChunk = globalSortedSeriesDataSource.getRawSeriesChunk();
		assertEquals(true, rawSeriesChunk.isEmpty());
		// Iterator<TimeValuePair> iterator = rawSeriesChunk.getIterator();
		// for (int j = 87; j <= 100; j++) {
		// iterator.hasNext();
		// TimeValuePair timeValuePair = iterator.next();
		// assertEquals(j, timeValuePair.getTimestamp());
		// assertEquals(j, timeValuePair.getValue().getLong());
		// }
		fileNodeProcessor.closeBufferWrite();
		fileNodeProcessor.closeOverflow();
		assertEquals(true, fileNodeProcessor.canBeClosed());
		fileNodeProcessor.close();
	}

	@Test
	public void testQueryToken() {
		try {
			fileNodeProcessor = new FileNodeProcessor(dbconfig.fileNodeDir, processorName);
			fileNodeProcessor.writeLock();
			int token = fileNodeProcessor.addMultiPassLock();
			assertEquals(0, token);
			assertEquals(false, fileNodeProcessor.canBeClosed());
			fileNodeProcessor.removeMultiPassLock(token);
			assertEquals(true, fileNodeProcessor.canBeClosed());

			token = fileNodeProcessor.addMultiPassLock();
			assertEquals(0, token);
			int token2 = fileNodeProcessor.addMultiPassLock();
			assertEquals(1, token2);
			fileNodeProcessor.removeMultiPassLock(token2);
			assertEquals(false, fileNodeProcessor.canBeClosed());
			fileNodeProcessor.removeMultiPassLock(token);
			fileNodeProcessor.close();
		} catch (FileNodeProcessorException e) {
			e.printStackTrace();
			fail(e.getMessage());
		}

	}

}
