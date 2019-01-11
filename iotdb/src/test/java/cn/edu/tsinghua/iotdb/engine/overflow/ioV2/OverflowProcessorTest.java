package cn.edu.tsinghua.iotdb.engine.overflow.ioV2;

import static org.junit.Assert.assertEquals;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import cn.edu.tsinghua.iotdb.utils.TimeValuePair;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import cn.edu.tsinghua.iotdb.engine.PathUtils;
import cn.edu.tsinghua.iotdb.engine.bufferwrite.Action;
import cn.edu.tsinghua.iotdb.engine.bufferwrite.FileNodeConstants;
import cn.edu.tsinghua.iotdb.engine.querycontext.MergeSeriesDataSource;
import cn.edu.tsinghua.iotdb.engine.querycontext.OverflowSeriesDataSource;
import cn.edu.tsinghua.iotdb.exception.OverflowProcessorException;
import cn.edu.tsinghua.iotdb.utils.EnvironmentUtils;

public class OverflowProcessorTest {

	private String processorName = "test";
	private OverflowProcessor processor = null;
	private Map<String, Action> parameters = null;

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

	@Before
	public void setUp() throws Exception {
		EnvironmentUtils.envSetUp();
		parameters = new HashMap<String, Action>();
		parameters.put(FileNodeConstants.OVERFLOW_FLUSH_ACTION, overflowflushaction);
		parameters.put(FileNodeConstants.FILENODE_PROCESSOR_FLUSH_ACTION, filenodeflushaction);
	}

	@After
	public void tearDown() throws Exception {
		EnvironmentUtils.cleanEnv();
	}

	@Test
	public void testInsertUpdate() throws IOException, OverflowProcessorException, InterruptedException {
		processor = new OverflowProcessor(processorName, parameters, OverflowTestUtils.getFileSchema());
		assertEquals(true, new File(PathUtils.getOverflowWriteDir(processorName), "0").exists());
		assertEquals(false, processor.isFlush());
		assertEquals(false, processor.isMerge());
		// write update data
		OverflowSeriesDataSource overflowSeriesDataSource = processor.query(OverflowTestUtils.deviceId1,
				OverflowTestUtils.measurementId1, null, OverflowTestUtils.dataType1);
		assertEquals(OverflowTestUtils.dataType1, overflowSeriesDataSource.getDataType());
		assertEquals(true, overflowSeriesDataSource.getReadableMemChunk().isEmpty());
		assertEquals(1, overflowSeriesDataSource.getOverflowInsertFileList().size());
		assertEquals(0,
				overflowSeriesDataSource.getOverflowInsertFileList().get(0).getChunkMetaDataList().size());
		processor.flush();
		assertEquals(false, processor.isFlush());
		assertEquals(false, processor.isMerge());
		// write insert data
		OverflowTestUtils.produceInsertData(processor);
		TimeUnit.SECONDS.sleep(1);
		assertEquals(false, processor.isFlush());
		overflowSeriesDataSource = processor.query(OverflowTestUtils.deviceId1, OverflowTestUtils.measurementId1,
				null, OverflowTestUtils.dataType1);
		assertEquals(OverflowTestUtils.dataType1, overflowSeriesDataSource.getDataType());
		assertEquals(false, overflowSeriesDataSource.getReadableMemChunk().isEmpty());
		assertEquals(1, overflowSeriesDataSource.getOverflowInsertFileList().size());
		Iterator<TimeValuePair> iterator = overflowSeriesDataSource.getReadableMemChunk().getIterator();
		for (int i = 1; i <= 3; i++) {
			assertEquals(true,iterator.hasNext());
			TimeValuePair pair = iterator.next();
			assertEquals(i, pair.getTimestamp());
			assertEquals(i, pair.getValue().getInt());
		}
		// flush synchronously
		processor.close();
		overflowSeriesDataSource = processor.query(OverflowTestUtils.deviceId1, OverflowTestUtils.measurementId1,
				null, OverflowTestUtils.dataType1);
		assertEquals(true, overflowSeriesDataSource.getReadableMemChunk().isEmpty());
		assertEquals(1, overflowSeriesDataSource.getOverflowInsertFileList().size());
		assertEquals(1,
				overflowSeriesDataSource.getOverflowInsertFileList().get(0).getChunkMetaDataList().size());
		processor.switchWorkToMerge();
		overflowSeriesDataSource = processor.query(OverflowTestUtils.deviceId1, OverflowTestUtils.measurementId1,
				null, OverflowTestUtils.dataType1);
		assertEquals(2, overflowSeriesDataSource.getOverflowInsertFileList().size());
		assertEquals(1,
				overflowSeriesDataSource.getOverflowInsertFileList().get(0).getChunkMetaDataList().size());
		assertEquals(0,
				overflowSeriesDataSource.getOverflowInsertFileList().get(1).getChunkMetaDataList().size());
		assertEquals(true, processor.isMerge());
		assertEquals(false, processor.canBeClosed());
		MergeSeriesDataSource mergeSeriesDataSource = processor.queryMerge(OverflowTestUtils.deviceId1,
				OverflowTestUtils.measurementId1, OverflowTestUtils.dataType1);
		assertEquals(1, mergeSeriesDataSource.getInsertFile().getChunkMetaDataList().size());
		processor.switchMergeToWork();
		overflowSeriesDataSource = processor.query(OverflowTestUtils.deviceId1, OverflowTestUtils.measurementId1,
				null, OverflowTestUtils.dataType1);
		processor.close();
		processor.clear();
	}

	@Test
	public void testWriteMemoryAndQuery() throws IOException, OverflowProcessorException {
		processor = new OverflowProcessor(processorName, parameters, OverflowTestUtils.getFileSchema());
		OverflowTestUtils.produceInsertData(processor);
		processor.close();
		// test query
		OverflowSeriesDataSource overflowSeriesDataSource = processor.query(OverflowTestUtils.deviceId1,
				OverflowTestUtils.measurementId1, null, OverflowTestUtils.dataType2);
		assertEquals(true, overflowSeriesDataSource.getReadableMemChunk().isEmpty());
		assertEquals(0,
				overflowSeriesDataSource.getOverflowInsertFileList().get(0).getChunkMetaDataList().size());
		processor.clear();
	}

	@Test
	public void testFlushAndQuery() throws IOException, OverflowProcessorException {
		processor = new OverflowProcessor(processorName, parameters, OverflowTestUtils.getFileSchema());
		processor.flush();
		// waiting for the end of flush.
		try {
			TimeUnit.SECONDS.sleep(1);
		} catch (InterruptedException e) {
		}
		processor.query(OverflowTestUtils.deviceId1, OverflowTestUtils.measurementId1, null,
				OverflowTestUtils.dataType1);
		OverflowTestUtils.produceInsertData(processor);
		processor.query(OverflowTestUtils.deviceId1, OverflowTestUtils.measurementId1, null,
				OverflowTestUtils.dataType2);
		processor.close();
		processor.clear();
	}

	@Test
	public void testRecovery() throws OverflowProcessorException, IOException {
		processor = new OverflowProcessor(processorName, parameters, OverflowTestUtils.getFileSchema());
		processor.close();
		processor.switchWorkToMerge();
		assertEquals(true, processor.isMerge());
		processor.clear();
		OverflowProcessor overflowProcessor = new OverflowProcessor(processorName, parameters,
				OverflowTestUtils.getFileSchema());
		// recovery query
		assertEquals(false, overflowProcessor.isMerge());
		overflowProcessor.switchWorkToMerge();
		OverflowSeriesDataSource overflowSeriesDataSource = overflowProcessor.query(OverflowTestUtils.deviceId1,
				OverflowTestUtils.measurementId1, null, OverflowTestUtils.dataType1);
		assertEquals(true, overflowSeriesDataSource.getReadableMemChunk().isEmpty());
		assertEquals(2, overflowSeriesDataSource.getOverflowInsertFileList().size());
		overflowProcessor.switchMergeToWork();
		overflowProcessor.close();
		overflowProcessor.clear();
	}
}
