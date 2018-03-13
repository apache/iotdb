package cn.edu.tsinghua.iotdb.engine.bufferwrite;

import static org.junit.Assert.assertEquals;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import cn.edu.tsinghua.iotdb.engine.MetadataManagerHelper;
import cn.edu.tsinghua.iotdb.engine.querycontext.RawSeriesChunk;
import cn.edu.tsinghua.iotdb.exception.BufferWriteProcessorException;
import cn.edu.tsinghua.iotdb.utils.EnvironmentUtils;
import cn.edu.tsinghua.iotdb.utils.FileSchemaUtils;
import cn.edu.tsinghua.tsfile.common.utils.Pair;
import cn.edu.tsinghua.tsfile.file.metadata.TimeSeriesChunkMetaData;
import cn.edu.tsinghua.tsfile.file.metadata.enums.TSDataType;
import cn.edu.tsinghua.tsfile.timeseries.readV2.datatype.TimeValuePair;
import cn.edu.tsinghua.tsfile.timeseries.write.exception.WriteProcessException;

public class BufferWriteProcessorNewTest {

	Action bfflushaction = new Action() {

		@Override
		public void act() throws Exception {

		}
	};

	Action bfcloseaction = new Action() {

		@Override
		public void act() throws Exception {
		}
	};

	Action fnflushaction = new Action() {

		@Override
		public void act() throws Exception {

		}
	};
	Map<String, Object> parameters = new HashMap<>();
	private String processorName = "root.vehicle.d0";
	private String measurementId = "s0";
	private TSDataType dataType = TSDataType.INT32;
	private BufferWriteProcessor bufferwrite;
	private String filename = "tsfile";

	@Before
	public void setUp() throws Exception {
		parameters.put(FileNodeConstants.BUFFERWRITE_FLUSH_ACTION, bfflushaction);
		parameters.put(FileNodeConstants.BUFFERWRITE_CLOSE_ACTION, bfcloseaction);
		parameters.put(FileNodeConstants.FILENODE_PROCESSOR_FLUSH_ACTION, fnflushaction);
		MetadataManagerHelper.initMetadata();
		EnvironmentUtils.envSetUp();
	}

	@After
	public void tearDown() throws Exception {
		bufferwrite.close();
		EnvironmentUtils.cleanEnv();
	}

	@Test
	public void testWriteAndFlush()
			throws BufferWriteProcessorException, WriteProcessException, IOException, InterruptedException {
		bufferwrite = new BufferWriteProcessor(processorName, filename, parameters,
				FileSchemaUtils.constructFileSchema(processorName));
		assertEquals(filename, bufferwrite.getFileName());
		assertEquals(processorName + File.separator + filename, bufferwrite.getFileRelativePath());
		assertEquals(true, bufferwrite.isNewProcessor());
		bufferwrite.setNewProcessor(false);
		assertEquals(false, bufferwrite.isNewProcessor());
		Pair<RawSeriesChunk, List<TimeSeriesChunkMetaData>> pair = bufferwrite.queryBufferwriteData(processorName,
				measurementId, dataType);
		RawSeriesChunk left = pair.left;
		List<TimeSeriesChunkMetaData> right = pair.right;
		assertEquals(true, left.isEmpty());
		assertEquals(0, right.size());
		for (int i = 1; i <= 100; i++) {
			bufferwrite.write(processorName, measurementId, i, dataType, String.valueOf(i));
		}
		// query data in memory
		pair = bufferwrite.queryBufferwriteData(processorName, measurementId, dataType);
		left = pair.left;
		right = pair.right;
		assertEquals(false, left.isEmpty());
		int num = 1;
		Iterator<TimeValuePair> iterator = left.getIterator();
		for (; num <= 100; num++) {
			iterator.hasNext();
			TimeValuePair timeValuePair = iterator.next();
			assertEquals(num, timeValuePair.getTimestamp());
			assertEquals(num, timeValuePair.getValue().getInt());
		}
		assertEquals(false, bufferwrite.isFlush());
		// flush asynchronously
		bufferwrite.flush();
		assertEquals(true, bufferwrite.isFlush());
		assertEquals(true, bufferwrite.canBeClosed());
		// waiting for the end of flush.
		TimeUnit.SECONDS.sleep(1);
		assertEquals(false, bufferwrite.isFlush());
		pair = bufferwrite.queryBufferwriteData(processorName, measurementId, dataType);
		left = pair.left;
		right = pair.right;
		assertEquals(true, left.isEmpty());
		assertEquals(1, right.size());
		assertEquals(measurementId, right.get(0).getProperties().getMeasurementUID());
		assertEquals(dataType, right.get(0).getVInTimeSeriesChunkMetaData().getDataType());

		// test recovery
		BufferWriteProcessor bufferWriteProcessor = new BufferWriteProcessor(processorName, filename, parameters,
				FileSchemaUtils.constructFileSchema(processorName));
		pair = bufferWriteProcessor.queryBufferwriteData(processorName, measurementId, dataType);
		left = pair.left;
		right = pair.right;
		assertEquals(true, left.isEmpty());
		assertEquals(1, right.size());
		assertEquals(measurementId, right.get(0).getProperties().getMeasurementUID());
		assertEquals(dataType, right.get(0).getVInTimeSeriesChunkMetaData().getDataType());
		bufferWriteProcessor.close();
	}
}
