package cn.edu.tsinghua.iotdb.engine.bufferwrite;

import static org.junit.Assert.assertEquals;

import java.io.File;
import java.io.FileOutputStream;
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
import cn.edu.tsinghua.iotdb.engine.PathUtils;
import cn.edu.tsinghua.iotdb.engine.bufferwriteV2.BufferWriteProcessor;
import cn.edu.tsinghua.iotdb.engine.querycontext.RawSeriesChunk;
import cn.edu.tsinghua.iotdb.utils.EnvironmentUtils;
import cn.edu.tsinghua.iotdb.utils.FileSchemaUtils;
import cn.edu.tsinghua.tsfile.common.conf.TSFileConfig;
import cn.edu.tsinghua.tsfile.common.conf.TSFileDescriptor;
import cn.edu.tsinghua.tsfile.common.exception.ProcessorException;
import cn.edu.tsinghua.tsfile.common.utils.Pair;
import cn.edu.tsinghua.tsfile.file.metadata.TimeSeriesChunkMetaData;
import cn.edu.tsinghua.tsfile.file.metadata.enums.TSDataType;
import cn.edu.tsinghua.tsfile.timeseries.readV2.datatype.TimeValuePair;
import cn.edu.tsinghua.tsfile.timeseries.write.exception.WriteProcessException;

public class BufferWriteProcessorTest {

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

	private int groupSizeInByte;
	private TSFileConfig TsFileConf = TSFileDescriptor.getInstance().getConfig();
	private Map<String, Object> parameters = new HashMap<>();
	private BufferWriteProcessor bufferwrite;
	private String deltaObjectId = "root.vehicle.d0";
	private String measurementId = "s0";
	private TSDataType dataType = TSDataType.INT32;

	private String insertPath = "insertPath";

	@Before
	public void setUp() throws Exception {
		parameters.put(FileNodeConstants.BUFFERWRITE_FLUSH_ACTION, bfflushaction);
		parameters.put(FileNodeConstants.BUFFERWRITE_CLOSE_ACTION, bfcloseaction);
		parameters.put(FileNodeConstants.FILENODE_PROCESSOR_FLUSH_ACTION, fnflushaction);
		// origin value
		groupSizeInByte = TsFileConf.groupSizeInByte;
		// new value
		TsFileConf.groupSizeInByte = 1024;
		// init metadata
		MetadataManagerHelper.initMetadata();
		EnvironmentUtils.envSetUp();
	}

	@After
	public void tearDown() throws Exception {
		// recovery value
		TsFileConf.groupSizeInByte = groupSizeInByte;
		// clean environment
		EnvironmentUtils.cleanEnv();
		EnvironmentUtils.cleanDir(insertPath);
	}

	@Test
	public void testWriteAndAbnormalRecover()
			throws WriteProcessException, InterruptedException, IOException, ProcessorException {
		bufferwrite = new BufferWriteProcessor(deltaObjectId, insertPath, parameters,
				FileSchemaUtils.constructFileSchema(deltaObjectId));
		for (int i = 1; i < 100; i++) {
			bufferwrite.write(deltaObjectId, measurementId, i, dataType, String.valueOf(i));
		}
		// waiting for the end of flush
		TimeUnit.SECONDS.sleep(2);
		File dataFile = PathUtils.getBufferWriteDir(deltaObjectId);
		// check file
		String restoreFilePath = insertPath + ".restore";
		File restoreFile = new File(dataFile, restoreFilePath);
		assertEquals(true, restoreFile.exists());
		File insertFile = new File(dataFile, insertPath);
		long insertFileLength = insertFile.length();
		FileOutputStream fileOutputStream = new FileOutputStream(insertFile.getPath(), true);
		fileOutputStream.write(new byte[20]);
		fileOutputStream.close();
		assertEquals(insertFileLength + 20, insertFile.length());
		// copy restore file
		File file = new File("temp");
		restoreFile.renameTo(file);
		bufferwrite.close();
		file.renameTo(restoreFile);
		BufferWriteProcessor bufferWriteProcessor = new BufferWriteProcessor(deltaObjectId, insertPath, parameters,
				FileSchemaUtils.constructFileSchema(deltaObjectId));
		assertEquals(true, insertFile.exists());
		assertEquals(insertFileLength, insertFile.length());
		Pair<RawSeriesChunk, List<TimeSeriesChunkMetaData>> pair = bufferWriteProcessor
				.queryBufferwriteData(deltaObjectId, measurementId, dataType);
		assertEquals(true, pair.left.isEmpty());
		assertEquals(1, pair.right.size());
		TimeSeriesChunkMetaData chunkMetaData = pair.right.get(0);
		assertEquals(measurementId, chunkMetaData.getProperties().getMeasurementUID());
		assertEquals(dataType, chunkMetaData.getVInTimeSeriesChunkMetaData().getDataType());
		bufferWriteProcessor.close();
		assertEquals(false, restoreFile.exists());
	}

	@Test
	public void testWriteAndNormalRecover() throws WriteProcessException, ProcessorException, InterruptedException {
		bufferwrite = new BufferWriteProcessor(deltaObjectId, insertPath, parameters,
				FileSchemaUtils.constructFileSchema(deltaObjectId));
		for (int i = 1; i < 100; i++) {
			bufferwrite.write(deltaObjectId, measurementId, i, dataType, String.valueOf(i));
		}
		// waiting for the end of flush
		TimeUnit.SECONDS.sleep(2);
		File dataFile = PathUtils.getBufferWriteDir(deltaObjectId);
		// check file
		String restoreFilePath = insertPath + ".restore";
		File restoreFile = new File(dataFile, restoreFilePath);
		assertEquals(true, restoreFile.exists());
		BufferWriteProcessor bufferWriteProcessor = new BufferWriteProcessor(deltaObjectId, insertPath, parameters,
				FileSchemaUtils.constructFileSchema(deltaObjectId));
		Pair<RawSeriesChunk, List<TimeSeriesChunkMetaData>> pair = bufferWriteProcessor
				.queryBufferwriteData(deltaObjectId, measurementId, dataType);
		assertEquals(true, pair.left.isEmpty());
		assertEquals(1, pair.right.size());
		TimeSeriesChunkMetaData chunkMetaData = pair.right.get(0);
		assertEquals(measurementId, chunkMetaData.getProperties().getMeasurementUID());
		assertEquals(dataType, chunkMetaData.getVInTimeSeriesChunkMetaData().getDataType());
		bufferWriteProcessor.close();
		bufferwrite.close();
		assertEquals(false, restoreFile.exists());
	}

	@Test
	public void testWriteAndQuery() throws WriteProcessException, InterruptedException, ProcessorException {
		bufferwrite = new BufferWriteProcessor(deltaObjectId, insertPath, parameters,
				FileSchemaUtils.constructFileSchema(deltaObjectId));
		assertEquals(false, bufferwrite.isFlush());
		assertEquals(true, bufferwrite.canBeClosed());
		assertEquals(0, bufferwrite.memoryUsage());
		assertEquals(0, bufferwrite.getFileSize());
		assertEquals(0, bufferwrite.getMetaSize());
		for (int i = 1; i <= 85; i++) {
			bufferwrite.write(deltaObjectId, measurementId, i, dataType, String.valueOf(i));
			assertEquals(i * 12, bufferwrite.memoryUsage());
		}
		bufferwrite.write(deltaObjectId, measurementId, 86, dataType, String.valueOf(86));
		assertEquals(true, bufferwrite.isFlush());
		// sleep to the end of flush
		TimeUnit.SECONDS.sleep(2);
		assertEquals(false, bufferwrite.isFlush());
		assertEquals(0, bufferwrite.memoryUsage());
		// query result
		Pair<RawSeriesChunk, List<TimeSeriesChunkMetaData>> pair = bufferwrite.queryBufferwriteData(deltaObjectId,
				measurementId, dataType);
		assertEquals(true, pair.left.isEmpty());
		assertEquals(1, pair.right.size());
		TimeSeriesChunkMetaData chunkMetaData = pair.right.get(0);
		assertEquals(measurementId, chunkMetaData.getProperties().getMeasurementUID());
		assertEquals(dataType, chunkMetaData.getVInTimeSeriesChunkMetaData().getDataType());
		for (int i = 87; i <= 100; i++) {
			bufferwrite.write(deltaObjectId, measurementId, i, dataType, String.valueOf(i));
			assertEquals((i - 86) * 12, bufferwrite.memoryUsage());
		}
		pair = bufferwrite.queryBufferwriteData(deltaObjectId, measurementId, dataType);
		RawSeriesChunk rawSeriesChunk = pair.left;
		assertEquals(false, rawSeriesChunk.isEmpty());
		assertEquals(87, rawSeriesChunk.getMinTimestamp());
		assertEquals(87, rawSeriesChunk.getMinValue().getInt());
		assertEquals(100, rawSeriesChunk.getMaxTimestamp());
		assertEquals(100, rawSeriesChunk.getMaxValue().getInt());
		Iterator<TimeValuePair> iterator = rawSeriesChunk.getIterator();
		for (int i = 87; i <= 100; i++) {
			iterator.hasNext();
			TimeValuePair timeValuePair = iterator.next();
			assertEquals(i, timeValuePair.getTimestamp());
			assertEquals(i, timeValuePair.getValue().getInt());
		}
		bufferwrite.close();
	}
}
