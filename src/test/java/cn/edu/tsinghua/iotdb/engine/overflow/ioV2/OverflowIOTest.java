package cn.edu.tsinghua.iotdb.engine.overflow.ioV2;

import static org.junit.Assert.assertEquals;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.List;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import cn.edu.tsinghua.iotdb.engine.overflow.IntervalTreeOperation;
import cn.edu.tsinghua.iotdb.engine.overflow.metadata.OFRowGroupListMetadata;
import cn.edu.tsinghua.tsfile.common.utils.BytesUtils;
import cn.edu.tsinghua.tsfile.file.metadata.TimeSeriesChunkMetaData;
import cn.edu.tsinghua.tsfile.file.metadata.enums.TSDataType;
import cn.edu.tsinghua.tsfile.timeseries.read.TsRandomAccessLocalFileReader;
import cn.edu.tsinghua.tsfile.timeseries.read.query.DynamicOneColumnData;

public class OverflowIOTest {

	private String overflowFilePath = "overflowfile";
	private OverflowIO io = null;
	private TsRandomAccessLocalFileReader reader = null;

	private String deltaObjectId1 = "d1";
	private String deltaObjectId2 = "d2";
	private String measurementId1 = "s1";
	private String measurementId2 = "s2";
	private TSDataType dataType1 = TSDataType.INT32;
	private TSDataType dataType2 = TSDataType.FLOAT;
	private float error = 0.000001f;

	@Before
	public void setUp() throws Exception {
		io = new OverflowIO(overflowFilePath, 0, false);
		reader = new TsRandomAccessLocalFileReader(overflowFilePath);
	}

	@After
	public void tearDown() throws Exception {
		io.close();
		reader.close();
		File file = new File(overflowFilePath);
		file.delete();
	}

	@Test
	public void testFlushReadOneOverflowSeriesImpl() throws IOException {
		OverflowSeriesImpl index = new OverflowSeriesImpl("s1", TSDataType.INT32);
		index.update(2, 8, BytesUtils.intToBytes(5));
		index.update(4, 20, BytesUtils.intToBytes(20));
		index.update(30, 40, BytesUtils.intToBytes(30));
		index.update(50, 60, BytesUtils.intToBytes(40));
		DynamicOneColumnData originData = index.query(null, null, null, null, null);
		assertEquals(2, originData.getTime(0));
		assertEquals(3, originData.getTime(1));
		assertEquals(4, originData.getTime(2));
		assertEquals(20, originData.getTime(3));
		assertEquals(30, originData.getTime(4));
		assertEquals(40, originData.getTime(5));
		assertEquals(50, originData.getTime(6));
		assertEquals(60, originData.getTime(7));
		// assert value
		assertEquals(5, originData.getInt(0));
		assertEquals(20, originData.getInt(1));
		assertEquals(30, originData.getInt(2));
		assertEquals(40, originData.getInt(3));
		// flush index
		assertEquals(TSDataType.INT32, index.getDataType());
		assertEquals(4, index.getValueCount());
		assertEquals(60, index.getStatistics().getMax().longValue());
		assertEquals(0, index.getStatistics().getMin().longValue());
		TimeSeriesChunkMetaData chunkMetadata = io.flush(index);
		assertEquals(TSDataType.INT32, chunkMetadata.getVInTimeSeriesChunkMetaData().getDataType());
		assertEquals("s1", chunkMetadata.getProperties().getMeasurementUID());
		// query index
		InputStream input = OverflowIO.readOneTimeSeriesChunk(chunkMetadata, reader);
		IntervalTreeOperation overflowIndex = new IntervalTreeOperation(TSDataType.INT32);
		DynamicOneColumnData one = new DynamicOneColumnData(TSDataType.INT32);
		one = overflowIndex.queryFileBlock(null, null, null, input, one);
		assertEquals(2, one.getTime(0));
		assertEquals(3, one.getTime(1));
		assertEquals(4, one.getTime(2));
		assertEquals(20, one.getTime(3));
		assertEquals(30, one.getTime(4));
		assertEquals(40, one.getTime(5));
		assertEquals(50, one.getTime(6));
		assertEquals(60, one.getTime(7));
		// assert value
		assertEquals(5, one.getInt(0));
		assertEquals(20, one.getInt(1));
		assertEquals(30, one.getInt(2));
		assertEquals(40, one.getInt(3));
	}

	@Test
	public void testflushIndexTrees() throws IOException {
		OverflowSupport support = new OverflowSupport();
		OverflowTestUtils.produceUpdateData(support);
		List<OFRowGroupListMetadata> rowGroupListMetadatas = io.flush(support.getOverflowSeriesMap());
		assertEquals(2, rowGroupListMetadatas.size());
		OFRowGroupListMetadata d1 = null;
		OFRowGroupListMetadata d2 = null;
		if (rowGroupListMetadatas.get(0).getDeltaObjectId().equals(deltaObjectId1)) {
			d1 = rowGroupListMetadatas.get(0);
			d2 = rowGroupListMetadatas.get(1);
		} else {
			d1 = rowGroupListMetadatas.get(1);
			d2 = rowGroupListMetadatas.get(0);
		}
		assertEquals(2, d1.getMetaDatas().size());
		TimeSeriesChunkMetaData d1s1metadata = null;
		TimeSeriesChunkMetaData d1s2metadata = null;
		TimeSeriesChunkMetaData d2s1metadata = null;
		TimeSeriesChunkMetaData d2s2metadata = null;
		if (d1.getMetaDatas().get(0).getMeasurementId().equals(measurementId1)) {
			d1s1metadata = d1.getMetaDatas().get(0).getMetaDatas().get(0);
			d1s2metadata = d1.getMetaDatas().get(1).getMetaDatas().get(0);

			d2s1metadata = d2.getMetaDatas().get(0).getMetaDatas().get(0);
			d2s2metadata = d2.getMetaDatas().get(1).getMetaDatas().get(0);
		} else {
			d1s1metadata = d1.getMetaDatas().get(1).getMetaDatas().get(0);
			d1s2metadata = d1.getMetaDatas().get(0).getMetaDatas().get(0);

			d2s1metadata = d2.getMetaDatas().get(1).getMetaDatas().get(0);
			d2s2metadata = d2.getMetaDatas().get(0).getMetaDatas().get(0);
		}

		// d1 s1
		IntervalTreeOperation index = new IntervalTreeOperation(dataType1);
		DynamicOneColumnData d1s1 = index.queryFileBlock(null, null, null,
				OverflowIO.readOneTimeSeriesChunk(d1s1metadata, reader), new DynamicOneColumnData());
		assertEquals(2, d1s1.getTime(0));
		assertEquals(10, d1s1.getTime(1));
		assertEquals(20, d1s1.getTime(2));
		assertEquals(30, d1s1.getTime(3));

		assertEquals(10, d1s1.getInt(0));
		assertEquals(20, d1s1.getInt(1));
		// d1 s2
		index = new IntervalTreeOperation(dataType1);
		DynamicOneColumnData d1s2 = index.queryFileBlock(null, null, null,
				OverflowIO.readOneTimeSeriesChunk(d1s2metadata, reader), new DynamicOneColumnData());
		assertEquals(0, d1s2.getTime(0));
		assertEquals(-10, d1s2.getTime(1));
		assertEquals(20, d1s2.getTime(2));
		assertEquals(30, d1s2.getTime(3));

		assertEquals(0, d1s2.getInt(0));
		assertEquals(20, d1s2.getInt(1));
		// d2 s1
		index = new IntervalTreeOperation(dataType2);
		DynamicOneColumnData d2s1 = index.queryFileBlock(null, null, null,
				OverflowIO.readOneTimeSeriesChunk(d2s1metadata, reader), new DynamicOneColumnData());
		assertEquals(10, d2s1.getTime(0));
		assertEquals(14, d2s1.getTime(1));
		assertEquals(15, d2s1.getTime(2));
		assertEquals(40, d2s1.getTime(3));

		assertEquals(10.5f, d2s1.getFloat(0), error);
		assertEquals(20.5f, d2s1.getFloat(1), error);
		// d2 s2
		index = new IntervalTreeOperation(dataType2);
		DynamicOneColumnData d2s2 = index.queryFileBlock(null, null, null,
				OverflowIO.readOneTimeSeriesChunk(d2s2metadata, reader), new DynamicOneColumnData());

		assertEquals(0, d2s2.getTime(0));
		assertEquals(-20, d2s2.getTime(1));

		assertEquals(0, d2s2.getFloat(0), error);
	}
	
	@Test
	public void testFileCutoff() throws IOException {
		File file = new File("testoverflowfile");
		FileOutputStream fileOutputStream = new FileOutputStream(file);
		byte[] bytes = new byte[20];
		fileOutputStream.write(bytes);
		fileOutputStream.close();
		assertEquals(20, file.length());
		OverflowIO overflowIO = new OverflowIO(file.getPath(), 0, false);
		assertEquals(0, file.length());
		overflowIO.close();
		file.delete();
	}

}
