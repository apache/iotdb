package cn.edu.thu.tsfiledb.engine.overflow.io;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.io.File;
import java.io.IOException;
import java.util.List;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import cn.edu.thu.tsfile.common.utils.BytesUtils;
import cn.edu.thu.tsfile.compress.Compressor;
import cn.edu.thu.tsfile.file.metadata.TimeSeriesChunkMetaData;
import cn.edu.thu.tsfile.file.metadata.enums.CompressionTypeName;
import cn.edu.thu.tsfile.file.metadata.enums.TSDataType;
import cn.edu.thu.tsfile.timeseries.read.query.DynamicOneColumnData;
import cn.edu.thu.tsfiledb.conf.TsfileDBConfig;
import cn.edu.thu.tsfiledb.conf.TsfileDBDescriptor;
import cn.edu.thu.tsfiledb.metadata.MManager;
import cn.edu.thu.tsfiledb.sys.writelog.WriteLogManager;

/**
 * @author liukun
 *
 */
public class OverflowSeriesImplTest {

	private String filePath = "overflowseriesimpltest";

	private String mergeFilePath = filePath + ".merge";

	private OverflowReadWriter ofrw = null;
	private OverflowFileIO ofio = null;
	private OverflowSeriesImpl seriesimpl = null;
	private OverflowSeriesImpl mergeseriesimpl = null;
	private String measurementId = "s0";
	private TsfileDBConfig tsFileDBConfig = TsfileDBDescriptor.getInstance().getConfig();

	@Before
	public void setUp() throws Exception {
		EngineTestHelper.delete(mergeFilePath);
		EngineTestHelper.delete(filePath);
		EngineTestHelper.delete(tsFileDBConfig.walFolder);
		EngineTestHelper.delete(tsFileDBConfig.metadataDir);
		WriteLogManager.getInstance().close();
	}

	@After
	public void tearDown() throws Exception {
		MManager.getInstance().flushObjectToFile();
		WriteLogManager.getInstance().close();
		EngineTestHelper.delete(filePath);
		EngineTestHelper.delete(mergeFilePath);
		EngineTestHelper.delete(tsFileDBConfig.walFolder);
		EngineTestHelper.delete(tsFileDBConfig.metadataDir);
	}

	@Test
	public void testMergeAndQuery() {
		try {
			ofrw = new OverflowReadWriter(filePath);
		} catch (IOException e) {
			e.printStackTrace();
			fail("Construct the overflow read write failed");
		}
		try {
			ofio = new OverflowFileIO(ofrw, filePath, 0);
		} catch (IOException e) {
			e.printStackTrace();
			fail("Construct the overflowfile io failed");
		}

		seriesimpl = new OverflowSeriesImpl(measurementId, TSDataType.INT32, ofio,
				Compressor.getCompressor(CompressionTypeName.UNCOMPRESSED), null);
		assertEquals(TSDataType.INT32, seriesimpl.getTSDataType());

		// insert data and flush
		for (int i = 1; i < 11; i++) {
			seriesimpl.insert(i, BytesUtils.intToBytes(i));
		}

		seriesimpl.switchWorkingToFlushing();
		try {
			seriesimpl.flushToFileWriter(ofio);
		} catch (IOException e) {
			e.printStackTrace();
			fail("Flush overflowfileio failed");
		}
		// insert data and flush
		for (int i = 11; i < 21; i++) {
			seriesimpl.insert(i, BytesUtils.intToBytes(i));
		}
		seriesimpl.switchWorkingToFlushing();
		try {
			seriesimpl.flushToFileWriter(ofio);
		} catch (IOException e) {
			e.printStackTrace();
			fail("Flush overflowfileio failed");
		}
		assertEquals(2, seriesimpl.getOFSeriesListMetadata().getMetaDatas().size());
		List<TimeSeriesChunkMetaData> metaForRead = seriesimpl.getOFSeriesListMetadata().getMetaDatas();
		// close file
		try {
			seriesimpl.close();
		} catch (IOException e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
		
		// change file name
		File overflowFile = new File(filePath);
		File overflowMergeFile = new File(mergeFilePath);
		overflowFile.renameTo(overflowMergeFile);
		long lastupdatepostion = overflowMergeFile.length();
		// construct merge serriesimpl
		try {
			ofrw = new OverflowReadWriter(mergeFilePath);
		} catch (IOException e) {
			e.printStackTrace();
			fail("Construct the overflow read write failed");
		}
		try {
			ofio = new OverflowFileIO(ofrw, mergeFilePath, lastupdatepostion);
		} catch (IOException e) {
			e.printStackTrace();
			fail("Construct the overflowfile io failed");
		}
		mergeseriesimpl = new OverflowSeriesImpl(measurementId, TSDataType.INT32, ofio,
				Compressor.getCompressor(CompressionTypeName.UNCOMPRESSED), metaForRead);

		// construct new seriesimpl
		try {
			ofrw = new OverflowReadWriter(filePath);
		} catch (IOException e) {
			e.printStackTrace();
			fail("Construct the overflow read write failed");
		}
		try {
			ofio = new OverflowFileIO(ofrw, filePath, 0);
		} catch (IOException e) {
			e.printStackTrace();
			fail("Construct the overflowfile io failed");
		}

		seriesimpl = new OverflowSeriesImpl(measurementId, TSDataType.INT32, ofio,
				Compressor.getCompressor(CompressionTypeName.UNCOMPRESSED), null);
		assertEquals(TSDataType.INT32, seriesimpl.getTSDataType());
		// insert data
		for (int i = 21; i < 31; i++) {
			seriesimpl.insert(i, BytesUtils.intToBytes(i));
		}

		seriesimpl.switchWorkingToFlushing();
		try {
			seriesimpl.flushToFileWriter(ofio);
		} catch (IOException e) {
			e.printStackTrace();
			fail("Flush overflowfileio failed");
		}

		for (int i = 31; i < 41; i++) {
			seriesimpl.insert(i, BytesUtils.intToBytes(i));
		}
		// no merge query
		List<Object> queryResult = seriesimpl.query(null, null, null);
		DynamicOneColumnData insertData = (DynamicOneColumnData) queryResult.get(0);
		assertEquals(20, insertData.length);
		for (int i = 0; i < 20; i++) {
			assertEquals(i + 21, insertData.getTime(i));
			assertEquals(i + 21, insertData.getInt(i));
		}
		// merge and query
		seriesimpl.switchWorkingToMerging();
		seriesimpl.setMergingSeriesImpl(mergeseriesimpl);

		queryResult = seriesimpl.query(null, null, null);
		try {
			seriesimpl.switchMergeToWorking();
		} catch (IOException e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
		
		insertData = (DynamicOneColumnData) queryResult.get(0);
		assertEquals(40, insertData.length);
		for (int i = 0; i < 40; i++) {
			assertEquals(i + 1, insertData.getTime(i));
			assertEquals(i + 1, insertData.getInt(i));
		}

		try {
			seriesimpl.close();
		} catch (IOException e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}

	@Test
	public void testInsertUpdateDeleteFlushAndQuery() {
		try {
			ofrw = new OverflowReadWriter(filePath);
		} catch (IOException e) {
			e.printStackTrace();
			fail("Construct the overflow read write failed");
		}

		try {
			ofio = new OverflowFileIO(ofrw, filePath, 0);
		} catch (IOException e) {
			e.printStackTrace();
			fail("Construct the overflowfile io failed");
		}

		seriesimpl = new OverflowSeriesImpl(measurementId, TSDataType.INT32, ofio,
				Compressor.getCompressor(CompressionTypeName.UNCOMPRESSED), null);
		assertEquals(TSDataType.INT32, seriesimpl.getTSDataType());

		// test insert
		for (int i = 1; i < 11; i++) {
			seriesimpl.insert(i, BytesUtils.intToBytes(i));
		}
		assertEquals(true, seriesimpl.isEmptyForWrite());
		// flush data
		seriesimpl.switchWorkingToFlushing();
		try {
			seriesimpl.flushToFileWriter(ofio);
		} catch (IOException e) {
			e.printStackTrace();
			fail("Flush overflowfileio failed");
		}
		assertEquals(false, seriesimpl.isEmptyForWrite());
		assertEquals(1, seriesimpl.getOFSeriesListMetadata().getMetaDatas().size());
		// query insert data
		List<Object> result = seriesimpl.query(null, null, null);
		DynamicOneColumnData insertresult = (DynamicOneColumnData) result.get(0);
		assertEquals(false, insertresult == null);
		assertEquals(10, insertresult.length);
		for (int i = 1; i < 11; i++) {
			assertEquals(i, insertresult.getTime(i - 1));
			assertEquals(i, insertresult.getInt(i - 1));
		}
		// test update
		for (int i = 11; i < 30; i = i + 2) {
			seriesimpl.update(i, i + 1, BytesUtils.intToBytes(i));
		}
		seriesimpl.switchWorkingToFlushing();
		try {
			seriesimpl.flushToFileWriter(ofio);
		} catch (IOException e) {
			e.printStackTrace();
			fail("Flush overflowfileio failed");
		}
		assertEquals(2, seriesimpl.getOFSeriesListMetadata().getMetaDatas().size());
		// query update data
		result = seriesimpl.query(null, null, null);
		DynamicOneColumnData updateresult = (DynamicOneColumnData) result.get(1);
		assertEquals(10, updateresult.length);
		assertEquals(20, updateresult.timeLength);
		for (int i = 0; i < 10; i++) {
			assertEquals(10 + 2 * i + 1, updateresult.getTime(i * 2));
			assertEquals(10 + 2 * i + 2, updateresult.getTime(i * 2 + 1));
			assertEquals(10 + 2 * i + 1, updateresult.getInt(i));
		}
		seriesimpl.insert(40, BytesUtils.intToBytes(40));
		seriesimpl.insert(41, BytesUtils.intToBytes(41));
		seriesimpl.switchWorkingToFlushing();
		try {
			seriesimpl.flushToFileWriter(ofio);
		} catch (IOException e) {
			e.printStackTrace();
			fail("Flush overflowfileio failed");
		}
		assertEquals(3, seriesimpl.getOFSeriesListMetadata().getMetaDatas().size());
		// test delete
		seriesimpl.delete(50);
		seriesimpl.switchWorkingToFlushing();
		try {
			seriesimpl.flushToFileWriter(ofio);
		} catch (IOException e) {
			e.printStackTrace();
			fail("Flush overflowfileio failed");
		}
		assertEquals(4, seriesimpl.getOFSeriesListMetadata().getMetaDatas().size());

		result = seriesimpl.query(null, null, null);
		insertresult = (DynamicOneColumnData) result.get(0);
		updateresult = (DynamicOneColumnData) result.get(1);
		assertEquals(0, updateresult.length);
		assertEquals(0, insertresult.length);

		// test flush empty data
		seriesimpl.switchWorkingToFlushing();
		try {
			seriesimpl.flushToFileWriter(ofio);
		} catch (IOException e) {
			e.printStackTrace();
			fail("Flush overflowfileio failed");
		}
		assertEquals(4, seriesimpl.getOFSeriesListMetadata().getMetaDatas().size());
		
		try {
			seriesimpl.close();
		} catch (IOException e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}

	private OverflowSeriesImpl createSeriesImpl(TSDataType dataType) {
		try {
			ofrw = new OverflowReadWriter(filePath);
		} catch (IOException e) {
			e.printStackTrace();
			fail("Construct the overflow read write failed");
		}

		try {
			ofio = new OverflowFileIO(ofrw, filePath, 0);
		} catch (IOException e) {
			e.printStackTrace();
			fail("Construct the overflowfile io failed");
		}

		return seriesimpl = new OverflowSeriesImpl(measurementId, dataType, ofio,
				Compressor.getCompressor(CompressionTypeName.UNCOMPRESSED), null);
	}

	@Test
	public void testLong() {

		seriesimpl = createSeriesImpl(TSDataType.INT64);
		// flush data
		for (long i = 1; i < 11; i++) {
			seriesimpl.insert(i, BytesUtils.longToBytes(i));
		}
		assertEquals(true, seriesimpl.isEmptyForWrite());
		// flush data
		seriesimpl.switchWorkingToFlushing();
		try {
			seriesimpl.flushToFileWriter(ofio);
		} catch (IOException e) {
			e.printStackTrace();
			fail("Flush overflowfileio failed");
		}
		assertEquals(false, seriesimpl.isEmptyForWrite());
		assertEquals(1, seriesimpl.getOFSeriesListMetadata().getMetaDatas().size());
		// query
		List<Object> result = seriesimpl.query(null, null, null);
		DynamicOneColumnData insertresult = (DynamicOneColumnData) result.get(0);
		assertEquals(false, insertresult == null);
		assertEquals(10, insertresult.length);
		for (int i = 1; i < 11; i++) {
			assertEquals(i, insertresult.getTime(i - 1));
			assertEquals(i, insertresult.getLong(i - 1));
		}
		try {
			seriesimpl.close();
		} catch (IOException e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}

	@Test
	public void testFloat() {

		seriesimpl = createSeriesImpl(TSDataType.FLOAT);
		for (long i = 1; i < 11; i++) {
			seriesimpl.insert(i, BytesUtils.floatToBytes((float) (1.1 + i)));
		}
		assertEquals(true, seriesimpl.isEmptyForWrite());
		// flush data
		seriesimpl.switchWorkingToFlushing();
		try {
			seriesimpl.flushToFileWriter(ofio);
		} catch (IOException e) {
			e.printStackTrace();
			fail("Flush overflowfileio failed");
		}
		assertEquals(false, seriesimpl.isEmptyForWrite());
		assertEquals(1, seriesimpl.getOFSeriesListMetadata().getMetaDatas().size());
		// query
		List<Object> result = seriesimpl.query(null, null, null);
		DynamicOneColumnData insertresult = (DynamicOneColumnData) result.get(0);
		assertEquals(false, insertresult == null);
		assertEquals(10, insertresult.length);
		for (int i = 1; i < 11; i++) {
			assertEquals(i, insertresult.getTime(i - 1));
			assertEquals(String.valueOf(i + 1.1), String.valueOf(insertresult.getFloat(i - 1)));
		}
		try {
			seriesimpl.close();
		} catch (IOException e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}

	@Test
	public void testDouble() {

		seriesimpl = createSeriesImpl(TSDataType.DOUBLE);

		for (long i = 1; i < 11; i++) {
			seriesimpl.insert(i, BytesUtils.doubleToBytes((double) (1.1 + i)));
		}
		assertEquals(true, seriesimpl.isEmptyForWrite());
		// flush data
		seriesimpl.switchWorkingToFlushing();
		try {
			seriesimpl.flushToFileWriter(ofio);
		} catch (IOException e) {
			e.printStackTrace();
			fail("Flush overflowfileio failed");
		}
		assertEquals(false, seriesimpl.isEmptyForWrite());
		assertEquals(1, seriesimpl.getOFSeriesListMetadata().getMetaDatas().size());
		// query
		List<Object> result = seriesimpl.query(null, null, null);
		DynamicOneColumnData insertresult = (DynamicOneColumnData) result.get(0);
		assertEquals(false, insertresult == null);
		assertEquals(10, insertresult.length);
		for (int i = 1; i < 11; i++) {
			assertEquals(i, insertresult.getTime(i - 1));
			assertEquals(String.valueOf(i + 1.1), String.valueOf(insertresult.getDouble(i - 1)));
		}
		try {
			seriesimpl.close();
		} catch (IOException e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}

	@Test
	public void testBoolean() {

		seriesimpl = createSeriesImpl(TSDataType.BOOLEAN);
		for (long i = 1; i < 11; i++) {
			seriesimpl.insert(i, BytesUtils.boolToBytes(i / 2 == 0 ? true : false));
		}
		assertEquals(true, seriesimpl.isEmptyForWrite());
		// flush data
		seriesimpl.switchWorkingToFlushing();
		try {
			seriesimpl.flushToFileWriter(ofio);
		} catch (IOException e) {
			e.printStackTrace();
			fail("Flush overflowfileio failed");
		}
		assertEquals(false, seriesimpl.isEmptyForWrite());
		assertEquals(1, seriesimpl.getOFSeriesListMetadata().getMetaDatas().size());
		// query
		List<Object> result = seriesimpl.query(null, null, null);
		DynamicOneColumnData insertresult = (DynamicOneColumnData) result.get(0);
		assertEquals(false, insertresult == null);
		assertEquals(10, insertresult.length);
		for (int i = 1; i < 11; i++) {
			assertEquals(i, insertresult.getTime(i - 1));
			assertEquals(i / 2 == 0 ? true : false, insertresult.getBoolean(i - 1));
		}
		try {
			seriesimpl.close();
		} catch (IOException e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}

	@Test
	public void testByteArray() {
		seriesimpl = createSeriesImpl(TSDataType.TEXT);

		for (long i = 1; i < 11; i++) {
			seriesimpl.insert(i, BytesUtils.StringToBytes(String.valueOf(i)));
		}
		assertEquals(true, seriesimpl.isEmptyForWrite());
		// flush data
		seriesimpl.switchWorkingToFlushing();
		try {
			seriesimpl.flushToFileWriter(ofio);
		} catch (IOException e) {
			e.printStackTrace();
			fail("Flush overflowfileio failed");
		}
		assertEquals(false, seriesimpl.isEmptyForWrite());
		assertEquals(1, seriesimpl.getOFSeriesListMetadata().getMetaDatas().size());
		// query
		List<Object> result = seriesimpl.query(null, null, null);
		DynamicOneColumnData insertresult = (DynamicOneColumnData) result.get(0);
		assertEquals(false, insertresult == null);
		assertEquals(10, insertresult.length);
		for (int i = 1; i < 11; i++) {
			assertEquals(i, insertresult.getTime(i - 1));
			assertEquals(String.valueOf(i), insertresult.getStringValue(i - 1));
		}
		try {
			seriesimpl.close();
		} catch (IOException e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}

	@Test
	public void testEnum() {

		seriesimpl = createSeriesImpl(TSDataType.ENUMS);
		
		try {
			seriesimpl.close();
		} catch (IOException e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
		//fail("Not support Type");
	}
}
