package cn.edu.thu.tsfiledb.engine.overflow.io;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.util.List;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import cn.edu.thu.tsfile.common.utils.BytesUtils;
import cn.edu.thu.tsfile.common.utils.RandomAccessOutputStream;
import cn.edu.thu.tsfile.compress.Compressor;
import cn.edu.thu.tsfile.file.metadata.enums.CompressionTypeName;
import cn.edu.thu.tsfile.file.metadata.enums.TSDataType;
import cn.edu.thu.tsfile.timeseries.read.query.DynamicOneColumnData;
import cn.edu.thu.tsfiledb.engine.overflow.io.OverflowFileIO;
import cn.edu.thu.tsfiledb.engine.overflow.io.OverflowReadWriter;
import cn.edu.thu.tsfiledb.engine.overflow.io.OverflowSeriesImpl;

public class OverflowSeriesImplTest {

	private String filePath = "overflowseriesimpltest";

	private RandomAccessOutputStream raf = null;
	private OverflowReadWriter ofrw = null;
	private OverflowFileIO ofio = null;
	private OverflowSeriesImpl seriesimpl = null;
	private String measurementId = "s0";

	@Before
	public void setUp() throws Exception {

		EngineTestHelper.delete(filePath);
	}

	@After
	public void tearDown() throws Exception {
		EngineTestHelper.delete(filePath);

	}

	@Test
	public void test() {
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
	}
	
	@Test
	public void testLong(){
		
	}
	
	@Test
	public void testFloat(){
		
	}
	
	
	@Test
	public void testDouble(){
		
	}
	
	@Test
	public void testBoolean(){
		
	}
	
	@Test
	public void testByteArray(){
		
	}
}
