package cn.edu.thu.tsfiledb.engine.bufferwrite;

import static org.junit.Assert.*;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import cn.edu.thu.tsfile.common.utils.Binary;
import cn.edu.thu.tsfile.file.metadata.enums.TSDataType;
import cn.edu.thu.tsfile.timeseries.read.query.DynamicOneColumnData;
import cn.edu.thu.tsfile.timeseries.write.record.DataPoint;
import cn.edu.thu.tsfile.timeseries.write.record.TSRecord;
import cn.edu.thu.tsfiledb.engine.bufferwrite.BufferWriteIndex;
import cn.edu.thu.tsfiledb.engine.bufferwrite.MemoryBufferWriteIndexImpl;

/**
 * <p>
 * This class is used to assert the function of code
 * {@code MemoryBufferWriteIndexImpl} and {@code BufferWriteIndex}<br>
 * 
 * @author liukun
 *
 */
public class BufferWriteIndexTest {

	private BufferWriteIndex bufferwriteindex;

	private String deltaObjectId0 = "d0";

	private String measurementId0 = "s0";// int
	private String measurementId1 = "s1";// long

	private String measurementId2 = "s2";// float
	private String measurementId3 = "s3";// double
	
	private String measurementId4 = "s4";// binary
	private String measurementId5 = "s5";// boolean

	@Before
	public void setUp() throws Exception {

		bufferwriteindex = new MemoryBufferWriteIndexImpl();
	}

	@After
	public void tearDown() throws Exception {

	}

	@Test
	public void testInt() {
		// insert 10 record
		for (int i = 0; i < 10; i++) {
			long timestamp = i * 10;
			TSRecord tsRecord = new TSRecord(timestamp, deltaObjectId0);
			DataPoint tuple = DataPoint.getDataPoint(TSDataType.INT32, measurementId0,
					Integer.valueOf(i * 10).toString());
			DataPoint tuple2 = DataPoint.getDataPoint(TSDataType.INT64, measurementId1,
					Long.valueOf(i * 10).toString());
			DataPoint tuple3 = DataPoint.getDataPoint(TSDataType.FLOAT, measurementId2,
					Float.valueOf(i * 1.5 + "").toString());
			DataPoint tuple4 = DataPoint.getDataPoint(TSDataType.DOUBLE, measurementId3,
					Double.valueOf(i * 2.5 + "").toString());
			DataPoint tuple5 = DataPoint.getDataPoint(TSDataType.BYTE_ARRAY, measurementId4,
					Binary.valueOf(i + "").toString());
			DataPoint tuple6 = DataPoint.getDataPoint(TSDataType.BOOLEAN, measurementId5,
					Boolean.valueOf(i / 2 == 0 ? true : false).toString());
			tsRecord.addTuple(tuple);
			tsRecord.addTuple(tuple2);
			tsRecord.addTuple(tuple3);
			tsRecord.addTuple(tuple4);
			tsRecord.addTuple(tuple5);
			tsRecord.addTuple(tuple6);
			bufferwriteindex.insert(tsRecord);
		}
		DynamicOneColumnData oneColumnData = null;
		// test int
		oneColumnData = bufferwriteindex.query(deltaObjectId0, measurementId0);
		int allLength = oneColumnData.length;
		int timeLength = oneColumnData.timeLength;
		assertEquals(allLength, timeLength);

		for (int i = 0; i < allLength; i++) {
			long timestamp = oneColumnData.getTime(i);
			int value = oneColumnData.getInt(i);
			assertEquals(i * 10, timestamp);
			assertEquals(i * 10, value);
		}
		// test long
		oneColumnData = bufferwriteindex.query(deltaObjectId0, measurementId1);

		for (int i = 0; i < allLength; i++) {
			long timestamp = oneColumnData.getTime(i);
			long value = oneColumnData.getLong(i);
			assertEquals(i * 10, timestamp);
			assertEquals(i * 10, value);
		}

		// test float
		oneColumnData = bufferwriteindex.query(deltaObjectId0, measurementId2);

		for (int i = 0; i < allLength; i++) {
			long timestamp = oneColumnData.getTime(i);
			float value = oneColumnData.getFloat(i);
			assertEquals(i * 10, timestamp);
			assertEquals(i * 1.5+"", value+"");
		}

		// test double
		oneColumnData = bufferwriteindex.query(deltaObjectId0, measurementId3);

		for (int i = 0; i < allLength; i++) {
			long timestamp = oneColumnData.getTime(i);
			double value = oneColumnData.getDouble(i);
			assertEquals(i * 10, timestamp);
			assertEquals(i * 2.5+"", value+"");
		}
		// test binary
		oneColumnData = bufferwriteindex.query(deltaObjectId0, measurementId4);

		for (int i = 0; i < allLength; i++) {
			long timestamp = oneColumnData.getTime(i);
			String value = oneColumnData.getStringValue(i);
			assertEquals(i * 10, timestamp);
			assertEquals(i + "", value);
		}
		// test boolean
		oneColumnData = bufferwriteindex.query(deltaObjectId0, measurementId5);

		for (int i = 0; i < allLength; i++) {
			long timestamp = oneColumnData.getTime(i);
			boolean value = oneColumnData.getBoolean(i);
			assertEquals(i * 10, timestamp);
			assertEquals(i/2==0?true:false, value);
		}
		// test clear
		bufferwriteindex.clear();
		oneColumnData = bufferwriteindex.query(deltaObjectId0, measurementId3);
		assertEquals(null, oneColumnData);
		oneColumnData = bufferwriteindex.query(deltaObjectId0, measurementId0);
		assertEquals(null, oneColumnData);
	}
}
