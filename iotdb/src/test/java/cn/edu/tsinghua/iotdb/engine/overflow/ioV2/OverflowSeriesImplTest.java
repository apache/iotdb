package cn.edu.tsinghua.iotdb.engine.overflow.ioV2;

import static org.junit.Assert.*;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import cn.edu.tsinghua.tsfile.common.utils.BytesUtils;
import cn.edu.tsinghua.tsfile.file.metadata.enums.TSDataType;
import cn.edu.tsinghua.tsfile.timeseries.read.query.DynamicOneColumnData;

public class OverflowSeriesImplTest {

	private static final float error = 0.000001f;

	@Before
	public void setUp() throws Exception {
	}

	@After
	public void tearDown() throws Exception {
	}

	@Test
	public void test() {
		testInt();
		testFloat();
		testLong();
		testDouble();
	}

	public void testInt() {
		OverflowSeriesImpl index = new OverflowSeriesImpl("s1", TSDataType.INT32);
		index.update(2, 8, BytesUtils.intToBytes(5));
		index.update(4, 20, BytesUtils.intToBytes(20));
		index.update(30, 40, BytesUtils.intToBytes(30));
		index.update(50, 60, BytesUtils.intToBytes(40));
		DynamicOneColumnData one = index.query(null);
		assertEquals(4, index.getValueCount());
		// assert time
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

	public void testLong() {
		OverflowSeriesImpl index = new OverflowSeriesImpl("s1", TSDataType.INT64);
		// delete
		index.delete(10);
		index.delete(20);
		index.update(2, 8, BytesUtils.longToBytes(5));
		index.update(4, 20, BytesUtils.longToBytes(20));
		index.update(30, 40, BytesUtils.longToBytes(30));
		index.update(50, 60, BytesUtils.longToBytes(40));
		DynamicOneColumnData one = index.query(null);
		// assert time
		assertEquals(0, one.getTime(0));
		assertEquals(-20, one.getTime(1));
		assertEquals(30, one.getTime(2));
		assertEquals(40, one.getTime(3));
		assertEquals(50, one.getTime(4));
		assertEquals(60, one.getTime(5));
		// assert value
		assertEquals(0, one.getLong(0));
		assertEquals(30, one.getLong(1));
		assertEquals(40, one.getLong(2));
	}

	public void testFloat() {
		OverflowSeriesImpl index = new OverflowSeriesImpl("s1", TSDataType.FLOAT);
		index.update(2, 8, BytesUtils.floatToBytes(5.5f));
		index.update(4, 20, BytesUtils.floatToBytes(20.5f));
		index.update(30, 40, BytesUtils.floatToBytes(30.5f));
		index.update(50, 60, BytesUtils.floatToBytes(40.7f));
		DynamicOneColumnData one = index.query(null);
		assertEquals(4, index.getValueCount());
		// assert time
		assertEquals(2, one.getTime(0));
		assertEquals(3, one.getTime(1));
		assertEquals(4, one.getTime(2));
		assertEquals(20, one.getTime(3));
		assertEquals(30, one.getTime(4));
		assertEquals(40, one.getTime(5));
		assertEquals(50, one.getTime(6));
		assertEquals(60, one.getTime(7));
		// assert value
		assertEquals(5.5f, one.getFloat(0), error);
		assertEquals(20.5f, one.getFloat(1), error);
		assertEquals(30.5f, one.getFloat(2), error);
		assertEquals(40.7f, one.getFloat(3), error);
	}

	public void testDouble() {
		OverflowSeriesImpl index = new OverflowSeriesImpl("s1", TSDataType.DOUBLE);
		index.update(2, 8, BytesUtils.doubleToBytes(5.5d));
		index.update(4, 20, BytesUtils.doubleToBytes(20.5d));
		index.update(30, 40, BytesUtils.doubleToBytes(30.5d));
		index.update(50, 60, BytesUtils.doubleToBytes(40.7d));
		DynamicOneColumnData one = index.query(null);
		assertEquals(4, index.getValueCount());
		// assert time
		assertEquals(2, one.getTime(0));
		assertEquals(3, one.getTime(1));
		assertEquals(4, one.getTime(2));
		assertEquals(20, one.getTime(3));
		assertEquals(30, one.getTime(4));
		assertEquals(40, one.getTime(5));
		assertEquals(50, one.getTime(6));
		assertEquals(60, one.getTime(7));
		// assert value
		assertEquals(5.5d, one.getDouble(0), error);
		assertEquals(20.5d, one.getDouble(1), error);
		assertEquals(30.5d, one.getDouble(2), error);
		assertEquals(40.7d, one.getDouble(3), error);
	}

	public void testText() {

	}

	public void testBool() {

	}

}
