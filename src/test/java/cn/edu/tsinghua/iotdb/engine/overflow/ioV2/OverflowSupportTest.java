package cn.edu.tsinghua.iotdb.engine.overflow.ioV2;

import static org.junit.Assert.assertEquals;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import cn.edu.tsinghua.tsfile.common.utils.BytesUtils;
import cn.edu.tsinghua.tsfile.file.metadata.enums.TSDataType;
import cn.edu.tsinghua.tsfile.timeseries.read.query.DynamicOneColumnData;
import cn.edu.tsinghua.tsfile.timeseries.readV2.datatype.TimeValuePair;

public class OverflowSupportTest {

	private OverflowSupport support = new OverflowSupport();
	private String deltaObjectId1 = "d1";
	private String deltaObjectId2 = "d2";
	private String measurementId1 = "s1";
	private String measurementId2 = "s2";
	private TSDataType dataType1 = TSDataType.INT32;
	private TSDataType dataType2 = TSDataType.FLOAT;
	private float error = 0.000001f;

	@Before
	public void setUp() throws Exception {

		assertEquals(true, support.isEmptyOfOverflowSeriesMap());
		assertEquals(true, support.isEmptyOfMemTable());
		// d1 s1
		support.update(deltaObjectId1, measurementId1, 2, 10, dataType1, BytesUtils.intToBytes(10));
		support.update(deltaObjectId1, measurementId1, 20, 30, dataType1, BytesUtils.intToBytes(20));
		// time :[2,10] [20,30] value: int [10,10] int[20,20]
		// d1 s2
		support.delete(deltaObjectId1, measurementId2, 10, dataType1);
		support.update(deltaObjectId1, measurementId2, 20, 30, dataType1, BytesUtils.intToBytes(20));
		// time: [0,-10] [20,30] value[20,20]
		// d2 s1
		support.update(deltaObjectId2, measurementId1, 10, 20, dataType2, BytesUtils.floatToBytes(10.5f));
		support.update(deltaObjectId2, measurementId1, 15, 40, dataType2, BytesUtils.floatToBytes(20.5f));
		// time: [5,9] [10,40] value [10.5,10.5] [20.5,20.5]
		// d2 s2
		support.update(deltaObjectId2, measurementId2, 2, 10, dataType2, BytesUtils.floatToBytes(5.5f));
		support.delete(deltaObjectId2, measurementId2, 20, dataType2);
		// time : [0,-20]

	}

	@After
	public void tearDown() throws Exception {
		support.clear();
	}

	@Test
	public void testOverflowUpdate() {
		// assert d1 s1
		DynamicOneColumnData d1s1 = support.queryOverflowUpdateInMemory(deltaObjectId1, measurementId1, null, null,
				null, dataType1, null);
		assertEquals(2, d1s1.getTime(0));
		assertEquals(10, d1s1.getTime(1));
		assertEquals(20, d1s1.getTime(2));
		assertEquals(30, d1s1.getTime(3));

		assertEquals(10, d1s1.getInt(0));
		assertEquals(20, d1s1.getInt(1));

		// assert d1 s2
		DynamicOneColumnData d1s2 = support.queryOverflowUpdateInMemory(deltaObjectId1, measurementId2, null, null,
				null, dataType1, null);
		assertEquals(0, d1s2.getTime(0));
		assertEquals(-10, d1s2.getTime(1));
		assertEquals(20, d1s2.getTime(2));
		assertEquals(30, d1s2.getTime(3));

		assertEquals(0, d1s2.getInt(0));
		assertEquals(20, d1s2.getInt(1));

		// assert d2 s1
		DynamicOneColumnData d2s1 = support.queryOverflowUpdateInMemory(deltaObjectId2, measurementId1, null, null,
				null, dataType2, null);
		assertEquals(10, d2s1.getTime(0));
		assertEquals(14, d2s1.getTime(1));
		assertEquals(15, d2s1.getTime(2));
		assertEquals(40, d2s1.getTime(3));

		assertEquals(10.5f, d2s1.getFloat(0), error);
		assertEquals(20.5f, d2s1.getFloat(1), error);

		// assert d2 s2
		DynamicOneColumnData d2s2 = support.queryOverflowUpdateInMemory(deltaObjectId2, measurementId2, null, null,
				null, dataType2, null);
		assertEquals(0, d2s2.getTime(0));
		assertEquals(-20, d2s2.getTime(1));

		assertEquals(0, d2s2.getFloat(0), error);
	}

	@Test
	public void testInsert() {
		support.clear();
		assertEquals(true, support.isEmptyOfMemTable());
		OverflowTestUtils.produceInsertData(support);
		assertEquals(false, support.isEmptyOfMemTable());

		int num = 1;
		for (TimeValuePair pair : support.queryOverflowInsertInMemory(deltaObjectId1,
				measurementId1, dataType1).getSortedTimeValuePairList()) {
			assertEquals(num, pair.getTimestamp());
			assertEquals(num, pair.getValue().getInt());
			num++;
		}
		num = 1;
		for (TimeValuePair pair : support.queryOverflowInsertInMemory(deltaObjectId2,
				measurementId2, dataType2).getSortedTimeValuePairList()) {
			assertEquals(num, pair.getTimestamp());
			if (num == 2) {
				assertEquals(10.5, pair.getValue().getFloat(), error);
			} else {
				assertEquals(5.5, pair.getValue().getFloat(), error);
			}
			num++;
		}
	}

}
