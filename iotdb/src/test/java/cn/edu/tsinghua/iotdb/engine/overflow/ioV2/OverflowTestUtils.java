package cn.edu.tsinghua.iotdb.engine.overflow.ioV2;

import static org.junit.Assert.assertEquals;

import java.io.IOException;

import cn.edu.tsinghua.tsfile.common.utils.BytesUtils;
import cn.edu.tsinghua.tsfile.file.metadata.enums.TSDataType;
import cn.edu.tsinghua.tsfile.file.metadata.enums.TSEncoding;
import cn.edu.tsinghua.tsfile.timeseries.write.desc.MeasurementDescriptor;
import cn.edu.tsinghua.tsfile.timeseries.write.record.DataPoint;
import cn.edu.tsinghua.tsfile.timeseries.write.record.TSRecord;
import cn.edu.tsinghua.tsfile.timeseries.write.schema.FileSchema;

public class OverflowTestUtils {
	public static String deltaObjectId1 = "d1";
	public static String deltaObjectId2 = "d2";
	public static String measurementId1 = "s1";
	public static String measurementId2 = "s2";
	public static TSDataType dataType1 = TSDataType.INT32;
	public static TSDataType dataType2 = TSDataType.FLOAT;
	private static FileSchema fileSchema = new FileSchema();
	static {
		fileSchema.registerMeasurement(new MeasurementDescriptor(measurementId1, dataType1, TSEncoding.PLAIN));
		fileSchema.registerMeasurement(new MeasurementDescriptor(measurementId2, dataType2, TSEncoding.PLAIN));
	}

	public static FileSchema getFileSchema() {
		return fileSchema;
	}

	public static void produceUpdateData(OverflowSupport support) {
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
	}

	public static void produceInsertData(OverflowSupport support) {
		support.insert(getData(deltaObjectId1, measurementId1, dataType1, String.valueOf(1), 1));
		support.insert(getData(deltaObjectId1, measurementId1, dataType1, String.valueOf(3), 3));
		support.insert(getData(deltaObjectId1, measurementId1, dataType1, String.valueOf(2), 2));

		support.insert(getData(deltaObjectId2, measurementId2, dataType2, String.valueOf(5.5f), 1));
		support.insert(getData(deltaObjectId2, measurementId2, dataType2, String.valueOf(5.5f), 2));
		support.insert(getData(deltaObjectId2, measurementId2, dataType2, String.valueOf(10.5f), 2));
	}

	private static TSRecord getData(String d, String m, TSDataType type, String value, long time) {
		TSRecord record = new TSRecord(time, d);
		record.addTuple(DataPoint.getDataPoint(type, m, value));
		return record;
	}

	public static void produceUpdateData(OverflowProcessor processor) {
		// d1 s1
		processor.update(deltaObjectId1, measurementId1, 2, 10, dataType1, BytesUtils.intToBytes(10));
		processor.update(deltaObjectId1, measurementId1, 20, 30, dataType1, BytesUtils.intToBytes(20));
		// time :[2,10] [20,30] value: int [10,10] int[20,20]
		// d1 s2
		processor.delete(deltaObjectId1, measurementId2, 10, dataType1);
		processor.update(deltaObjectId1, measurementId2, 20, 30, dataType1, BytesUtils.intToBytes(20));
		// time: [0,-10] [20,30] value[20,20]
		// d2 s1
		processor.update(deltaObjectId2, measurementId1, 10, 20, dataType2, BytesUtils.floatToBytes(10.5f));
		processor.update(deltaObjectId2, measurementId1, 15, 40, dataType2, BytesUtils.floatToBytes(20.5f));
		// time: [5,9] [10,40] value [10.5,10.5] [20.5,20.5]
		// d2 s2
		processor.update(deltaObjectId2, measurementId2, 2, 10, dataType2, BytesUtils.floatToBytes(5.5f));
		processor.delete(deltaObjectId2, measurementId2, 20, dataType2);
	}

	public static void produceInsertData(OverflowProcessor processor) throws IOException {

		processor.insert(getData(deltaObjectId1, measurementId1, dataType1, String.valueOf(1), 1));
		processor.insert(getData(deltaObjectId1, measurementId1, dataType1, String.valueOf(3), 3));
		processor.insert(getData(deltaObjectId1, measurementId1, dataType1, String.valueOf(2), 2));

		// processor.insert(getData(deltaObjectId1, measurementId2, dataType1,
		// String.valueOf(1), 1));
		// processor.insert(getData(deltaObjectId1, measurementId2, dataType1,
		// String.valueOf(3), 3));
		// processor.insert(getData(deltaObjectId1, measurementId2, dataType1,
		// String.valueOf(2), 2));

		// processor.insert(getData(deltaObjectId2, measurementId1, dataType2,
		// String.valueOf(5.5f), 1));
		// processor.insert(getData(deltaObjectId2, measurementId1, dataType2,
		// String.valueOf(5.5f), 2));
		// processor.insert(getData(deltaObjectId2, measurementId1, dataType2,
		// String.valueOf(10.5f), 2));

		processor.insert(getData(deltaObjectId2, measurementId2, dataType2, String.valueOf(5.5f), 1));
		processor.insert(getData(deltaObjectId2, measurementId2, dataType2, String.valueOf(5.5f), 2));
		processor.insert(getData(deltaObjectId2, measurementId2, dataType2, String.valueOf(10.5f), 2));
	}

}
