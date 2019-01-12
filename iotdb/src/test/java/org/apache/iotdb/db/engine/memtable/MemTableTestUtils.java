package org.apache.iotdb.db.engine.memtable;

import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.apache.iotdb.tsfile.write.schema.FileSchema;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;

public class MemTableTestUtils {

	public static String deviceId0 = "d0";

	public static String measurementId0 = "s0";

	public static TSDataType dataType0 = TSDataType.INT32;

	public static void produceData(IMemTable iMemTable, long startTime, long endTime, String deviceId,
			String measurementId, TSDataType dataType) {
		if (startTime > endTime) {
			throw new RuntimeException(String.format("start time %d > end time %d", startTime, endTime));
		}
		for (long l = startTime; l <= endTime; l++) {
			iMemTable.write(deviceId, measurementId, dataType, l, String.valueOf(l));
		}
	}

	private static FileSchema fileSchema = new FileSchema();
	static {
		fileSchema.registerMeasurement(new MeasurementSchema(measurementId0, dataType0, TSEncoding.PLAIN));
	}

	public static FileSchema getFileSchema() {
		return fileSchema;
	}

}
