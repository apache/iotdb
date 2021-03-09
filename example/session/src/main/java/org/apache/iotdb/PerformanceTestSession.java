package org.apache.iotdb;

import org.apache.iotdb.session.Session;
import org.apache.iotdb.tsfile.file.metadata.enums.CompressionType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.apache.iotdb.tsfile.write.record.Tablet;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;

import java.util.*;

public class PerformanceTestSession {
	static Session session = new Session("192.168.130.38", 6667, "root", "root");
	static final int DEVICE_NUM = 10;
	static final int TIMESERIES_NUM = 10000;
	static final int DATA_NUM = 1000;
	public static void main(String[] args) throws Exception{
		session.open(false);
		try{
			session.deleteStorageGroup("root.test");
		} catch (Exception e) {
			e.printStackTrace();
			
		}
		session.setStorageGroup("root.test");
		createTimeseries();
		testInsertion();
		session.close();
	}

	static void createTimeseries() throws Exception {
		List<String> paths = new ArrayList<>();
		List<TSDataType> tsDataTypes = new ArrayList<>();
		List<TSEncoding> tsEncodings = new ArrayList<>();
		List<CompressionType> compressionTypes = new ArrayList<>();
		for (int i = 0; i < DEVICE_NUM; ++i) {
			for (int j = 0; j < TIMESERIES_NUM; ++j) {
				paths.add(String.format("root.test.device%d.s%d", i, j));
				tsDataTypes.add(TSDataType.DOUBLE);
				tsEncodings.add(TSEncoding.RLE);
				compressionTypes.add(CompressionType.SNAPPY);
			}
			session.createMultiTimeseries(paths, tsDataTypes, tsEncodings, compressionTypes, null, null, null, null);
			paths.clear();
			tsDataTypes.clear();
			tsEncodings.clear();
			compressionTypes.clear();
		}

	}

	static void testInsertion() throws Exception {
		Random r = new Random();
		List<String> measurements = new ArrayList<>();
		for (int i = 0; i < TIMESERIES_NUM; ++i) {
			measurements.add("s" + i);
		}
		List<String> deviceIds = new ArrayList<>();
		List<List<String>> measurementsList = new ArrayList<>();
		List<List<Object>> valuesList = new ArrayList<>();
		List<Long> timestamps = new ArrayList<>();
		List<List<TSDataType>> typesList = new ArrayList<>();

		long startTime = System.currentTimeMillis();
		for (long time = 0; time < DATA_NUM; time++) {
			System.out.println((float)time / (float)DATA_NUM);
			for(int deviceIdx = 0; deviceIdx < DEVICE_NUM; ++deviceIdx) {
				List<Object> values = new ArrayList<>();
				List<TSDataType> types = new ArrayList<>();
				for (int i = 0; i < TIMESERIES_NUM; ++i) {
					values.add(r.nextDouble());
					types.add(TSDataType.DOUBLE);
				}
				deviceIds.add("root.test.device" + deviceIdx);
				measurementsList.add(measurements);
				valuesList.add(values);
				typesList.add(types);
				timestamps.add(time);
			}
			if (time != 0 && time % 5 == 0) {
				session.insertRecords(deviceIds, timestamps, measurementsList, typesList, valuesList);
				deviceIds.clear();
				measurementsList.clear();
				valuesList.clear();
				timestamps.clear();
				typesList.clear();
			}
		}
		System.out.println(System.currentTimeMillis() - startTime);
	}
}
