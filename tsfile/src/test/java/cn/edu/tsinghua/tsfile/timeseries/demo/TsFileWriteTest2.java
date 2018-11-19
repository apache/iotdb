package cn.edu.tsinghua.tsfile.timeseries.demo;

import java.io.File;

import cn.edu.tsinghua.tsfile.file.metadata.enums.TSDataType;
import cn.edu.tsinghua.tsfile.file.metadata.enums.TSEncoding;
import cn.edu.tsinghua.tsfile.timeseries.write.TsFileWriter;
import cn.edu.tsinghua.tsfile.timeseries.write.desc.MeasurementDescriptor;
import cn.edu.tsinghua.tsfile.timeseries.write.record.DataPoint;
import cn.edu.tsinghua.tsfile.timeseries.write.record.TSRecord;
import cn.edu.tsinghua.tsfile.timeseries.write.record.datapoint.FloatDataPoint;

public class TsFileWriteTest2 {

	public static void main(String args[]) {
		try {
			TsFileWriter tsFileWriter = new TsFileWriter(new File("test1.ts"));

			// add measurements
			tsFileWriter.addMeasurement(new MeasurementDescriptor("cpu_utility", TSDataType.FLOAT, TSEncoding.TS_2DIFF));
			tsFileWriter.addMeasurement(new MeasurementDescriptor("memory_utility", TSDataType.FLOAT, TSEncoding.TS_2DIFF));

			// construct TSRecord
			TSRecord tsRecord = new TSRecord(1000, "hxd");
			DataPoint dPoint1 = new FloatDataPoint("cpu_utility", 90.0f);
			DataPoint dPoint2 = new FloatDataPoint("memory_utility", 80.0f);
			tsRecord.addTuple(dPoint1);
			tsRecord.addTuple(dPoint2);

			// write TSRecord to TsFile
			tsFileWriter.write(tsRecord);

			// close TsFile
			tsFileWriter.close();
		} catch (Throwable e) {
			e.printStackTrace();
			System.out.println(e.getMessage());
		}
	}

}
