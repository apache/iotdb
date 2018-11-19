package cn.edu.tsinghua.tsfile.timeseries.demo;

/**
 * Created by beyyes on 17/12/5.
 */
import java.io.File;

import cn.edu.tsinghua.tsfile.file.metadata.enums.TSDataType;
import cn.edu.tsinghua.tsfile.file.metadata.enums.TSEncoding;
import cn.edu.tsinghua.tsfile.timeseries.write.TsFileWriter;
import cn.edu.tsinghua.tsfile.timeseries.write.desc.MeasurementDescriptor;
import cn.edu.tsinghua.tsfile.timeseries.write.record.DataPoint;
import cn.edu.tsinghua.tsfile.timeseries.write.record.TSRecord;
import cn.edu.tsinghua.tsfile.timeseries.write.record.datapoint.*;

public class TsFileWrite2 {

    public static void main(String args[]) {
        try {
            TsFileWriter tsFileWriter = new TsFileWriter(new File("test.ts"));

            // add measurements
            tsFileWriter.addMeasurement(new MeasurementDescriptor("sensor_1", TSDataType.FLOAT, TSEncoding.RLE));
            tsFileWriter.addMeasurement(new MeasurementDescriptor("sensor_2", TSDataType.INT32, TSEncoding.TS_2DIFF));
            tsFileWriter.addMeasurement(new MeasurementDescriptor("sensor_3", TSDataType.INT32, TSEncoding.TS_2DIFF));

            // construct TSRecord
            TSRecord tsRecord = new TSRecord(1, "device_1");
            DataPoint dPoint1 = new FloatDataPoint("sensor_1", 1.2f);
            DataPoint dPoint2 = new IntDataPoint("sensor_2", 20);
            DataPoint dPoint3;
            tsRecord.addTuple(dPoint1);
            tsRecord.addTuple(dPoint2);
            tsFileWriter.write(tsRecord);


            tsRecord = new TSRecord(2, "device_1");
            dPoint2 = new IntDataPoint("sensor_2", 20);
            dPoint3 = new IntDataPoint("sensor_3", 50);
            tsRecord.addTuple(dPoint2);
            tsRecord.addTuple(dPoint3);
            tsFileWriter.write(tsRecord);

            tsRecord = new TSRecord(3, "device_1");
            dPoint1 = new FloatDataPoint("sensor_1", 1.4f);
            dPoint2 = new IntDataPoint("sensor_2", 21);
            tsRecord.addTuple(dPoint1);
            tsRecord.addTuple(dPoint2);
            tsFileWriter.write(tsRecord);

            tsRecord = new TSRecord(4, "device_1");
            dPoint1 = new FloatDataPoint("sensor_1", 1.2f);
            dPoint2 = new IntDataPoint("sensor_2", 20);
            dPoint3 = new IntDataPoint("sensor_3", 51);
            tsRecord.addTuple(dPoint1);
            tsRecord.addTuple(dPoint2);
            tsRecord.addTuple(dPoint3);
            tsFileWriter.write(tsRecord);

            tsRecord = new TSRecord(6, "device_1");
            dPoint1 = new FloatDataPoint("sensor_1", 7.2f);
            dPoint2 = new IntDataPoint("sensor_2", 10);
            dPoint3 = new IntDataPoint("sensor_3", 11);
            tsRecord.addTuple(dPoint1);
            tsRecord.addTuple(dPoint2);
            tsRecord.addTuple(dPoint3);
            tsFileWriter.write(tsRecord);

            tsRecord = new TSRecord(7, "device_1");
            dPoint1 = new FloatDataPoint("sensor_1", 6.2f);
            dPoint2 = new IntDataPoint("sensor_2", 20);
            dPoint3 = new IntDataPoint("sensor_3", 21);
            tsRecord.addTuple(dPoint1);
            tsRecord.addTuple(dPoint2);
            tsRecord.addTuple(dPoint3);
            tsFileWriter.write(tsRecord);

            tsRecord = new TSRecord(8, "device_1");
            dPoint1 = new FloatDataPoint("sensor_1", 9.2f);
            dPoint2 = new IntDataPoint("sensor_2", 30);
            dPoint3 = new IntDataPoint("sensor_3", 31);
            tsRecord.addTuple(dPoint1);
            tsRecord.addTuple(dPoint2);
            tsRecord.addTuple(dPoint3);
            tsFileWriter.write(tsRecord);

            // close TsFile
            tsFileWriter.close();
        } catch (Throwable e) {
            e.printStackTrace();
            System.out.println(e.getMessage());
        }
    }

}