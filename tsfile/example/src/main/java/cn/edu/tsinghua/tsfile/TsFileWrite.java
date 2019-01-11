/**
 * There are two ways to construct a TsFile instance,they generate the same TsFile file.
 * The class use the second interface:
 * public void addMeasurement(MeasurementSchema MeasurementSchema) throws WriteProcessException
 */
package cn.edu.tsinghua.tsfile;

import java.io.File;

import cn.edu.tsinghua.tsfile.file.metadata.enums.TSDataType;
import cn.edu.tsinghua.tsfile.file.metadata.enums.TSEncoding;
import cn.edu.tsinghua.tsfile.write.TsFileWriter;
import cn.edu.tsinghua.tsfile.write.schema.MeasurementSchema;
import cn.edu.tsinghua.tsfile.write.record.datapoint.DataPoint;
import cn.edu.tsinghua.tsfile.write.record.TSRecord;
import cn.edu.tsinghua.tsfile.write.record.datapoint.*;

/**
 * An example of writing data to TsFile
 */
public class TsFileWrite {

    public static void main(String args[]) {
        try {
            String path = "test.tsfile";
            File f = new File(path);
            if (f.exists()) {
                f.delete();
            }
            TsFileWriter tsFileWriter = new TsFileWriter(f);

            // add measurements into file schema
            tsFileWriter.addMeasurement(new MeasurementSchema("sensor_1", TSDataType.FLOAT, TSEncoding.RLE));
            tsFileWriter.addMeasurement(new MeasurementSchema("sensor_2", TSDataType.INT32, TSEncoding.TS_2DIFF));
            tsFileWriter.addMeasurement(new MeasurementSchema("sensor_3", TSDataType.INT32, TSEncoding.TS_2DIFF));

            // construct TSRecord
            TSRecord tsRecord = new TSRecord(1, "device_1");
            DataPoint dPoint1 = new FloatDataPoint("sensor_1", 1.2f);
            DataPoint dPoint2 = new IntDataPoint("sensor_2", 20);
            DataPoint dPoint3;
            tsRecord.addTuple(dPoint1);
            tsRecord.addTuple(dPoint2);

            // write a TSRecord to TsFile
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