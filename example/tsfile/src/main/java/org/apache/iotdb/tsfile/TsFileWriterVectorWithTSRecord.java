package org.apache.iotdb.tsfile;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.iotdb.tsfile.exception.write.WriteProcessException;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.apache.iotdb.tsfile.fileSystem.FSFactoryProducer;
import org.apache.iotdb.tsfile.read.common.Path;
import org.apache.iotdb.tsfile.write.TsFileWriter;
import org.apache.iotdb.tsfile.write.record.TSRecord;
import org.apache.iotdb.tsfile.write.record.datapoint.DataPoint;
import org.apache.iotdb.tsfile.write.record.datapoint.LongDataPoint;
import org.apache.iotdb.tsfile.write.schema.IMeasurementSchema;
import org.apache.iotdb.tsfile.write.schema.UnaryMeasurementSchema;

public class TsFileWriterVectorWithTSRecord {

  public static void main(String[] args) throws IOException {
    File f = FSFactoryProducer.getFSFactory().getFile("test.tsfile");
    if (f.exists() && !f.delete()) {
      throw new RuntimeException("can not delete " + f.getAbsolutePath());
    }
    try (TsFileWriter tsFileWriter = new TsFileWriter(f)) {
      // register timeseries
      List<IMeasurementSchema> measurementSchemas = new ArrayList<>();
      measurementSchemas.add(new UnaryMeasurementSchema("s1", TSDataType.INT64, TSEncoding.RLE));
      measurementSchemas.add(new UnaryMeasurementSchema("s2", TSDataType.INT64, TSEncoding.RLE));
      measurementSchemas.add(new UnaryMeasurementSchema("s3", TSDataType.INT64, TSEncoding.RLE));
      tsFileWriter.registerAlignedTimeseries(new Path("root.sg.d1"), measurementSchemas);
      // construct TsRecord
      TSRecord tsRecord = new TSRecord(100, "root.sg.d1");
      DataPoint dPoint1 = new LongDataPoint("s1", 1000);
      DataPoint dPoint2 = new LongDataPoint("s2", 2000);
      tsRecord.addTuple(dPoint1);
      tsRecord.addTuple(dPoint2);
      // write
      tsFileWriter.writeAligned(tsRecord);

      // construct TsRecord
      tsRecord = new TSRecord(200, "root.sg.d1");
      dPoint1 = new LongDataPoint("s1", 1500);
      dPoint2 = new LongDataPoint("s2", 2500);
      tsRecord.addTuple(dPoint1);
      tsRecord.addTuple(dPoint2);
      // write
      tsFileWriter.writeAligned(tsRecord);

      // construct TsRecord
      tsRecord = new TSRecord(300, "root.sg.d1");
      dPoint1 = new LongDataPoint("s3", 3000);
      tsRecord.addTuple(dPoint1);
      // write
      tsFileWriter.writeAligned(tsRecord);
    } catch (WriteProcessException e) {
      e.printStackTrace();
    }
  }
}
