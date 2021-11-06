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

public class TsFileWriteAlignedWithTSRecord {

  public static void main(String[] args) throws IOException {
    File f = FSFactoryProducer.getFSFactory().getFile("alignedRecord.tsfile");
    if (f.exists() && !f.delete()) {
      throw new RuntimeException("can not delete " + f.getAbsolutePath());
    }
    try (TsFileWriter tsFileWriter = new TsFileWriter(f)) {
      List<IMeasurementSchema> measurementSchemas = new ArrayList<>();
      measurementSchemas.add(new UnaryMeasurementSchema("s1", TSDataType.INT64, TSEncoding.RLE));
      measurementSchemas.add(new UnaryMeasurementSchema("s2", TSDataType.INT64, TSEncoding.RLE));
      measurementSchemas.add(new UnaryMeasurementSchema("s3", TSDataType.INT64, TSEncoding.RLE));

      // register timeseries
      tsFileWriter.registerAlignedTimeseries(new Path("root.sg.d1"), measurementSchemas);

      List<IMeasurementSchema> writeMeasurementScheams = new ArrayList<>();
      // example1
      writeMeasurementScheams.add(measurementSchemas.get(0));
      writeMeasurementScheams.add(measurementSchemas.get(1));
      writeAligned(tsFileWriter, "root.sg.d1", writeMeasurementScheams, 1000000, 0, 0);

      // example2
      writeMeasurementScheams.clear();
      writeMeasurementScheams.add(measurementSchemas.get(2));
      writeMeasurementScheams.add(measurementSchemas.get(0));
      writeAligned(tsFileWriter, "root.sg.d1", writeMeasurementScheams, 2000, 10000000, 500);

    } catch (WriteProcessException e) {
      e.printStackTrace();
    }
  }

  private static void writeAligned(
      TsFileWriter tsFileWriter,
      String deviceId,
      List<IMeasurementSchema> schemas,
      long rowSize,
      long startTime,
      long startValue)
      throws IOException, WriteProcessException {
    for (long time = startTime; time < rowSize + startTime; time++) {
      // construct TsRecord
      TSRecord tsRecord = new TSRecord(time, deviceId);
      for (IMeasurementSchema schema : schemas) {
        DataPoint dPoint = new LongDataPoint(schema.getMeasurementId(), startValue++);
        tsRecord.addTuple(dPoint);
      }
      // write
      tsFileWriter.writeAligned(tsRecord);
    }
  }
}
