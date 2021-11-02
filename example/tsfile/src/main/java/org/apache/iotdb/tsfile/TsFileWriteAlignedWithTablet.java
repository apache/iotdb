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
import org.apache.iotdb.tsfile.utils.Binary;
import org.apache.iotdb.tsfile.write.TsFileWriter;
import org.apache.iotdb.tsfile.write.record.Tablet;
import org.apache.iotdb.tsfile.write.schema.IMeasurementSchema;
import org.apache.iotdb.tsfile.write.schema.UnaryMeasurementSchema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TsFileWriteAlignedWithTablet {
  private static final Logger logger =
      LoggerFactory.getLogger(TsFileWriteVectorWithTabletOld.class);
  private static String deviceId = "root.sg.d1";
  private static long timestamp = 1;

  public static void main(String[] args) throws IOException {
    File f = FSFactoryProducer.getFSFactory().getFile("vectorTablet.tsfile");
    if (f.exists() && !f.delete()) {
      throw new RuntimeException("can not delete " + f.getAbsolutePath());
    }
    try (TsFileWriter tsFileWriter = new TsFileWriter(f)) {
      // register align timeseries
      List<IMeasurementSchema> measurementSchemas = new ArrayList<>();
      measurementSchemas.add(new UnaryMeasurementSchema("s1", TSDataType.INT64, TSEncoding.RLE));
      measurementSchemas.add(new UnaryMeasurementSchema("s2", TSDataType.TEXT, TSEncoding.PLAIN));
      measurementSchemas.add(new UnaryMeasurementSchema("s3", TSDataType.INT64, TSEncoding.RLE));
      measurementSchemas.add(new UnaryMeasurementSchema("s4", TSDataType.INT64, TSEncoding.RLE));
      tsFileWriter.registerAlignedTimeseries(new Path(deviceId), measurementSchemas);

      // construct and write Tablet1
      List<IMeasurementSchema> tmpSchemas = new ArrayList<>();
      tmpSchemas.add(measurementSchemas.get(0));
      tmpSchemas.add(measurementSchemas.get(1));
      writeWithTablet(tsFileWriter, tmpSchemas, 100000);

      // construct and write Tablet2
      tmpSchemas = new ArrayList<>();
      tmpSchemas.add(measurementSchemas.get(2));
      writeWithTablet(tsFileWriter, tmpSchemas, 20);

    } catch (WriteProcessException e) {
      e.printStackTrace();
    }
  }

  private static void writeWithTablet(
      TsFileWriter tsFileWriter, List<IMeasurementSchema> tmpSchemas, int rowNum)
      throws IOException, WriteProcessException {
    Tablet tablet = new Tablet(deviceId, tmpSchemas);
    long[] timestamps = tablet.timestamps;
    Object[] values = tablet.values;
    int sensorNum = tmpSchemas.size();

    long value = 100L;
    for (int r = 0; r < rowNum; r++, value++) {
      int row = tablet.rowSize++;
      timestamps[row] = timestamp++;
      for (int i = 0; i < sensorNum; i++) {
        if (i == 1) {
          Binary[] textSensor = (Binary[]) values[i];
          textSensor[row] =
              new Binary("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa");
          continue;
        }
        long[] longSensor = (long[]) values[i];
        longSensor[row] = value;
      }
      // write
      if (tablet.rowSize == tablet.getMaxRowNumber()) {
        tsFileWriter.writeAligned(tablet);
        tablet.reset();
      }
    }
    // write
    if (tablet.rowSize != 0) {
      tsFileWriter.writeAligned(tablet);
      tablet.reset();
    }
  }
}
