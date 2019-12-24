package org.apache.iotdb;

import org.apache.iotdb.session.IoTDBSessionException;
import org.apache.iotdb.session.Session;
import org.apache.iotdb.tsfile.file.metadata.enums.CompressionType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.apache.iotdb.tsfile.write.record.RowBatch;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;
import org.apache.iotdb.tsfile.write.schema.Schema;

public class SessionExample {

  private static Session session;
  private static int sensorNum = 100;
  private static int rowNumber = 20_000_000;

  public static void main(String[] args) throws IoTDBSessionException {
    session = new Session("127.0.0.1", 6667, "root", "root");
    session.open();
    session.setStorageGroup("root.sg1");
    for (int i = 0; i < sensorNum; i++) {
      session.createTimeseries("root.sg1.d1.s" + i, TSDataType.INT64, TSEncoding.RLE,
          CompressionType.SNAPPY);
    }
    insertRowBatch();
    session.close();
  }

  private static void insertRowBatch() throws IoTDBSessionException {
    Schema schema = new Schema();
    for (int i = 0; i < sensorNum; i++) {
      schema.registerMeasurement(new MeasurementSchema("s" + i, TSDataType.INT64, TSEncoding.RLE));
    }

    RowBatch rowBatch = schema.createRowBatch("root.sg1.d1", 1000);

    long[] timestamps = rowBatch.timestamps;
    Object[] values = rowBatch.values;

    for (long time = 0; time < rowNumber; time++) {
      int row = rowBatch.batchSize++;
      timestamps[row] = time;
      for (int i = 0; i < sensorNum; i++) {
        long[] sensor = (long[]) values[i];
        sensor[row] = i;
      }
      if (rowBatch.batchSize == rowBatch.getMaxBatchSize()) {
        session.insertBatch(rowBatch);
        rowBatch.reset();
      }
    }

    if (rowBatch.batchSize != 0) {
      session.insertBatch(rowBatch);
      rowBatch.reset();
    }
  }
}
