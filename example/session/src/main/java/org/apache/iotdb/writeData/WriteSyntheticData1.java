package org.apache.iotdb.writeData;

import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.StatementExecutionException;
import org.apache.iotdb.session.Session;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;

import java.io.IOException;
import java.util.Collections;

public class WriteSyntheticData1 {

  /**
   * !!!!!!!Before writing data, make sure check the following server parameters:
   *
   * <p>system_dir=/data3/ruilei/iotdb-server-0.12.4/synData1/system
   * data_dirs=/data3/ruilei/iotdb-server-0.12.4/synData1/data
   * wal_dir=/data3/ruilei/iotdb-server-0.12.4/synData1/wal timestamp_precision=ms
   * unseq_tsfile_size=1073741824 # maximum size of unseq TsFile is 1024^3 Bytes
   * seq_tsfile_size=1073741824 # maximum size of seq TsFile is 1024^3 Bytes
   * avg_series_point_number_threshold=10000 # each chunk contains 10000 data points
   * compaction_strategy=NO_COMPACTION # compaction between levels is disabled
   * enable_unseq_compaction=false # unseq compaction is disabled
   */
  public static final String device = "root.vehicle.d0";

  public static final String measurements = "s0";

  public static void main(String[] args)
      throws IOException, IoTDBConnectionException, StatementExecutionException {

    Session session = new Session("127.0.0.1", 6667, "root", "root");
    session.open(false);

    for (long timestamp = 1; timestamp <= 10000000; timestamp++) {
      double value = Math.random();
      session.insertRecord(
          device,
          timestamp,
          Collections.singletonList(measurements),
          Collections.singletonList(TSDataType.DOUBLE),
          value);
    }
    session.executeNonQueryStatement("flush");
    session.close();
  }
}
