package org.apache.iotdb;

import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.StatementExecutionException;
import org.apache.iotdb.session.Session;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;

public class Write {

  static final String FILE_PATH =
      "/Users/surevil/Documents/private/incubator-iotdb/example/session/src/main/java/org/apache/iotdb/data_for_iotdb.csv";
  static final String STORAGE_GROUP = "root.storage_group";
  static final String DEVICE_ID = STORAGE_GROUP + ".device";

  public static void main(String[] args)
      throws IOException, IoTDBConnectionException, StatementExecutionException {

    Session session = new Session("127.0.0.1", 6667, "root", "root");
    session.open(false);

    File file = new File(FILE_PATH);
    BufferedReader reader = new BufferedReader(new FileReader(file));
    String line;
    while ((line = reader.readLine()) != null) {
      String[] dataPoint = line.split(",");
      Double arrivalTime = Double.parseDouble(dataPoint[0]);
      Integer generateTime = Integer.parseInt(String.valueOf(dataPoint[1].split("\\.")[0]));
      Double value = Double.parseDouble(dataPoint[2]);

      List<String> measurements = Arrays.asList("arrival_time", "value");
      List<TSDataType> types = Arrays.asList(TSDataType.DOUBLE, TSDataType.DOUBLE);
      session.insertRecord(DEVICE_ID, (long) generateTime, measurements, types, arrivalTime, value);
    }
    session.close();
    reader.close();
  }
}
