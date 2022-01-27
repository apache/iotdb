package org.apache.iotdb;

import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.StatementExecutionException;
import org.apache.iotdb.session.Session;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.Collections;

public class WriteCSVToIoTDB {

  public static final String fullPath = "root.kobelco.trans.03.1090001603.2401604.KOB_0002_00_67";
  public static final String path = "/home/kyy/Documents/kdd/data/" + fullPath + ".csv";

  public static void main(String[] args)
      throws IOException, IoTDBConnectionException, StatementExecutionException {

    String[] nodes = fullPath.split("\\.");
    String measurements = nodes[nodes.length - 1];
    String device = fullPath.replace("." + measurements, "");

    Session session = new Session("127.0.0.1", 6667, "root", "root");
    session.open(false);

    File f = new File(path);
    String line = null;
    BufferedReader reader = new BufferedReader(new FileReader(f));
    while ((line = reader.readLine()) != null) {
      String[] split = line.split(",");
      long timestamp = Long.valueOf(split[0]);
      long value = Long.valueOf(split[1]);
      session.insertRecord(
          device,
          timestamp,
          Collections.singletonList(measurements),
          Collections.singletonList(TSDataType.INT64),
          value);
    }
    session.executeNonQueryStatement("flush");
    session.close();
  }
}
