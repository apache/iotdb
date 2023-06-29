package org.apache.iotdb.flink.it;

import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.session.Session;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;

import java.util.ArrayList;
import java.util.List;

public class BoundedScanTest {
  private static final String deviceId = "root.test.flink.scan";

  protected static void prepareData(String host, int port) throws IoTDBConnectionException {
    Session session = new Session.Builder().host(host).port(port).build();
    session.open(false);
    List<Long> times = new ArrayList<>();
    List<List<String>> measurementsList = new ArrayList<>();
    List<List<TSDataType>> typesList = new ArrayList<>();
    List<List<Object>> valuesList = new ArrayList<>();

    ArrayList<String> measurements =
        new ArrayList<String>() {
          {
            for (int i = 0; i < 5; i++) {
              add(String.format("s%d", i));
            }
          }
        };
    ArrayList<TSDataType> types = new ArrayList<>();
    ArrayList<Object> values = new ArrayList<>();
    for (int i = 0; i < 1000; i++) {
      times.add(Long.valueOf(i));
    }
    //        session.insertRecordsOfOneDevice();
  }
}
