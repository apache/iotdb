package org.apache.iotdb.session.doublewrite;

import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.StatementExecutionException;
import org.apache.iotdb.session.Session;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;

import java.util.ArrayList;
import java.util.List;

public class MultiThreadTest {
  private static ArrayList<SingleThread> threads = new ArrayList<>();

  public static void main(String[] args) {
    for (int i = 0; i < 10; i++) {
      String deviceId = "root.dw.d0" + (i / 2 + 1);
      List<String> measurements = new ArrayList<>();
      measurements.add("s1");
      measurements.add("s2");
      measurements.add("s3");
      SingleThread threadA = new SingleThread("127.0.0.1", 6668, deviceId, measurements);
      threads.add(threadA);

      measurements = new ArrayList<>();
      measurements.add("s4");
      measurements.add("s5");
      measurements.add("s6");
      SingleThread threadB = new SingleThread("127.0.0.1", 6668, deviceId, measurements);
      threads.add(threadB);
    }

    for (SingleThread thread : threads) {
      new Thread(thread).start();
    }
  }
}
