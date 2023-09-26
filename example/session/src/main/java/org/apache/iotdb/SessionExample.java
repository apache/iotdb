package org.apache.iotdb;

import org.apache.iotdb.isession.SessionDataSet;
import org.apache.iotdb.isession.util.Version;
import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.StatementExecutionException;
import org.apache.iotdb.session.Session;
import org.apache.iotdb.tsfile.file.metadata.enums.CompressionType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.apache.iotdb.tsfile.write.record.Tablet;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;

@SuppressWarnings({"squid:S106", "squid:S1144", "squid:S125"})
public class SessionExample {

  private static Session session;
  private static Session sessionEnableRedirect;
  private static final String LOCAL_HOST = "127.0.0.1";
  private static final String ROOT_SG1_D1 = "root.sg.d";

  private static Random random = new Random();

  public static void main(String[] args)
      throws IoTDBConnectionException, StatementExecutionException {
    session =
        new Session.Builder()
            .host(LOCAL_HOST)
            .port(6667)
            .username("root")
            .password("root")
            .version(Version.V_1_0)
            .build();
    session.open(false);
    long start = System.currentTimeMillis();
    try (SessionDataSet dataSet = session.executeQueryStatement("select ** from root")) {
      dataSet.getColumnNames();
      dataSet.getColumnTypes();
//      System.out.println(dataSet.getColumnNames());
//      System.out.println(dataSet.getColumnTypes());
      while (dataSet.hasNext()) {
        dataSet.next();
        // System.out.println(dataSet.next());
      }
    }
    System.out.println("query test costs: " + (System.currentTimeMillis() - start) + "ms");
    session.close();
  }
}
