package org.apache.iotdb;

import org.apache.iotdb.common.rpc.thrift.TAggregationType;
import org.apache.iotdb.isession.SessionDataSet;
import org.apache.iotdb.isession.util.Version;
import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.StatementExecutionException;
import org.apache.iotdb.session.Session;

import java.util.ArrayList;
import java.util.List;

public class AggregationExample {

  private static Session session;

  private static final String LOCAL_HOST = "127.0.0.1";

  public static void main(String[] args) throws IoTDBConnectionException {
    session =
        new Session.Builder()
            .host(LOCAL_HOST)
            .port(6668)
            .username("root")
            .password("root")
            .version(Version.V_1_0)
            .build();
    session.open(false);

    List<String> paths = new ArrayList<>();
    paths.add("root.cpu.d1.**");
    // paths.add("root.cpu.d1.s1");
    // paths.add("root.cpu.d1.s1");

    List<TAggregationType> aggregations = new ArrayList<>();
    aggregations.add(TAggregationType.COUNT);
    // aggregations.add(TAggregationType.MAX_VALUE);
    // aggregations.add(TAggregationType.COUNT_TIME);
    try (SessionDataSet sessionDataSet = session.executeAggregationQuery(paths, aggregations)) {
      System.out.println(sessionDataSet.getColumnNames());
      sessionDataSet.setFetchSize(1024);
      while (sessionDataSet.hasNext()) {
        System.out.println(sessionDataSet.next());
      }
    } catch (StatementExecutionException e) {
      throw new RuntimeException(e);
    }
  }
}
