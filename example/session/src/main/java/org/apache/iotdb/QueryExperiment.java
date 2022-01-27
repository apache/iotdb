package org.apache.iotdb;

import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.StatementExecutionException;
import org.apache.iotdb.session.Session;
import org.apache.iotdb.session.SessionDataSet;
import org.apache.iotdb.tsfile.read.common.RowRecord;

import org.apache.thrift.TException;

import java.util.Random;

public class QueryExperiment {

  private static Session session;
  private static final String timeseries =
      "root.kobelco.trans.03.1090001603.2401604.KOB_0002_00_67";
  private static final String queryFormat =
      "select min_time(%s), max_time(%s), first_value(%s), last_value(%s), min_value(%s), max_value(%s) "
          + "from %s "
          + "group by ([%d, %d), %dms)";
  private static final long totalIntervalLengthMS = 1 * 60 * 60 * 1000L;
  private static final int totalIntervalNumber = 1000;
  private static Random random = new Random(System.currentTimeMillis());
  private static final int sqlNum = 1;

  public static void main(String[] args)
      throws IoTDBConnectionException, StatementExecutionException, TException {

    String[] split = timeseries.split("\\.");
    String measurement = split[split.length - 1];
    String device = timeseries.replace("." + measurement, "");

    session = new Session("127.0.0.1", 6667, "root", "root");
    session.open(false);

    SessionDataSet dataSet;

    //    dataSet =
    //        session.executeQueryStatement(
    //            "select min_time(" + measurement + "), max_time(" + measurement + ") from " +
    // device);
    //    long minTime = -1;
    //    long maxTime = -1;
    //    while (dataSet.hasNext()) {
    //      RowRecord r = dataSet.next();
    //      minTime = r.getFields().get(0).getLongV();
    //      maxTime = r.getFields().get(1).getLongV();
    //      System.out.println(minTime + ", " + maxTime);
    //    }
    //    assert minTime != -1;
    //    assert maxTime != -1;

    long minTime = 1616805035973L;
    long maxTime = 1627380839563L;

    for (int i = 0; i < sqlNum; i++) {
      long startTime = nextLong(minTime, maxTime - totalIntervalLengthMS);
      long endTime = startTime + totalIntervalLengthMS;
      String sql =
          String.format(
              queryFormat,
              measurement,
              measurement,
              measurement,
              measurement,
              measurement,
              measurement,
              device,
              startTime,
              endTime,
              (int) (totalIntervalLengthMS / totalIntervalNumber));
      dataSet = session.executeQueryStatement(sql);
      while (dataSet.hasNext()) {
        RowRecord r = dataSet.next();
      }
      session.executeNonQueryStatement("clear cache");
    }

    dataSet = session.executeFinish();
    String info = dataSet.getFinishResult();

    System.out.println(info);

    dataSet.closeOperationHandle();
    session.close();
  }

  public static long nextLong(long min, long max) {
    if (max <= min) {
      throw new IllegalArgumentException("max is less than min!");
    }
    return (long) (random.nextDouble() * (max - min)) + min;
  }
}
