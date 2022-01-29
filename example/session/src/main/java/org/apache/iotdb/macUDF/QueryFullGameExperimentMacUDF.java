package org.apache.iotdb.macUDF;

import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.StatementExecutionException;
import org.apache.iotdb.session.Session;
import org.apache.iotdb.session.SessionDataSet;
import org.apache.iotdb.tsfile.read.common.RowRecord;

import org.apache.thrift.TException;

public class QueryFullGameExperimentMacUDF {

  private static final String queryFormat =
      "select M4(%1$s,'tqs'='%3$d','tqe'='%4$d','w'='%5$d') from %2$s where time>=%3$d and time<%4$d";

  public static Session session;

  public static void main(String[] args)
      throws IoTDBConnectionException, StatementExecutionException, TException {
    int intervalNum = Integer.parseInt(args[0]);
    String measurement = "s6";
    String device = "root.game";
    session = new Session("127.0.0.1", 6667, "root", "root");
    session.open(false);
    SessionDataSet dataSet;
    long minTime = 0L;
    long maxTime = 4264605928301L;
    long interval = (long) Math.ceil((double) (maxTime - minTime) / intervalNum);
    maxTime = minTime + interval * intervalNum;
    String sql = String.format(queryFormat, measurement, device, minTime, maxTime, intervalNum);
    dataSet = session.executeQueryStatement(sql);
    while (dataSet.hasNext()) {
      RowRecord r = dataSet.next();
    }
    session.executeNonQueryStatement("clear cache");
    dataSet = session.executeFinish();
    String info = dataSet.getFinishResult();
    System.out.println(info);
    dataSet.closeOperationHandle();
    session.close();
  }
}
