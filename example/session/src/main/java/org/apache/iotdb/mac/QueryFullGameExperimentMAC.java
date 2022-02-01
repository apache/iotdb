//package org.apache.iotdb.mac;
//
//import org.apache.iotdb.rpc.IoTDBConnectionException;
//import org.apache.iotdb.rpc.StatementExecutionException;
//import org.apache.iotdb.session.Session;
//import org.apache.iotdb.session.SessionDataSet;
//import org.apache.iotdb.tsfile.read.common.RowRecord;
//
//import org.apache.thrift.TException;
//
//public class QueryFullGameExperimentMAC {
//
//  private static final String queryFormat =
//      "select %s " + "from %s " + "where time >= %d and time < %d";
//
//  public static Session session;
//
//  public static void main(String[] args)
//      throws IoTDBConnectionException, StatementExecutionException, TException {
//    int intervalNum = Integer.parseInt(args[0]);
//    String measurement = "s6";
//    String device = "root.game";
//    session = new Session("127.0.0.1", 6667, "root", "root");
//    session.open(false);
//    SessionDataSet dataSet;
//    long minTime = 0L;
//    long maxTime = 4264605928301L;
//    long interval = (long) Math.ceil((double) (maxTime - minTime) / intervalNum);
//    maxTime = minTime + interval * intervalNum;
//
//    for (int i = 0; i < intervalNum; i++) {
//      long start = i * interval;
//      long end = (i + 1) * interval;
//      String sql = String.format(queryFormat, measurement, device, start, end);
//      dataSet = session.executeQueryStatement(sql);
//      while (dataSet.hasNext()) {
//        RowRecord r = dataSet.next();
//      }
//    }
//    session.executeNonQueryStatement("clear cache");
//    dataSet = session.executeFinish();
//    String info = dataSet.getFinishResult();
//    System.out.println(info);
//    dataSet.closeOperationHandle();
//    session.close();
//  }
//}
