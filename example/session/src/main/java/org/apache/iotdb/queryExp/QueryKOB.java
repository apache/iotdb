package org.apache.iotdb.queryExp;

import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.StatementExecutionException;
import org.apache.iotdb.session.Session;
import org.apache.iotdb.session.SessionDataSet;
import org.apache.iotdb.tsfile.read.common.RowRecord;

import org.apache.thrift.TException;

public class QueryKOB {

  // * (1) min_time(%s), max_time(%s), first_value(%s), last_value(%s), min_value(%s), max_value(%s)
  //       => Don't change the sequence of the above six aggregates!
  // * (2) group by ([tqs,tqe),IntervalLength) => Make sure (tqe-tqs) is divisible by
  // IntervalLength!
  // * (3) NOTE the time unit of interval. Update for different datasets!
  private static final String queryFormat =
      "select min_time(%s), max_time(%s), first_value(%s), last_value(%s), min_value(%s), max_value(%s) "
          + "from %s "
          + "group by ([%d, %d), %dms)"; // note this "ms" time precision

  private static final String queryFormat_UDF =
      "select M4(%1$s,'tqs'='%3$d','tqe'='%4$d','w'='%5$d') from %2$s where time>=%3$d and time<%4$d";

  public static Session session;

  public static void main(String[] args)
      throws IoTDBConnectionException, StatementExecutionException, TException {
    // fixed time series path
    String measurement = "KOB_0002_00_67"; // [[update]]
    String device = "root.kobelco.trans.03.1090001603.2401604"; // [[update]]
    // used to bound tqs random position
    long dataMinTime = 1616805035973L; // [[update]]
    long dataMaxTime = 1627380839564L; // 1627380839563+1 // [[update]]

    // 实验自变量1：[tqs,tqe) range length, i.e., tqe-tqs
    long range = Long.parseLong(args[0]);
    // 实验自变量2：w数量
    int intervalNum = Integer.parseInt(args[1]);
    // 实验自变量3：方法
    // 1: MAC, 2: MOC, 3: CPV
    int approach = Integer.parseInt(args[2]);
    if (approach != 1 && approach != 2 && approach != 3) {
      throw new TException("Wrong input parameter approach!");
    }
    if (approach != 1) {
      // MOC and CPV sql are the same sql: queryFormat.
      // Set the server parameter in iotdb-engine.properties: enable_CPV=true for CPV, false for
      // MOC.
      if (approach == 2) { // MOC
        System.out.println(
            "MAKE SURE you have set the enable_CPV as false in `iotdb-engine.properties` for MOC!");
      } else { // CPV
        System.out.println(
            "MAKE SURE you have set the enable_CPV as true in `iotdb-engine.properties` for CPV!");
      }
    }

    long minTime;
    long maxTime;
    long interval;
    if (range >= (dataMaxTime - dataMinTime)) {
      minTime = dataMinTime;
      interval = (long) Math.ceil((double) (dataMaxTime - dataMinTime) / intervalNum);
    } else {
      // randomize between [dataMinTime, dataMaxTime-range]
      minTime =
          (long) Math.ceil(dataMinTime + Math.random() * (dataMaxTime - range - dataMinTime + 1));
      interval = (long) Math.ceil((double) range / intervalNum);
    }
    maxTime = minTime + interval * intervalNum;

    session = new Session("127.0.0.1", 6667, "root", "root");
    session.open(false);
    session.setFetchSize(100000); // this is important. Set it big to avoid multiple fetch.

    String sql;
    if (approach == 1) { // MAC UDF
      sql =
          String.format(queryFormat_UDF, measurement, device, minTime, maxTime, intervalNum); // MAC
    } else {
      // MOC and CPV sql use the same sql queryFormat.
      sql =
          String.format(
              queryFormat,
              measurement,
              measurement,
              measurement,
              measurement,
              measurement,
              measurement,
              device,
              minTime,
              maxTime,
              interval);
    }

    SessionDataSet dataSet;
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
