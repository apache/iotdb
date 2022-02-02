package org.apache.iotdb.queryExp;

import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.StatementExecutionException;
import org.apache.iotdb.session.Session;
import org.apache.iotdb.session.SessionDataSet;
import org.apache.iotdb.tsfile.read.common.RowRecord;

import org.apache.thrift.TException;

/**
 * !!!!!!!Before query data, make sure check the following server parameters:
 *
 * <p>system_dir=/data3/ruilei/iotdb-server-0.12.4/synData1/system
 * data_dirs=/data3/ruilei/iotdb-server-0.12.4/synData1/data
 * wal_dir=/data3/ruilei/iotdb-server-0.12.4/synData1/wal timestamp_precision=ms
 * unseq_tsfile_size=1073741824 # maximum size of unseq TsFile is 1024^3 Bytes
 * seq_tsfile_size=1073741824 # maximum size of seq TsFile is 1024^3 Bytes
 * avg_series_point_number_threshold=10000 # each chunk contains 10000 data points
 * compaction_strategy=NO_COMPACTION # compaction between levels is disabled
 * enable_unseq_compaction=false # unseq compaction is disabled
 */
public class QuerySyntheticData1 {

  // * (1) min_time(%s), max_time(%s), first_value(%s), last_value(%s), min_value(%s), max_value(%s)
  //       => Don't change the sequence of the above six aggregates!
  // * (2) group by ([tqs,tqe),IntervalLength) => Make sure (tqe-tqs) is divisible by
  // IntervalLength!
  // * (3) NOTE the time unit of interval. Update for different datasets!!!!!!!!!!!
  private static final String queryFormat =
      "select min_time(%s), max_time(%s), first_value(%s), last_value(%s), min_value(%s), max_value(%s) "
          + "from %s "
          + "group by ([%d, %d), %dms)";

  private static final String queryFormat_UDF =
      "select M4(%1$s,'tqs'='%3$d','tqe'='%4$d','w'='%5$d') from %2$s where time>=%3$d and time<%4$d";

  public static Session session;

  public static void main(String[] args)
      throws IoTDBConnectionException, StatementExecutionException, TException {
    // fix parameters for synthetic data1
    String measurement = "s0";
    String device = "root.vehicle.d0";
    // fixed query total range
    long minTime = 0L;
    long maxTime = 10000000L; // unit:ms. Set in iotdb-engine.properties `timestamp_precision`.
    // 实验自变量1：w数量
    //    int intervalNum = Integer.parseInt(args[0]);
    int intervalNum = 50;
    // 实验自变量2：方法
    // 1: MAC, 2: MOC, 3: CPV
    //    int approach = Integer.parseInt(args[1]);
    int approach = 2;
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

    session = new Session("127.0.0.1", 6667, "root", "root");
    session.open(false);
    session.setFetchSize(100000); // this is important. Set it big to avoid multiple fetch.

    long interval = (long) Math.ceil((double) (maxTime - minTime) / intervalNum);
    maxTime = minTime + interval * intervalNum;

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
