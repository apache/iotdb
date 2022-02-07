// package org.apache.iotdb.writeData;
//
// import org.apache.iotdb.rpc.IoTDBConnectionException;
// import org.apache.iotdb.rpc.StatementExecutionException;
// import org.apache.iotdb.session.Session;
// import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
//
// import java.util.ArrayList;
// import java.util.Collections;
// import java.util.List;
// import java.util.Random;
//
// public class WriteSyntheticDataWithDeletes {
//
//  /**
//   * !!!!!!!Before writing data, make sure check the following server parameters:
//   *
//   * <p>system_dir data_dirs wal_dir timestamp_precision=ms unseq_tsfile_size=1073741824 # maximum
//   * size of unseq TsFile is 1024^3 Bytes seq_tsfile_size=1073741824 # maximum size of seq TsFile
// is
//   * 1024^3 Bytes avg_series_point_number_threshold=10000 # each chunk contains 10000 data points
//   * compaction_strategy=NO_COMPACTION # compaction between levels is disabled
//   * enable_unseq_compaction=false # unseq compaction is disabled
//   */
//  public static final String device = "root.vehicle.d0";
//
//  public static final String measurements = "s0";
//
//  public static void main(String[] args)
//      throws IoTDBConnectionException, StatementExecutionException {
//    // 实验自变量1：每隔deleteFreq时间（本实验时间单位ms）就执行一次删除
//    long deleteFreq = Long.parseLong(args[0]);
//    // 实验自变量2：每次删除的时间长度
//    long deleteLen = Long.parseLong(args[1]);
//
//    Session session = new Session("127.0.0.1", 6667, "root", "root");
//    session.open(false);
//
//    List<String> paths = new ArrayList<>();
//    paths.add(device + "." + measurements);
//
//    for (long timestamp = 0; timestamp < 10000000; timestamp++) {
//      if (timestamp != 0 && timestamp % deleteFreq == 0) {
//        // [timestamp-deleteFreq, timestamp-1]内随机取一个删除时间起点
//        long min = timestamp - deleteFreq;
//        long max = timestamp - 1;
//        long deleteStartTime = min + (((long) (new Random().nextDouble() * (max - min))));
//        // delete length 5000ms, half the time length of a chunk
//        long deleteEndTime = deleteStartTime + deleteLen;
//        session.deleteData(paths, deleteStartTime, deleteEndTime);
//      }
//
//      double value = Math.random();
//      session.insertRecord(
//          device,
//          timestamp,
//          Collections.singletonList(measurements),
//          Collections.singletonList(TSDataType.DOUBLE),
//          value);
//    }
//    session.executeNonQueryStatement("flush");
//    session.close();
//  }
// }
