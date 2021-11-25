package org.apache.iotdb.generator;

import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.StatementExecutionException;
import org.apache.iotdb.session.ClusterSession;
import org.apache.iotdb.session.SessionDataSet;
import org.apache.iotdb.tsfile.write.record.Tablet;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class GeneratorEntrance {
  private static final Logger logger = LoggerFactory.getLogger(GeneratorEntrance.class);

  public static void main(String[] args)
      throws IoTDBConnectionException, StatementExecutionException, InterruptedException {
    args = new String[6];
    args[1] = "127.0.0.1:6667";
    args[2] = "root.sg1.d1.s1";
    args[3] = "3";
    args[4] = "1000";
    String[] addressElements = args[1].split(":");
    String seriesPath = args[2];
    int timeInterval = Integer.parseInt(args[3]) * 1000;
    int batchNum = Integer.parseInt(args[4]);
    String[] pathElements = seriesPath.split("\\.");
    String measurementId = pathElements[pathElements.length - 1];
    String deviceId = seriesPath.substring(0, seriesPath.length() - measurementId.length() - 1);

    ClusterSession clusterSession =
        new ClusterSession(addressElements[0], Integer.parseInt(addressElements[1]));

    long timestampForInsert = 0;
    while (true) {
      long startTime = System.currentTimeMillis();
      Tablet tablet =
          Generator.generateTablet(
              deviceId, pathElements[pathElements.length - 1], timestampForInsert, batchNum);
      clusterSession.insertTablet(tablet);
      logger.info("Insert {} data points to {}", batchNum, seriesPath);
      String query =  String.format("select count(%s) from %s", measurementId, deviceId);
      SessionDataSet sessionDataSet =
          clusterSession.queryTablet(
                  query, deviceId);
      logger.info("Execute query {} with result : {}",query,sessionDataSet.next().getFields().get(0));
      timestampForInsert += batchNum;
      long endTime = System.currentTimeMillis();
      if (timeInterval - (endTime - startTime)>0) {
        Thread.sleep(timeInterval - (endTime - startTime));
      }
    }
  }
}
