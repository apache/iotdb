package org.apache.iotdb.spark.it;

import org.apache.iotdb.it.env.EnvFactory;
import org.apache.iotdb.rpc.IoTDBConnectionException;

import org.apache.spark.sql.SparkSession;

public class AbstractTest {
  protected SparkSession spark;

  protected static String ip;
  protected static String port;

  protected String jdbcUrl;

  public void before() throws ClassNotFoundException, IoTDBConnectionException {
    EnvFactory.getEnv().initClusterEnvironment();
    ip = EnvFactory.getEnv().getIP();
    port = EnvFactory.getEnv().getPort();

    jdbcUrl = String.format("jdbc:iotdb://%s:%s/", ip, port);

    spark =
        SparkSession.builder()
            .config("spark.master", "local")
            .appName("spark-iotdb-connector read test")
            .getOrCreate();
  }

  public void after() throws IoTDBConnectionException {
    if (spark != null) {
      spark.sparkContext().stop();
    }

    EnvFactory.getEnv().cleanClusterEnvironment();
  }
}
