package org.apache.iotdb.influxdb;

import org.influxdb.InfluxDB;

public enum IotDBInfluxDBFactory {
  INSTANCE;

  private IotDBInfluxDBFactory() {}

  public static InfluxDB connect(String url, String username, String password) {
    IotDBInfluxDBUtils.checkNonEmptyString(url, "url");
    IotDBInfluxDBUtils.checkNonEmptyString(username, "username");
    return new IotDBInfluxDB(url, username, password);
  }

  public static InfluxDB connect(String host, int rpcPort, String userName, String password) {
    IotDBInfluxDBUtils.checkNonEmptyString(host, "host");
    IotDBInfluxDBUtils.checkNonEmptyString(userName, "username");
    return new IotDBInfluxDB(host, rpcPort, userName, password);
  }
}
