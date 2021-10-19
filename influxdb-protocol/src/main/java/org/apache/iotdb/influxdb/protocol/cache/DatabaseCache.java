package org.apache.iotdb.influxdb.protocol.cache;

import java.util.HashMap;
import java.util.Map;

public class DatabaseCache {
  // Tag list and order under current measurement
  private Map<String, Integer> tagOrders = new HashMap<>();

  private Map<String, Map<String, Integer>> measurementTagOrder = new HashMap<>();

  // Database currently selected by influxdb
  private String database;

  public DatabaseCache() {}

  public DatabaseCache(
      Map<String, Integer> tagOrders,
      String database,
      Map<String, Map<String, Integer>> measurementTagOrder) {
    this.tagOrders = tagOrders;
    this.database = database;
    this.measurementTagOrder = measurementTagOrder;
  }

  public Map<String, Integer> getTagOrders() {
    return tagOrders;
  }

  public void setTagOrders(Map<String, Integer> tagOrders) {
    this.tagOrders = tagOrders;
  }

  public Map<String, Map<String, Integer>> getMeasurementTagOrder() {
    return measurementTagOrder;
  }

  public String getDatabase() {
    return database;
  }

  public void setDatabase(String database) {
    this.database = database;
  }

  public void setMeasurementTagOrder(Map<String, Map<String, Integer>> measurementTagOrder) {
    this.measurementTagOrder = measurementTagOrder;
  }
}
