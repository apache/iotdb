package org.apache.iotdb.metrics.utils;

import java.util.HashMap;
import java.util.Map;

public enum MonitorType {
  dropwizard("dropwizard"),
  micrometer("micrometer");

  private String name;

  MonitorType(String name) {
    this.name = name;
  }

  public String getName() {
    return name;
  }

  private static final Map<String, MonitorType> lookup = new HashMap<>();

  static {
    for (MonitorType monitorType : MonitorType.values()) {
      lookup.put(monitorType.getName(), monitorType);
    }
  }

  public static MonitorType get(String name) {
    return lookup.get(name);
  }

  @Override
  public String toString() {
    return name;
  }
}
