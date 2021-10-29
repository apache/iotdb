package org.apache.iotdb.tsfile.utils;

import java.util.HashMap;
import java.util.Map;
import org.apache.iotdb.tsfile.write.schema.IMeasurementSchema;

public class MeasurementGroup {
  private boolean isAligned = false;
  private Map<String, IMeasurementSchema> measurementSchemaMap;

  public MeasurementGroup(boolean isAligned) {
    this.isAligned = isAligned;
    measurementSchemaMap = new HashMap<>();
  }

  public MeasurementGroup(boolean isAligned, Map<String, IMeasurementSchema> measurementSchemaMap) {
    this.isAligned = isAligned;
    this.measurementSchemaMap = measurementSchemaMap;
  }

  public boolean isAligned() {
    return isAligned;
  }

  public void setAligned(boolean aligned) {
    isAligned = aligned;
  }

  public Map<String, IMeasurementSchema> getMeasurementSchemaMap() {
    return measurementSchemaMap;
  }

  public void setMeasurementSchemaMap(Map<String, IMeasurementSchema> measurementSchemaMap) {
    this.measurementSchemaMap = measurementSchemaMap;
  }
}
