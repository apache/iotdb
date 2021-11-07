package org.apache.iotdb.tsfile.utils;

import org.apache.iotdb.tsfile.write.schema.UnaryMeasurementSchema;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class MeasurementGroup {
  private boolean isAligned;
  private Map<String, UnaryMeasurementSchema> measurementSchemaMap;

  public MeasurementGroup(boolean isAligned) {
    this.isAligned = isAligned;
    measurementSchemaMap = new HashMap<>();
  }

  public MeasurementGroup(boolean isAligned, List<UnaryMeasurementSchema> measurementSchemas) {
    this.isAligned = isAligned;
    measurementSchemaMap = new HashMap<>();
    for (UnaryMeasurementSchema schema : measurementSchemas) {
      measurementSchemaMap.put(schema.getMeasurementId(), schema);
    }
  }

  public MeasurementGroup(
      boolean isAligned, Map<String, UnaryMeasurementSchema> measurementSchemaMap) {
    this.isAligned = isAligned;
    this.measurementSchemaMap = measurementSchemaMap;
  }

  public boolean isAligned() {
    return isAligned;
  }

  public void setAligned(boolean aligned) {
    isAligned = aligned;
  }

  public Map<String, UnaryMeasurementSchema> getMeasurementSchemaMap() {
    return measurementSchemaMap;
  }

  public void setMeasurementSchemaMap(Map<String, UnaryMeasurementSchema> measurementSchemaMap) {
    this.measurementSchemaMap = measurementSchemaMap;
  }
}
