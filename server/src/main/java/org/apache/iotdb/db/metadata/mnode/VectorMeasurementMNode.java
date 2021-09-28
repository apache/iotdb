package org.apache.iotdb.db.metadata.mnode;

import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.write.schema.VectorMeasurementSchema;

import java.util.List;

public class VectorMeasurementMNode extends MeasurementMNode {

  /** measurement's Schema for one aligned timeseries represented by current leaf node */
  private VectorMeasurementSchema schema;

  VectorMeasurementMNode(
      IEntityMNode parent, String measurementName, VectorMeasurementSchema schema, String alias) {
    super(parent, measurementName, alias);
    this.schema = schema;
  }

  @Override
  public VectorMeasurementSchema getSchema() {
    return schema;
  }

  public void setSchema(VectorMeasurementSchema schema) {
    this.schema = schema;
  }

  @Override
  public int getMeasurementCount() {
    return schema.getSubMeasurementsCount();
  }

  @Override
  public TSDataType getDataType(String measurementId) {
    int index = schema.getSubMeasurementIndex(measurementId);
    return schema.getSubMeasurementsTSDataTypeList().get(index);
  }

  public List<String> getSubMeasurementList() {
    return schema.getSubMeasurementsList();
  }

  @Override
  public boolean isVectorMeasurement() {
    return true;
  }
}
