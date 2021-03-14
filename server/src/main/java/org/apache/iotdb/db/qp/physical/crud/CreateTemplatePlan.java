package org.apache.iotdb.db.qp.physical.crud;

import org.apache.iotdb.db.metadata.PartialPath;
import org.apache.iotdb.db.qp.logical.Operator.OperatorType;
import org.apache.iotdb.db.qp.physical.PhysicalPlan;
import org.apache.iotdb.tsfile.file.metadata.enums.CompressionType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;

import java.util.List;

public class CreateTemplatePlan extends PhysicalPlan {

  String name;
  List<List<String>> measurements;
  List<List<TSDataType>> dataTypes;
  List<List<TSEncoding>> encodings;
  List<CompressionType> compressors;

  public String getName() {
    return name;
  }

  public void setName(String name) {
    this.name = name;
  }

  public List<List<String>> getMeasurements() {
    return measurements;
  }

  public void setMeasurements(List<List<String>> measurements) {
    this.measurements = measurements;
  }

  public List<List<TSDataType>> getDataTypes() {
    return dataTypes;
  }

  public void setDataTypes(List<List<TSDataType>> dataTypes) {
    this.dataTypes = dataTypes;
  }

  public List<List<TSEncoding>> getEncodings() {
    return encodings;
  }

  public void setEncodings(List<List<TSEncoding>> encodings) {
    this.encodings = encodings;
  }

  public List<CompressionType> getCompressors() {
    return compressors;
  }

  public void setCompressors(List<CompressionType> compressors) {
    this.compressors = compressors;
  }

  public CreateTemplatePlan(
      String name,
      List<List<String>> measurements,
      List<List<TSDataType>> dataTypes,
      List<List<TSEncoding>> encodings,
      List<CompressionType> compressors) {
    super(false, OperatorType.CREATE_TEMPLATE);
    this.name = name;
    this.measurements = measurements;
    this.dataTypes = dataTypes;
    this.encodings = encodings;
    this.compressors = compressors;
  }

  @Override
  public List<PartialPath> getPaths() {
    return null;
  }
}
