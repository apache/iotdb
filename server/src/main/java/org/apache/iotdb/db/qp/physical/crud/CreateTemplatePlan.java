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
