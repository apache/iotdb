package org.apache.iotdb.cluster.utils;

import org.apache.iotdb.db.qp.physical.crud.CreateTemplatePlan;
import org.apache.iotdb.tsfile.file.metadata.enums.CompressionType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class CreateTemplatePlanUtil {

  public static CreateTemplatePlan getCreateTemplatePlan() {
    // create createTemplatePlan for template
    List<List<String>> measurementList = new ArrayList<>();
    measurementList.add(Collections.singletonList("template_sensor"));
    List<String> measurements = new ArrayList<>();
    for (int j = 0; j < 10; j++) {
      measurements.add("s" + j);
    }
    measurementList.add(measurements);

    List<List<TSDataType>> dataTypeList = new ArrayList<>();
    dataTypeList.add(Collections.singletonList(TSDataType.INT64));
    List<TSDataType> dataTypes = new ArrayList<>();
    for (int j = 0; j < 10; j++) {
      dataTypes.add(TSDataType.INT64);
    }
    dataTypeList.add(dataTypes);

    List<List<TSEncoding>> encodingList = new ArrayList<>();
    encodingList.add(Collections.singletonList(TSEncoding.RLE));
    List<TSEncoding> encodings = new ArrayList<>();
    for (int j = 0; j < 10; j++) {
      encodings.add(TSEncoding.RLE);
    }
    encodingList.add(encodings);

    List<CompressionType> compressionTypes = new ArrayList<>();
    for (int j = 0; j < 11; j++) {
      compressionTypes.add(CompressionType.SNAPPY);
    }

    List<String> schemaNames = new ArrayList<>();
    schemaNames.add("template_sensor");
    schemaNames.add("vector");

    return new CreateTemplatePlan(
        "template", schemaNames, measurementList, dataTypeList, encodingList, compressionTypes);
  }
}
