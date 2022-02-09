/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iotdb.cluster.utils;

import org.apache.iotdb.db.qp.physical.sys.CreateTemplatePlan;
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

    List<List<CompressionType>> compressionTypes = new ArrayList<>();
    compressionTypes.add(Collections.singletonList(CompressionType.SNAPPY));
    List<CompressionType> compressors = new ArrayList<>();
    for (int j = 0; j < 11; j++) {
      compressors.add(CompressionType.SNAPPY);
    }
    compressionTypes.add(compressors);

    List<String> schemaNames = new ArrayList<>();
    schemaNames.add("template_sensor");
    schemaNames.add("vector");

    return new CreateTemplatePlan(
        "template", schemaNames, measurementList, dataTypeList, encodingList, compressionTypes);
  }
}
