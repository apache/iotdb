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
package org.apache.iotdb.db.metadata;

import org.apache.iotdb.db.qp.physical.PhysicalPlan;
import org.apache.iotdb.db.qp.physical.sys.CreateTemplatePlan;
import org.apache.iotdb.db.utils.EnvironmentUtils;
import org.apache.iotdb.tsfile.file.metadata.enums.CompressionType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.Assert.assertEquals;

public class TemplateTest {
  @Before
  public void setUp() {
    EnvironmentUtils.envSetUp();
  }

  @After
  public void tearDown() throws Exception {
    EnvironmentUtils.cleanEnv();
  }

  @Test
  public void testCreateSchemaTemplateSerialization() throws IOException {
    CreateTemplatePlan plan = getTreeTemplatePlan();
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    DataOutputStream dos = new DataOutputStream(baos);
    plan.serialize(dos);
    byte[] byteArray = baos.toByteArray();
    ByteBuffer buffer = ByteBuffer.wrap(byteArray);

    assertEquals(PhysicalPlan.PhysicalPlanType.CREATE_TEMPLATE.ordinal(), buffer.get());

    CreateTemplatePlan deserializedPlan = new CreateTemplatePlan();
    deserializedPlan.deserialize(buffer);

    assertEquals(
        plan.getCompressors().get(0).get(0), deserializedPlan.getCompressors().get(0).get(0));
    assertEquals(plan.getMeasurements().size(), deserializedPlan.getMeasurements().size());
    assertEquals(plan.getName(), deserializedPlan.getName());
  }

  private CreateTemplatePlan getTreeTemplatePlan() {
    /*
     Construct a template like: create schema template treeTemplate ( (d1.s1 INT32 GORILLA
     SNAPPY), (s2 INT32 GORILLA SNAPPY), (GPS.x FLOAT RLE SNAPPY), (GPS.y FLOAT RLE SNAPPY), )with
     aligned (GPS)
    */
    List<List<String>> measurementList = new ArrayList<>();
    measurementList.add(Collections.singletonList("d1.s1"));
    measurementList.add(Collections.singletonList("s2"));
    measurementList.add(Arrays.asList("GPS.x", "GPS.y"));

    List<List<TSDataType>> dataTypeList = new ArrayList<>();
    dataTypeList.add(Collections.singletonList(TSDataType.INT64));
    dataTypeList.add(Collections.singletonList(TSDataType.INT64));
    dataTypeList.add(Arrays.asList(TSDataType.FLOAT, TSDataType.FLOAT));

    List<List<TSEncoding>> encodingList = new ArrayList<>();
    encodingList.add(Collections.singletonList(TSEncoding.GORILLA));
    encodingList.add(Collections.singletonList(TSEncoding.GORILLA));
    encodingList.add(Arrays.asList(TSEncoding.RLE, TSEncoding.RLE));

    List<List<CompressionType>> compressionTypes = new ArrayList<>();
    compressionTypes.add(Collections.singletonList(CompressionType.UNCOMPRESSED));
    compressionTypes.add(Collections.singletonList(CompressionType.SNAPPY));
    compressionTypes.add(Arrays.asList(CompressionType.SNAPPY, CompressionType.SNAPPY));

    return new CreateTemplatePlan(
        "treeTemplate", measurementList, dataTypeList, encodingList, compressionTypes);
  }

  private CreateTemplatePlan getCreateTemplatePlan() {
    List<List<String>> measurementList = new ArrayList<>();
    measurementList.add(Collections.singletonList("s11"));
    List<String> measurements = new ArrayList<>();
    for (int i = 0; i < 10; i++) {
      measurements.add("vector.s" + i);
    }
    measurementList.add(measurements);

    List<List<TSDataType>> dataTypeList = new ArrayList<>();
    dataTypeList.add(Collections.singletonList(TSDataType.INT64));
    List<TSDataType> dataTypes = new ArrayList<>();
    for (int i = 0; i < 10; i++) {
      dataTypes.add(TSDataType.INT64);
    }
    dataTypeList.add(dataTypes);

    List<List<TSEncoding>> encodingList = new ArrayList<>();
    encodingList.add(Collections.singletonList(TSEncoding.RLE));
    List<TSEncoding> encodings = new ArrayList<>();
    for (int i = 0; i < 10; i++) {
      encodings.add(TSEncoding.RLE);
    }
    encodingList.add(encodings);

    List<List<CompressionType>> compressionTypes = new ArrayList<>();
    List<CompressionType> compressorList = new ArrayList<>();
    for (int i = 0; i < 10; i++) {
      compressorList.add(CompressionType.SNAPPY);
    }
    compressionTypes.add(Collections.singletonList(CompressionType.SNAPPY));
    compressionTypes.add(compressorList);

    List<String> schemaNames = new ArrayList<>();
    schemaNames.add("s21");
    schemaNames.add("vector");

    return new CreateTemplatePlan(
        "template1", schemaNames, measurementList, dataTypeList, encodingList, compressionTypes);
  }

  private CreateTemplatePlan getDirectAlignedTemplate() {
    List<List<String>> measurementList = new ArrayList<>();
    List<String> measurements = new ArrayList<>();
    for (int i = 0; i < 10; i++) {
      measurements.add("vs" + i);
    }
    measurementList.add(measurements);

    List<List<TSDataType>> dataTypeList = new ArrayList<>();
    List<TSDataType> dataTypes = new ArrayList<>();
    for (int i = 0; i < 10; i++) {
      dataTypes.add(TSDataType.INT64);
    }
    dataTypeList.add(dataTypes);

    List<List<TSEncoding>> encodingList = new ArrayList<>();
    List<TSEncoding> encodings = new ArrayList<>();
    for (int i = 0; i < 10; i++) {
      encodings.add(TSEncoding.RLE);
    }
    encodingList.add(encodings);

    List<List<CompressionType>> compressionTypes = new ArrayList<>();
    List<CompressionType> compressorList = new ArrayList<>();
    for (int i = 0; i < 10; i++) {
      compressorList.add(CompressionType.SNAPPY);
    }
    compressionTypes.add(compressorList);

    List<String> schemaNames = new ArrayList<>();
    schemaNames.add("vector");

    return new CreateTemplatePlan(
        "templateDA", schemaNames, measurementList, dataTypeList, encodingList, compressionTypes);
  }
}
