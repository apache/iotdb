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
package org.apache.iotdb.db.metadata.mlog;

import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.exception.metadata.MetadataException;
import org.apache.iotdb.db.metadata.IMetaManager;
import org.apache.iotdb.db.metadata.MetadataConstant;
import org.apache.iotdb.db.metadata.logfile.MLogTxtWriter;
import org.apache.iotdb.db.metadata.logfile.MLogUpgrader;
import org.apache.iotdb.db.metadata.path.PartialPath;
import org.apache.iotdb.db.metadata.tag.TagLogFile;
import org.apache.iotdb.db.qp.physical.PhysicalPlan;
import org.apache.iotdb.db.qp.physical.sys.CreateTemplatePlan;
import org.apache.iotdb.db.qp.physical.sys.CreateTimeSeriesPlan;
import org.apache.iotdb.db.qp.physical.sys.ShowTimeSeriesPlan;
import org.apache.iotdb.db.query.dataset.ShowTimeSeriesResult;
import org.apache.iotdb.db.service.IoTDB;
import org.apache.iotdb.db.utils.EnvironmentUtils;
import org.apache.iotdb.tsfile.file.metadata.enums.CompressionType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;

public class MLogUpgraderTest {

  @Before
  public void setUp() {
    EnvironmentUtils.envSetUp();
  }

  @After
  public void tearDown() throws Exception {
    EnvironmentUtils.cleanEnv();
  }

  @Test
  public void testUpgrade() throws IOException, MetadataException {
    IMetaManager manager = IoTDB.metaManager;
    manager.clear();

    String schemaDir = IoTDBDescriptor.getInstance().getConfig().getSchemaDir();

    MLogTxtWriter mLogTxtWriter = new MLogTxtWriter(schemaDir, MetadataConstant.METADATA_TXT_LOG);
    mLogTxtWriter.createTimeseries(
        new CreateTimeSeriesPlan(
            new PartialPath("root.sg.d.s"),
            TSDataType.BOOLEAN,
            TSEncoding.PLAIN,
            CompressionType.UNCOMPRESSED,
            null,
            null,
            null,
            null),
        0);
    mLogTxtWriter.close();

    TagLogFile tagLogFile = new TagLogFile(schemaDir, MetadataConstant.TAG_LOG);
    Map<String, String> tags = new HashMap<>();
    tags.put("name", "IoTDB");
    Map<String, String> attributes = new HashMap<>();
    attributes.put("type", "tsDB");
    tagLogFile.write(tags, attributes, 0);
    tagLogFile.close();

    File mLog = new File(schemaDir + File.separator + MetadataConstant.METADATA_LOG);
    if (mLog.exists()) {
      mLog.delete();
    }

    MLogUpgrader.upgradeMLog();

    manager.init();
    ShowTimeSeriesPlan plan =
        new ShowTimeSeriesPlan(new PartialPath("root.**"), true, "name", "DB", 0, 0, false);
    ShowTimeSeriesResult result = manager.showTimeseries(plan, null).get(0);
    Assert.assertEquals("root.sg.d.s", result.getName());
    Assert.assertEquals(tags, result.getTag());
    Assert.assertEquals(attributes, result.getAttribute());
  }

  @Test
  public void testCreateSchemaTemplateSerializationAdaptation() throws IOException {
    CreateTemplatePlan plan = getCreateTemplatePlan();
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    DataOutputStream dos = new DataOutputStream(baos);
    plan.formerSerialize(dos);
    byte[] byteArray = baos.toByteArray();
    ByteBuffer buffer = ByteBuffer.wrap(byteArray);

    assertEquals(PhysicalPlan.PhysicalPlanType.CREATE_TEMPLATE.ordinal(), buffer.get());

    CreateTemplatePlan deserializedPlan = new CreateTemplatePlan();
    deserializedPlan.deserialize(buffer);

    assertEquals(plan.getCompressors().size(), deserializedPlan.getCompressors().size());
    assertEquals(
        plan.getMeasurements().get(0).get(0), deserializedPlan.getMeasurements().get(0).get(0));
    assertEquals(plan.getDataTypes().size(), deserializedPlan.getDataTypes().size());
  }

  @SuppressWarnings("Duplicates")
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
}
