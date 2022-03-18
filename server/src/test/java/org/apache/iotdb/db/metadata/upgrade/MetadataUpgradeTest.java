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
package org.apache.iotdb.db.metadata.upgrade;

import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.exception.metadata.IllegalPathException;
import org.apache.iotdb.db.metadata.MetadataConstant;
import org.apache.iotdb.db.metadata.SchemaEngine;
import org.apache.iotdb.db.metadata.logfile.MLogWriter;
import org.apache.iotdb.db.metadata.mnode.EntityMNode;
import org.apache.iotdb.db.metadata.mnode.IEntityMNode;
import org.apache.iotdb.db.metadata.mnode.IMNode;
import org.apache.iotdb.db.metadata.mnode.IMeasurementMNode;
import org.apache.iotdb.db.metadata.mnode.IStorageGroupMNode;
import org.apache.iotdb.db.metadata.mnode.InternalMNode;
import org.apache.iotdb.db.metadata.mnode.MeasurementMNode;
import org.apache.iotdb.db.metadata.mnode.StorageGroupMNode;
import org.apache.iotdb.db.metadata.path.PartialPath;
import org.apache.iotdb.db.metadata.tag.TagLogFile;
import org.apache.iotdb.db.qp.physical.sys.AutoCreateDeviceMNodePlan;
import org.apache.iotdb.db.qp.physical.sys.CreateAlignedTimeSeriesPlan;
import org.apache.iotdb.db.qp.physical.sys.CreateTemplatePlan;
import org.apache.iotdb.db.qp.physical.sys.CreateTimeSeriesPlan;
import org.apache.iotdb.db.qp.physical.sys.SetTemplatePlan;
import org.apache.iotdb.db.qp.physical.sys.ShowTimeSeriesPlan;
import org.apache.iotdb.db.qp.physical.sys.UnsetTemplatePlan;
import org.apache.iotdb.db.query.dataset.ShowTimeSeriesResult;
import org.apache.iotdb.db.service.IoTDB;
import org.apache.iotdb.db.utils.EnvironmentUtils;
import org.apache.iotdb.db.writelog.io.LogWriter;
import org.apache.iotdb.tsfile.file.metadata.enums.CompressionType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.apache.iotdb.tsfile.write.schema.IMeasurementSchema;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.apache.iotdb.commons.conf.IoTDBConstant.PATH_ROOT;

public class MetadataUpgradeTest {

  private IoTDBConfig config = IoTDBDescriptor.getInstance().getConfig();
  private String schemaDirPath = config.getSchemaDir();

  @Before
  public void setUp() {
    EnvironmentUtils.envSetUp();
  }

  @After
  public void tearDown() throws Exception {
    EnvironmentUtils.cleanEnv();
  }

  @Test
  public void testMetadataUpgrade() throws Exception {
    prepareSnapshot();
    prepareTagFile();
    prepareMLog();

    SchemaEngine schemaEngine = IoTDB.schemaEngine;
    schemaEngine.clear();
    MetadataUpgrader.upgrade();
    schemaEngine.init();

    Assert.assertEquals(8, schemaEngine.getStorageGroupNum(new PartialPath("root.**"), false));
    Assert.assertEquals(10, schemaEngine.getAllTimeseriesCount(new PartialPath("root.**")));

    ShowTimeSeriesPlan showTimeSeriesPlan =
        new ShowTimeSeriesPlan(
            new PartialPath("root.**"), false, "t-k-0", "t-k-0-v-0", 0, 0, false);
    List<ShowTimeSeriesResult> resultList = schemaEngine.showTimeseries(showTimeSeriesPlan, null);
    Assert.assertEquals(1, resultList.size());
    ShowTimeSeriesResult result = resultList.get(0);
    Assert.assertEquals("root.test.sg1.d1.s1", result.getName());

    showTimeSeriesPlan =
        new ShowTimeSeriesPlan(
            new PartialPath("root.**"), false, "t-k-1", "t-k-1-v-1", 0, 0, false);
    resultList = schemaEngine.showTimeseries(showTimeSeriesPlan, null);
    resultList =
        resultList.stream()
            .sorted(Comparator.comparing(ShowTimeSeriesResult::getName))
            .collect(Collectors.toList());
    Assert.assertEquals(2, resultList.size());
    result = resultList.get(0);
    Assert.assertEquals("root.test.sg1.d2.s1", result.getName());
    result = resultList.get(1);
    Assert.assertEquals("root.test.sg2.d1.s1", result.getName());

    Assert.assertEquals(4, schemaEngine.getPathsSetTemplate("template").size());
    Assert.assertEquals(0, schemaEngine.getPathsSetTemplate("unsetTemplate").size());

    Assert.assertEquals(
        "root.test.sg3.d3", new ArrayList<>(schemaEngine.getPathsUsingTemplate("template")).get(0));
  }

  private void prepareMLog() throws Exception {
    try (MLogWriter logWriter =
        new MLogWriter(schemaDirPath + File.separator + MetadataConstant.METADATA_LOG)) {

      CreateTimeSeriesPlan createTimeSeriesPlan =
          new CreateTimeSeriesPlan(
              new PartialPath("root.test.sg1.d2.s1"),
              TSDataType.INT32,
              TSEncoding.PLAIN,
              CompressionType.GZIP,
              null,
              null,
              null,
              null);
      createTimeSeriesPlan.setTagOffset(-1);
      logWriter.createTimeseries(createTimeSeriesPlan);

      createTimeSeriesPlan =
          new CreateTimeSeriesPlan(
              new PartialPath("root.test.sg1.d2.s2"),
              TSDataType.INT32,
              TSEncoding.PLAIN,
              CompressionType.GZIP,
              null,
              null,
              null,
              null);
      logWriter.createTimeseries(createTimeSeriesPlan);

      logWriter.setStorageGroup(new PartialPath("root.test.sg2"));

      Map<String, String> tags = new HashMap<>();
      tags.put("t-k-1", "t-k-1-v-1");
      createTimeSeriesPlan =
          new CreateTimeSeriesPlan(
              new PartialPath("root.test.sg2.d1.s1"),
              TSDataType.INT32,
              TSEncoding.PLAIN,
              CompressionType.GZIP,
              null,
              tags,
              null,
              null);
      createTimeSeriesPlan.setTagOffset(config.getTagAttributeTotalSize());
      logWriter.createTimeseries(createTimeSeriesPlan);

      logWriter.changeOffset(
          new PartialPath("root.test.sg1.d2.s1"), 2L * config.getTagAttributeTotalSize());

      CreateTemplatePlan createTemplatePlan = getCreateTemplatePlan("template", "s3");
      logWriter.createSchemaTemplate(createTemplatePlan);
      SetTemplatePlan setTemplatePlan = new SetTemplatePlan("template", "root.test");
      logWriter.setSchemaTemplate(setTemplatePlan);

      logWriter.setStorageGroup(new PartialPath("root.test.sg3"));
      logWriter.autoCreateDeviceMNode(
          new AutoCreateDeviceMNodePlan(new PartialPath("root.test.sg3.d3")));
      logWriter.setUsingSchemaTemplate(new PartialPath("root.test.sg3.d3"));

      logWriter.setStorageGroup(new PartialPath("root.unsetTemplate1.sg1"));
      logWriter.createSchemaTemplate(getCreateTemplatePlan("unsetTemplate", "s1"));
      logWriter.setSchemaTemplate(new SetTemplatePlan("unsetTemplate", "root.unsetTemplate1"));
      logWriter.setStorageGroup(new PartialPath("root.unsetTemplate1.sg2"));
      logWriter.unsetSchemaTemplate(new UnsetTemplatePlan("root.unsetTemplate1", "unsetTemplate"));

      logWriter.setStorageGroup(new PartialPath("root.unsetTemplate2.sg1"));
      logWriter.setSchemaTemplate(new SetTemplatePlan("unsetTemplate", "root.unsetTemplate2"));
      logWriter.unsetSchemaTemplate(new UnsetTemplatePlan("root.unsetTemplate2", "unsetTemplate"));

      logWriter.setStorageGroup(new PartialPath("root.test.sg4"));
      logWriter.setStorageGroup(new PartialPath("root.unsetTemplate2.sg2"));

      logWriter.force();
    }

    CreateAlignedTimeSeriesPlan createAlignedTimeSeriesPlan = getCreateAlignedTimeseriesPlan();
    LogWriter rawLogWriter =
        new LogWriter(schemaDirPath + File.separator + MetadataConstant.METADATA_LOG, true);
    try {
      ByteBuffer byteBuffer = ByteBuffer.allocate(1024 * 1024);
      createAlignedTimeSeriesPlan.formerSerialize(byteBuffer);
      rawLogWriter.write(byteBuffer);
      rawLogWriter.force();
    } finally {
      rawLogWriter.close();
    }
  }

  private CreateTemplatePlan getCreateTemplatePlan(String templateName, String measurementName) {
    List<List<String>> measurementList = new ArrayList<>();
    measurementList.add(Collections.singletonList(measurementName));

    List<List<TSDataType>> dataTypeList = new ArrayList<>();
    dataTypeList.add(Collections.singletonList(TSDataType.INT64));

    List<List<TSEncoding>> encodingList = new ArrayList<>();
    encodingList.add(Collections.singletonList(TSEncoding.RLE));

    List<List<CompressionType>> compressionTypes = new ArrayList<>();
    compressionTypes.add(Collections.singletonList(CompressionType.SNAPPY));

    List<String> schemaNames = new ArrayList<>();
    schemaNames.add(measurementName);

    return new CreateTemplatePlan(
        templateName, schemaNames, measurementList, dataTypeList, encodingList, compressionTypes);
  }

  private CreateAlignedTimeSeriesPlan getCreateAlignedTimeseriesPlan() throws IllegalPathException {
    PartialPath devicePath = new PartialPath("root.unsetTemplate1.sg1.device");
    List<String> measurements = Arrays.asList("s1", "s2", "s3", "s4", "s5");
    List<TSDataType> tsDataTypes =
        Arrays.asList(
            TSDataType.DOUBLE,
            TSDataType.TEXT,
            TSDataType.FLOAT,
            TSDataType.BOOLEAN,
            TSDataType.INT32);
    List<TSEncoding> tsEncodings =
        Arrays.asList(
            TSEncoding.PLAIN,
            TSEncoding.PLAIN,
            TSEncoding.PLAIN,
            TSEncoding.PLAIN,
            TSEncoding.PLAIN);
    List<CompressionType> compressionTypes =
        Arrays.asList(
            CompressionType.UNCOMPRESSED,
            CompressionType.UNCOMPRESSED,
            CompressionType.UNCOMPRESSED,
            CompressionType.UNCOMPRESSED,
            CompressionType.UNCOMPRESSED);
    return new CreateAlignedTimeSeriesPlan(
        devicePath, measurements, tsDataTypes, tsEncodings, compressionTypes, null, null, null);
  }

  private void prepareTagFile() throws Exception {
    try (TagLogFile tagLogFile = new TagLogFile(schemaDirPath, MetadataConstant.TAG_LOG)) {
      Map<String, String> tags = new HashMap<>();
      Map<String, String> attributes = new HashMap<>();

      tags.put("t-k-0", "t-k-0-v-0");
      tagLogFile.write(tags, attributes);

      tags.clear();
      tags.put("t-k-1", "t-k-1-v-1");
      tagLogFile.write(tags, attributes);

      tags.clear();
      tags.put("t-k-1", "t-k-1-v-1");
      tagLogFile.write(tags, attributes);
    }
  }

  private void prepareSnapshot() throws Exception {
    IMNode root = new InternalMNode(null, PATH_ROOT);
    IMNode test = new InternalMNode(root, "test");
    root.addChild(test);
    IStorageGroupMNode storageGroupMNode = new StorageGroupMNode(test, "sg1", 10000);
    test.addChild(storageGroupMNode);
    IEntityMNode entityMNode = new EntityMNode(storageGroupMNode, "d1");
    storageGroupMNode.addChild(entityMNode);
    IMeasurementSchema schema =
        new MeasurementSchema("s1", TSDataType.INT32, TSEncoding.PLAIN, CompressionType.GZIP, null);
    IMeasurementMNode measurementMNode =
        MeasurementMNode.getMeasurementMNode(entityMNode, "s1", schema, "first");
    measurementMNode.setOffset(0);
    entityMNode.addChild(measurementMNode);
    entityMNode.addAlias("first", measurementMNode);
    try (MLogWriter mLogWriter =
        new MLogWriter(schemaDirPath + File.separator + MetadataConstant.MTREE_SNAPSHOT)) {
      root.serializeTo(mLogWriter);
    }
  }
}
