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
package org.apache.iotdb.db.metadata.tagSchemaRegion;

import org.apache.iotdb.commons.consensus.SchemaRegionId;
import org.apache.iotdb.commons.path.MeasurementPath;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.commons.utils.FileUtils;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.metadata.plan.schemaregion.impl.write.SchemaRegionWritePlanFactory;
import org.apache.iotdb.db.metadata.plan.schemaregion.write.ICreateAlignedTimeSeriesPlan;
import org.apache.iotdb.db.metadata.plan.schemaregion.write.ICreateTimeSeriesPlan;
import org.apache.iotdb.tsfile.common.conf.TSFileDescriptor;
import org.apache.iotdb.tsfile.file.metadata.enums.CompressionType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class TagSchemaRegionTest {

  private CompressionType compressionType;

  private String storageGroupDirPath;

  private String schemaRegionDirPath;

  private String storageGroupFullPath = "root/testTagSchemaRegion";

  private String storageGroup = "root.testTagSchemaRegion";

  private boolean isEnableIDTable = false;

  private String originalDeviceIDTransformationMethod = null;

  private boolean isEnableIDTableLogFile = false;

  private String schemaDir;

  private TagSchemaRegion tagSchemaRegion;

  @Before
  public void before() {
    compressionType = TSFileDescriptor.getInstance().getConfig().getCompressor();
    schemaDir = IoTDBDescriptor.getInstance().getConfig().getSchemaDir();
    isEnableIDTable = IoTDBDescriptor.getInstance().getConfig().isEnableIDTable();
    originalDeviceIDTransformationMethod =
        IoTDBDescriptor.getInstance().getConfig().getDeviceIDTransformationMethod();
    isEnableIDTableLogFile = IoTDBDescriptor.getInstance().getConfig().isEnableIDTableLogFile();
    IoTDBDescriptor.getInstance().getConfig().setEnableIDTable(true);
    IoTDBDescriptor.getInstance().getConfig().setDeviceIDTransformationMethod("SHA256");
    IoTDBDescriptor.getInstance().getConfig().setEnableIDTableLogFile(true);
    storageGroupDirPath = schemaDir + File.separator + storageGroupFullPath;
    schemaRegionDirPath = storageGroupDirPath + File.separator + 0;
  }

  @After
  public void clean() {
    tagSchemaRegion.clear();
    IoTDBDescriptor.getInstance().getConfig().setEnableIDTable(isEnableIDTable);
    IoTDBDescriptor.getInstance()
        .getConfig()
        .setDeviceIDTransformationMethod(originalDeviceIDTransformationMethod);
    IoTDBDescriptor.getInstance().getConfig().setEnableIDTableLogFile(isEnableIDTableLogFile);
    FileUtils.deleteDirectoryAndEmptyParent(new File(schemaDir));
  }

  @Test
  public void testCreateTimeseries() {
    try {
      tagSchemaRegion =
          new TagSchemaRegion(new PartialPath(storageGroup), new SchemaRegionId(0), null, null);
      createTimeseries();
    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  @Test
  public void testCreateAlignedTimeSeries() {
    try {
      tagSchemaRegion =
          new TagSchemaRegion(new PartialPath(storageGroup), new SchemaRegionId(0), null, null);
      createAlignedTimeseries();
    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  @Test
  public void testRecover() {
    try {
      tagSchemaRegion =
          new TagSchemaRegion(new PartialPath(storageGroup), new SchemaRegionId(0), null, null);
      createAlignedTimeseries();
      createTimeseries();

      tagSchemaRegion.clear();
      tagSchemaRegion =
          new TagSchemaRegion(new PartialPath(storageGroup), new SchemaRegionId(0), null, null);

      getMeasurementPathsTest();
    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  @Test
  public void testGetMeasurementPaths() {
    try {
      tagSchemaRegion =
          new TagSchemaRegion(new PartialPath(storageGroup), new SchemaRegionId(0), null, null);
      createAlignedTimeseries();
      createTimeseries();
      getMeasurementPathsTest();
    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  private void getMeasurementPathsTest() throws Exception {
    PartialPath pathPattern = new PartialPath(storageGroup + ".tag1.a.tag2.b.s0");
    List<MeasurementPath> measurementPaths =
        tagSchemaRegion.getMeasurementPaths(pathPattern, false, false);
    assertEquals(measurementPaths.size(), 1);
    assertEquals(measurementPaths.get(0).getFullPath(), storageGroup + ".tag1.a.tag2.b.s0");

    pathPattern = new PartialPath(storageGroup + ".tag1.a.**.s");
    measurementPaths = tagSchemaRegion.getMeasurementPaths(pathPattern, false, false);
    assertEquals(measurementPaths.size(), 5);

    pathPattern = new PartialPath(storageGroup + ".tag2.b.**.s");
    measurementPaths = tagSchemaRegion.getMeasurementPaths(pathPattern, false, false);
    assertEquals(measurementPaths.size(), 1);

    pathPattern = new PartialPath(storageGroup + ".tag3.b.**.s");
    measurementPaths = tagSchemaRegion.getMeasurementPaths(pathPattern, false, false);
    assertEquals(measurementPaths.size(), 3);

    pathPattern = new PartialPath(storageGroup + ".tag2.y.tag1.x.**.s");
    measurementPaths = tagSchemaRegion.getMeasurementPaths(pathPattern, false, false);
    assertEquals(measurementPaths.size(), 3);
  }

  private void createTimeseries() throws Exception {
    ICreateTimeSeriesPlan createTimeSeriesPlan =
        SchemaRegionWritePlanFactory.getCreateTimeSeriesPlan(
            new PartialPath(storageGroup + ".tag1.a.tag2.b.s0"),
            TSDataType.valueOf("INT32"),
            TSEncoding.valueOf("RLE"),
            compressionType,
            Collections.emptyMap(),
            null,
            null,
            null);
    tagSchemaRegion.createTimeseries(createTimeSeriesPlan, 0);
    createTimeSeriesPlan =
        SchemaRegionWritePlanFactory.getCreateTimeSeriesPlan(
            new PartialPath(storageGroup + ".tag1.a.tag2.c.s0"),
            TSDataType.valueOf("INT32"),
            TSEncoding.valueOf("RLE"),
            compressionType,
            Collections.emptyMap(),
            null,
            null,
            null);
    tagSchemaRegion.createTimeseries(createTimeSeriesPlan, 0);
  }

  private void createAlignedTimeseries() throws Exception {

    ICreateAlignedTimeSeriesPlan plan =
        SchemaRegionWritePlanFactory.getCreateAlignedTimeSeriesPlan(
            new PartialPath(storageGroup + ".tag1.a.tag3.b"),
            Arrays.asList("s1", "s2", "s3"),
            Arrays.asList(
                TSDataType.valueOf("FLOAT"),
                TSDataType.valueOf("INT64"),
                TSDataType.valueOf("INT32")),
            Arrays.asList(
                TSEncoding.valueOf("RLE"), TSEncoding.valueOf("RLE"), TSEncoding.valueOf("RLE")),
            Arrays.asList(compressionType, compressionType, compressionType),
            null,
            null,
            null);
    tagSchemaRegion.createAlignedTimeSeries(plan);
    plan =
        SchemaRegionWritePlanFactory.getCreateAlignedTimeSeriesPlan(
            new PartialPath(storageGroup + ".tag1.x.tag2.y"),
            Arrays.asList("s1", "s2", "s3"),
            Arrays.asList(
                TSDataType.valueOf("FLOAT"),
                TSDataType.valueOf("INT64"),
                TSDataType.valueOf("INT32")),
            Arrays.asList(
                TSEncoding.valueOf("RLE"), TSEncoding.valueOf("RLE"), TSEncoding.valueOf("RLE")),
            Arrays.asList(compressionType, compressionType, compressionType),
            null,
            null,
            null);
    tagSchemaRegion.createAlignedTimeSeries(plan);
  }
}
