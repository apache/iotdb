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
package org.apache.iotdb.db.metadata.tagSchemaRegion.idtable;

import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.commons.utils.FileUtils;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.exception.StorageEngineException;
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
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class IDTableWithDeviceIDListImplTest {

  private CompressionType compressionType;

  private String storageGroupDirPath;

  private String schemaRegionDirPath;

  private String storageGroupFullPath = "root/testIDTableWithDeviceIDListImpl";

  private String storageGroup = "root.testIDTableWithDeviceIDListImpl";

  private boolean isEnableIDTable = false;

  private String originalDeviceIDTransformationMethod = null;

  private boolean isEnableIDTableLogFile = false;

  private String schemaDir;

  private IDTableWithDeviceIDListImpl idTableWithDeviceIDList;

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
    idTableWithDeviceIDList = new IDTableWithDeviceIDListImpl(new File(schemaRegionDirPath));
  }

  @After
  public void clean() throws IOException, StorageEngineException {
    idTableWithDeviceIDList.clear();
    IoTDBDescriptor.getInstance().getConfig().setEnableIDTable(isEnableIDTable);
    IoTDBDescriptor.getInstance()
        .getConfig()
        .setDeviceIDTransformationMethod(originalDeviceIDTransformationMethod);
    IoTDBDescriptor.getInstance().getConfig().setEnableIDTableLogFile(isEnableIDTableLogFile);
    FileUtils.deleteDirectoryAndEmptyParent(new File(schemaDir));
  }

  @Test
  public void testCreateAlignedTimeseries() {
    try {
      createAlignedTimeseries();
      assertEquals(idTableWithDeviceIDList.size(), 2);
      assertEquals(
          idTableWithDeviceIDList.getDeviceEntry(storageGroup + ".d1.aligned_device").getDeviceID(),
          idTableWithDeviceIDList.get(0));
      assertEquals(
          idTableWithDeviceIDList.getDeviceEntry(storageGroup + ".d2.aligned_device").getDeviceID(),
          idTableWithDeviceIDList.get(1));
    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  @Test
  public void testCreateTimeseries() {
    try {
      createTimeseries();
      assertEquals(idTableWithDeviceIDList.size(), 2);
      assertEquals(
          idTableWithDeviceIDList.getDeviceEntry(storageGroup + ".d1").getDeviceID(),
          idTableWithDeviceIDList.get(0));
      assertEquals(
          idTableWithDeviceIDList.getDeviceEntry(storageGroup + ".d2").getDeviceID(),
          idTableWithDeviceIDList.get(1));
    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  @Test
  public void testRecover() {
    try {
      createTimeseries();
      createAlignedTimeseries();

      idTableWithDeviceIDList.clear();
      idTableWithDeviceIDList = new IDTableWithDeviceIDListImpl(new File(schemaRegionDirPath));

      assertEquals(idTableWithDeviceIDList.size(), 4);
      assertEquals(
          idTableWithDeviceIDList.getDeviceEntry(storageGroup + ".d1").getDeviceID(),
          idTableWithDeviceIDList.get(0));
      assertEquals(
          idTableWithDeviceIDList.getDeviceEntry(storageGroup + ".d2").getDeviceID(),
          idTableWithDeviceIDList.get(1));
      assertEquals(
          idTableWithDeviceIDList.getDeviceEntry(storageGroup + ".d1.aligned_device").getDeviceID(),
          idTableWithDeviceIDList.get(2));
      assertEquals(
          idTableWithDeviceIDList.getDeviceEntry(storageGroup + ".d2.aligned_device").getDeviceID(),
          idTableWithDeviceIDList.get(3));
    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  private void createAlignedTimeseries() throws Exception {
    ICreateAlignedTimeSeriesPlan plan =
        SchemaRegionWritePlanFactory.getCreateAlignedTimeSeriesPlan(
            new PartialPath(storageGroup + ".d1.aligned_device"),
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
    idTableWithDeviceIDList.createAlignedTimeseries(plan);
    plan =
        SchemaRegionWritePlanFactory.getCreateAlignedTimeSeriesPlan(
            new PartialPath(storageGroup + ".d2.aligned_device"),
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
    idTableWithDeviceIDList.createAlignedTimeseries(plan);
  }

  private void createTimeseries() throws Exception {
    ICreateTimeSeriesPlan createTimeSeriesPlan =
        SchemaRegionWritePlanFactory.getCreateTimeSeriesPlan(
            new PartialPath(storageGroup + ".d1.s0"),
            TSDataType.valueOf("INT32"),
            TSEncoding.valueOf("RLE"),
            compressionType,
            Collections.emptyMap(),
            null,
            null,
            null);
    idTableWithDeviceIDList.createTimeseries(createTimeSeriesPlan);
    createTimeSeriesPlan =
        SchemaRegionWritePlanFactory.getCreateTimeSeriesPlan(
            new PartialPath(storageGroup + ".d2.s0"),
            TSDataType.valueOf("INT32"),
            TSEncoding.valueOf("RLE"),
            compressionType,
            Collections.emptyMap(),
            null,
            null,
            null);
    idTableWithDeviceIDList.createTimeseries(createTimeSeriesPlan);
  }
}
