/**
 * Copyright © 2019 Apache IoTDB(incubating) (dev@iotdb.apache.org)
 *
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
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.iotdb.db.engine.memcontrol;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.File;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.conf.directories.Directories;
import org.apache.iotdb.db.engine.MetadataManagerHelper;
import org.apache.iotdb.db.engine.PathUtils;
import org.apache.iotdb.db.engine.bufferwrite.Action;
import org.apache.iotdb.db.engine.bufferwrite.BufferWriteProcessor;
import org.apache.iotdb.db.engine.bufferwrite.FileNodeConstants;
import org.apache.iotdb.db.engine.version.SysTimeVersionController;
import org.apache.iotdb.db.exception.BufferWriteProcessorException;
import org.apache.iotdb.db.metadata.ColumnSchema;
import org.apache.iotdb.db.metadata.MManager;
import org.apache.iotdb.db.utils.EnvironmentUtils;
import org.apache.iotdb.db.utils.MemUtils;
import org.apache.iotdb.tsfile.common.conf.TSFileConfig;
import org.apache.iotdb.tsfile.common.conf.TSFileDescriptor;
import org.apache.iotdb.tsfile.common.constant.JsonFormatConstant;
import org.apache.iotdb.tsfile.exception.write.WriteProcessException;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.write.schema.FileSchema;
import org.json.JSONArray;
import org.json.JSONObject;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class BufferwriteFileSizeControlTest {

  Action bfflushaction = new Action() {

    @Override
    public void act() throws Exception {

    }
  };

  Action bfcloseaction = new Action() {

    @Override
    public void act() throws Exception {
    }
  };

  Action fnflushaction = new Action() {

    @Override
    public void act() throws Exception {

    }
  };

  BufferWriteProcessor processor = null;
  String nsp = "root.vehicle.d0";
  String nsp2 = "root.vehicle.d1";

  private boolean cachePageData = false;
  private int groupSizeInByte;
  private int pageCheckSizeThreshold;
  private int pageSizeInByte;
  private int maxStringLength;
  private long fileSizeThreshold;
  private long memMonitorInterval;
  private TSFileConfig TsFileConf = TSFileDescriptor.getInstance().getConfig();
  private IoTDBConfig dbConfig = IoTDBDescriptor.getInstance().getConfig();

  private boolean skip = !false;

  @Before
  public void setUp() throws Exception {
    // origin value
    groupSizeInByte = TsFileConf.groupSizeInByte;
    pageCheckSizeThreshold = TsFileConf.pageCheckSizeThreshold;
    pageSizeInByte = TsFileConf.pageSizeInByte;
    maxStringLength = TsFileConf.maxStringLength;
    fileSizeThreshold = dbConfig.bufferwriteFileSizeThreshold;
    memMonitorInterval = dbConfig.memMonitorInterval;
    // new value
    TsFileConf.groupSizeInByte = 200000;
    TsFileConf.pageCheckSizeThreshold = 3;
    TsFileConf.pageSizeInByte = 10000;
    TsFileConf.maxStringLength = 2;
    dbConfig.bufferwriteFileSizeThreshold = 5 * 1024 * 1024;
    BasicMemController.getInstance().setCheckInterval(600 * 1000);
    // init metadata
    MetadataManagerHelper.initMetadata();
  }

  @After
  public void tearDown() throws Exception {
    // recovery value
    TsFileConf.groupSizeInByte = groupSizeInByte;
    TsFileConf.pageCheckSizeThreshold = pageCheckSizeThreshold;
    TsFileConf.pageSizeInByte = pageSizeInByte;
    TsFileConf.maxStringLength = maxStringLength;
    dbConfig.bufferwriteFileSizeThreshold = fileSizeThreshold;
    BasicMemController.getInstance().setCheckInterval(memMonitorInterval);
    // clean environment
    EnvironmentUtils.cleanEnv();
  }

  @Test
  public void test() throws BufferWriteProcessorException, WriteProcessException {
    if (skip) {
      return;
    }
    String filename = "bufferwritetest";
    new File(filename).delete();

    Map<String, Action> parameters = new HashMap<>();
    parameters.put(FileNodeConstants.BUFFERWRITE_FLUSH_ACTION, bfflushaction);
    parameters.put(FileNodeConstants.BUFFERWRITE_CLOSE_ACTION, bfcloseaction);
    parameters.put(FileNodeConstants.FILENODE_PROCESSOR_FLUSH_ACTION, fnflushaction);

    try {
      processor = new BufferWriteProcessor(Directories.getInstance().getFolderForTest(), nsp,
          filename,
          parameters, SysTimeVersionController.INSTANCE, constructFileSchema(nsp));
    } catch (BufferWriteProcessorException e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
    File nspdir = PathUtils.getBufferWriteDir(nsp);
    assertEquals(true, nspdir.isDirectory());
    for (int i = 0; i < 1000000; i++) {
      processor.write(nsp, "s1", i * i, TSDataType.INT64, i + "");
      processor.write(nsp2, "s1", i * i, TSDataType.INT64, i + "");
      if (i % 100000 == 0) {
        System.out.println(i + "," + MemUtils.bytesCntToStr(processor.getFileSize()));
      }
    }
    // wait to flush end
    try {
      Thread.sleep(3000);
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
    processor.close();
    assertTrue(processor.getFileSize() < dbConfig.bufferwriteFileSizeThreshold);
    fail("Method unimplemented");
  }

  private FileSchema constructFileSchema(String processorName) throws WriteProcessException {

    List<ColumnSchema> columnSchemaList;
    columnSchemaList = MManager.getInstance().getSchemaForFileName(processorName);

    FileSchema fileSchema = null;
    try {
      fileSchema = getFileSchemaFromColumnSchema(columnSchemaList, processorName);
    } catch (WriteProcessException e) {
      throw e;
    }
    return fileSchema;

  }

  private FileSchema getFileSchemaFromColumnSchema(List<ColumnSchema> schemaList,
      String deviceIdType)
      throws WriteProcessException {
    JSONArray rowGroup = new JSONArray();

    for (ColumnSchema col : schemaList) {
      JSONObject measurement = new JSONObject();
      measurement.put(JsonFormatConstant.MEASUREMENT_UID, col.name);
      measurement.put(JsonFormatConstant.DATA_TYPE, col.dataType.toString());
      measurement.put(JsonFormatConstant.MEASUREMENT_ENCODING, col.encoding.toString());
      for (Entry<String, String> entry : col.getArgsMap().entrySet()) {
        if (JsonFormatConstant.ENUM_VALUES.equals(entry.getKey())) {
          String[] valueArray = entry.getValue().split(",");
          measurement.put(JsonFormatConstant.ENUM_VALUES, new JSONArray(valueArray));
        } else {
          measurement.put(entry.getKey(), entry.getValue().toString());
        }
      }
      rowGroup.put(measurement);
    }
    JSONObject jsonSchema = new JSONObject();
    jsonSchema.put(JsonFormatConstant.JSON_SCHEMA, rowGroup);
    jsonSchema.put(JsonFormatConstant.DELTA_TYPE, deviceIdType);
    return new FileSchema(jsonSchema);
  }
}
