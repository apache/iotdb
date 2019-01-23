/**
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
package org.apache.iotdb.db.engine.memcontrol;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.engine.MetadataManagerHelper;
import org.apache.iotdb.db.engine.bufferwrite.Action;
import org.apache.iotdb.db.engine.bufferwrite.FileNodeConstants;
import org.apache.iotdb.db.engine.overflow.ioV2.OverflowProcessor;
import org.apache.iotdb.db.exception.OverflowProcessorException;
import org.apache.iotdb.db.utils.EnvironmentUtils;
import org.apache.iotdb.db.utils.FileSchemaUtils;
import org.apache.iotdb.db.utils.MemUtils;
import org.apache.iotdb.tsfile.common.conf.TSFileConfig;
import org.apache.iotdb.tsfile.common.conf.TSFileDescriptor;
import org.apache.iotdb.tsfile.exception.write.WriteProcessException;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.write.record.TSRecord;
import org.apache.iotdb.tsfile.write.record.datapoint.DataPoint;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class OverflowFileSizeControlTest {

  private String nameSpacePath = "nsp";
  private Map<String, Action> parameters = null;
  private OverflowProcessor ofprocessor = null;
  private TSFileConfig tsconfig = TSFileDescriptor.getInstance().getConfig();
  private String deviceId = "root.vehicle.d0";
  private String[] measurementIds = {"s0", "s1", "s2", "s3", "s4", "s5"};
  private TSDataType[] dataTypes = {TSDataType.INT32, TSDataType.INT64, TSDataType.FLOAT,
      TSDataType.DOUBLE,
      TSDataType.BOOLEAN, TSDataType.TEXT};

  private IoTDBConfig dbConfig = IoTDBDescriptor.getInstance().getConfig();
  private long overflowFileSize;
  private int groupSize;

  private boolean skip = !false;

  private Action overflowflushaction = new Action() {

    @Override
    public void act() throws Exception {
    }
  };

  private Action filenodeflushaction = new Action() {

    @Override
    public void act() throws Exception {
    }
  };

  private Action filenodemanagerbackupaction = new Action() {

    @Override
    public void act() throws Exception {
    }
  };

  private Action filenodemanagerflushaction = new Action() {

    @Override
    public void act() throws Exception {
    }
  };

  @Before
  public void setUp() throws Exception {
    parameters = new HashMap<>();
    parameters.put(FileNodeConstants.OVERFLOW_FLUSH_ACTION, overflowflushaction);
    parameters.put(FileNodeConstants.FILENODE_PROCESSOR_FLUSH_ACTION, filenodeflushaction);

    overflowFileSize = dbConfig.overflowFileSizeThreshold;
    groupSize = tsconfig.groupSizeInByte;
    dbConfig.overflowFileSizeThreshold = 10 * 1024 * 1024;
    tsconfig.groupSizeInByte = 1024 * 1024;

    MetadataManagerHelper.initMetadata();
  }

  @After
  public void tearDown() throws Exception {
    dbConfig.overflowFileSizeThreshold = overflowFileSize;
    tsconfig.groupSizeInByte = groupSize;
    EnvironmentUtils.cleanEnv();
  }

  @Test
  public void testInsert() throws InterruptedException, IOException, WriteProcessException {
    if (skip) {
      return;
    }
    // insert one point: int
    try {
      ofprocessor = new OverflowProcessor(nameSpacePath, parameters,
          FileSchemaUtils.constructFileSchema(deviceId));
      for (int i = 1; i < 1000000; i++) {
        TSRecord record = new TSRecord(i, deviceId);
        record.addTuple(DataPoint.getDataPoint(dataTypes[0], measurementIds[0], String.valueOf(i)));
        if (i % 100000 == 0) {
          System.out.println(i + "," + MemUtils.bytesCntToStr(ofprocessor.getFileSize()));
        }
      }
      // wait to flush
      Thread.sleep(1000);
      ofprocessor.close();
      assertTrue(ofprocessor.getFileSize() < dbConfig.overflowFileSizeThreshold);
      fail("Method unimplemented");
    } catch (OverflowProcessorException e) {
      e.printStackTrace();
      fail(e.getMessage());
    }

  }
}
