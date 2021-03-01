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

package org.apache.iotdb.db.writelog.recover;

import org.apache.iotdb.db.constant.TestConstant;
import org.apache.iotdb.db.engine.fileSystem.SystemFileFactory;
import org.apache.iotdb.db.engine.storagegroup.TsFileResource;
import org.apache.iotdb.db.exception.StorageEngineException;
import org.apache.iotdb.db.exception.metadata.IllegalPathException;
import org.apache.iotdb.db.exception.metadata.MetadataException;
import org.apache.iotdb.db.metadata.MManager;
import org.apache.iotdb.db.metadata.PartialPath;
import org.apache.iotdb.db.service.IoTDB;
import org.apache.iotdb.db.utils.EnvironmentUtils;
import org.apache.iotdb.tsfile.common.conf.TSFileDescriptor;
import org.apache.iotdb.tsfile.exception.write.WriteProcessException;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.apache.iotdb.tsfile.read.common.Path;
import org.apache.iotdb.tsfile.write.TsFileWriter;
import org.apache.iotdb.tsfile.write.record.TSRecord;
import org.apache.iotdb.tsfile.write.record.datapoint.DataPoint;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;
import org.apache.iotdb.tsfile.write.schema.Schema;

import org.apache.commons.io.FileUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.Collections;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertSame;

public class DeviceStringTest {

  private File tsF;
  private TsFileWriter writer;
  private String logNodePrefix = TestConstant.OUTPUT_DATA_DIR.concat("testNode/0");
  private Schema schema;
  private TsFileResource resource;
  private MManager mManager = IoTDB.metaManager;

  @Before
  public void setup() throws IOException, WriteProcessException, MetadataException {
    EnvironmentUtils.envSetUp();
    tsF = SystemFileFactory.INSTANCE.getFile(logNodePrefix, "1-1-1.tsfile");
    tsF.getParentFile().mkdirs();

    schema = new Schema();
    schema.registerTimeseries(
        new Path(("root.sg.device99"), ("sensor4")),
        new MeasurementSchema("sensor4", TSDataType.INT64, TSEncoding.PLAIN));
    mManager.createTimeseries(
        new PartialPath("root.sg.device99.sensor4"),
        TSDataType.INT64,
        TSEncoding.PLAIN,
        TSFileDescriptor.getInstance().getConfig().getCompressor(),
        Collections.emptyMap());
    schema.registerTimeseries(
        new Path(("root.sg.device99"), ("sensor2")),
        new MeasurementSchema("sensor2", TSDataType.INT64, TSEncoding.PLAIN));
    mManager.createTimeseries(
        new PartialPath("root.sg.device99.sensor2"),
        TSDataType.INT64,
        TSEncoding.PLAIN,
        TSFileDescriptor.getInstance().getConfig().getCompressor(),
        Collections.emptyMap());
    schema.registerTimeseries(
        new Path(("root.sg.device99"), ("sensor1")),
        new MeasurementSchema("sensor1", TSDataType.INT64, TSEncoding.PLAIN));
    mManager.createTimeseries(
        new PartialPath("root.sg.device99.sensor1"),
        TSDataType.INT64,
        TSEncoding.PLAIN,
        TSFileDescriptor.getInstance().getConfig().getCompressor(),
        Collections.emptyMap());
    writer = new TsFileWriter(tsF, schema);

    resource = new TsFileResource(tsF);
    TSRecord tsRecord = new TSRecord(100, "root.sg.device99");
    tsRecord.addTuple(DataPoint.getDataPoint(TSDataType.INT64, "sensor4", String.valueOf(0)));
    writer.write(tsRecord);
    tsRecord = new TSRecord(2, "root.sg.device99");
    tsRecord.addTuple(DataPoint.getDataPoint(TSDataType.INT64, "sensor1", String.valueOf(0)));
    writer.write(tsRecord);

    writer.flushAllChunkGroups();
    writer.getIOWriter().close();
    resource.updateStartTime(new String("root.sg.device99"), 2);
    resource.updateEndTime(new String("root.sg.device99"), 100);
    resource.close();
    resource.serialize();
  }

  @After
  public void tearDown() throws IOException, StorageEngineException {
    EnvironmentUtils.cleanEnv();
    FileUtils.deleteDirectory(tsF.getParentFile());
    resource.close();
  }

  @Test
  public void testDeviceString() throws IOException, IllegalPathException {
    resource = new TsFileResource(tsF);
    resource.deserialize();
    assertFalse(resource.getDevices().isEmpty());
    for (String device : resource.getDevices()) {
      assertSame(device, mManager.getDeviceId(new PartialPath(device)));
    }
  }
}
