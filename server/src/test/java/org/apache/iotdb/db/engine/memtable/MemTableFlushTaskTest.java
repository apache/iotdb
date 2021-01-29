/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.iotdb.db.engine.memtable;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.concurrent.ExecutionException;
import org.apache.iotdb.db.constant.TestConstant;
import org.apache.iotdb.db.engine.MetadataManagerHelper;
import org.apache.iotdb.db.engine.flush.MemTableFlushTask;
import org.apache.iotdb.db.utils.EnvironmentUtils;
import org.apache.iotdb.tsfile.file.metadata.ChunkMetadata;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.apache.iotdb.tsfile.fileSystem.FSFactoryProducer;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;
import org.apache.iotdb.tsfile.write.writer.RestorableTsFileIOWriter;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class MemTableFlushTaskTest {

  private RestorableTsFileIOWriter writer;
  private String storageGroup = "storage_group1";
  private String filePath = TestConstant.OUTPUT_DATA_DIR
      .concat("testUnsealedTsFileProcessor.tsfile");
  private IMemTable memTable;
  private long startTime = 1;
  private long endTime = 100;

  @Before
  public void setUp() throws Exception {
    EnvironmentUtils.envSetUp();
    MetadataManagerHelper.initMetadata();
    writer = new RestorableTsFileIOWriter(FSFactoryProducer.getFSFactory().getFile(filePath));
    memTable = new PrimitiveMemTable();
  }

  @After
  public void tearDown() throws Exception {
    writer.close();
    EnvironmentUtils.cleanEnv();
    EnvironmentUtils.cleanDir(TestConstant.OUTPUT_DATA_DIR);
  }

  @Test
  public void testFlushMemTable() throws ExecutionException, InterruptedException, IOException {
    MemTableTestUtils.produceData(memTable, startTime, endTime, MemTableTestUtils.deviceId0,
        MemTableTestUtils.measurementId0,
        MemTableTestUtils.dataType0);
    MemTableFlushTask memTableFlushTask = new MemTableFlushTask(memTable, writer, storageGroup);
    assertTrue(writer
        .getVisibleMetadataList(MemTableTestUtils.deviceId0, MemTableTestUtils.measurementId0,
            MemTableTestUtils.dataType0).isEmpty());
    memTableFlushTask.syncFlushMemTable();
    writer.makeMetadataVisible();
    assertEquals(1, writer
        .getVisibleMetadataList(MemTableTestUtils.deviceId0, MemTableTestUtils.measurementId0,
            MemTableTestUtils.dataType0).size());
    ChunkMetadata chunkMetaData = writer
        .getVisibleMetadataList(MemTableTestUtils.deviceId0, MemTableTestUtils.measurementId0,
            MemTableTestUtils.dataType0).get(0);
    assertEquals(MemTableTestUtils.measurementId0, chunkMetaData.getMeasurementUid());
    assertEquals(startTime, chunkMetaData.getStartTime());
    assertEquals(endTime, chunkMetaData.getEndTime());
    assertEquals(MemTableTestUtils.dataType0, chunkMetaData.getDataType());
    assertEquals(endTime - startTime + 1, chunkMetaData.getNumOfPoints());
  }

  @Test
  public void testRegularDecodingAndDataNoConsistentANdFlushMemTable() throws ExecutionException, InterruptedException, IOException {
    memTable.write(MemTableTestUtils.deviceId0, MemTableTestUtils.measurementId0, new MeasurementSchema(MemTableTestUtils.measurementId0,
            MemTableTestUtils.dataType0, TSEncoding.PLAIN), 1, 1200);
    memTable.write(MemTableTestUtils.deviceId0, MemTableTestUtils.measurementId0, new MeasurementSchema(MemTableTestUtils.measurementId0,
            MemTableTestUtils.dataType0, TSEncoding.PLAIN), 2, 1100);
    memTable.write(MemTableTestUtils.deviceId0, MemTableTestUtils.measurementId0, new MeasurementSchema(MemTableTestUtils.measurementId0,
            MemTableTestUtils.dataType0, TSEncoding.PLAIN), 3, 1000);
    memTable.write(MemTableTestUtils.deviceId0, MemTableTestUtils.measurementId0, new MeasurementSchema(MemTableTestUtils.measurementId0,
            MemTableTestUtils.dataType0, TSEncoding.PLAIN), 4, 2200);
    MemTableFlushTask memTableFlushTask = new MemTableFlushTask(memTable, writer, storageGroup);
    assertTrue(writer
            .getVisibleMetadataList(MemTableTestUtils.deviceId0, MemTableTestUtils.measurementId0,
                    MemTableTestUtils.dataType0).isEmpty());
    memTableFlushTask.syncFlushMemTable();
    writer.makeMetadataVisible();
    assertEquals(0, writer
            .getVisibleMetadataList(MemTableTestUtils.deviceId0, MemTableTestUtils.measurementId0,
                    MemTableTestUtils.dataType0).size());
  }

  @Test
  public void testRegularDecodingAndDataConsistentANdFlushMemTable() throws ExecutionException, InterruptedException, IOException {
    memTable.write(MemTableTestUtils.deviceId0, MemTableTestUtils.measurementId0, new MeasurementSchema(MemTableTestUtils.measurementId0,
            MemTableTestUtils.dataType0, TSEncoding.PLAIN), 1, 1100);
    memTable.write(MemTableTestUtils.deviceId0, MemTableTestUtils.measurementId0, new MeasurementSchema(MemTableTestUtils.measurementId0,
            MemTableTestUtils.dataType0, TSEncoding.PLAIN), 2, 1200);
    memTable.write(MemTableTestUtils.deviceId0, MemTableTestUtils.measurementId0, new MeasurementSchema(MemTableTestUtils.measurementId0,
            MemTableTestUtils.dataType0, TSEncoding.PLAIN), 3, 1300);
    memTable.write(MemTableTestUtils.deviceId0, MemTableTestUtils.measurementId0, new MeasurementSchema(MemTableTestUtils.measurementId0,
            MemTableTestUtils.dataType0, TSEncoding.PLAIN), 4, 1400);
    MemTableFlushTask memTableFlushTask = new MemTableFlushTask(memTable, writer, storageGroup);
    assertTrue(writer
            .getVisibleMetadataList(MemTableTestUtils.deviceId0, MemTableTestUtils.measurementId0,
                    MemTableTestUtils.dataType0).isEmpty());
    memTableFlushTask.syncFlushMemTable();
    writer.makeMetadataVisible();
    assertEquals(1, writer
            .getVisibleMetadataList(MemTableTestUtils.deviceId0, MemTableTestUtils.measurementId0,
                    MemTableTestUtils.dataType0).size());
  }

}