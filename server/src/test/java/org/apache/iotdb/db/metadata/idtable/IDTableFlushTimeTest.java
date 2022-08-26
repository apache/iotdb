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

package org.apache.iotdb.db.metadata.idtable;

import org.apache.iotdb.commons.exception.IllegalPathException;
import org.apache.iotdb.commons.exception.MetadataException;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.engine.StorageEngine;
import org.apache.iotdb.db.engine.storagegroup.DataRegion;
import org.apache.iotdb.db.exception.StorageEngineException;
import org.apache.iotdb.db.exception.query.QueryProcessException;
import org.apache.iotdb.db.qp.Planner;
import org.apache.iotdb.db.qp.executor.PlanExecutor;
import org.apache.iotdb.db.qp.physical.PhysicalPlan;
import org.apache.iotdb.db.qp.physical.crud.DeletePartitionPlan;
import org.apache.iotdb.db.qp.physical.crud.InsertTabletPlan;
import org.apache.iotdb.db.utils.EnvironmentUtils;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.utils.Binary;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.junit.Assert.assertEquals;

public class IDTableFlushTimeTest {
  private PlanExecutor executor = new PlanExecutor();

  private final Planner processor = new Planner();

  private boolean isEnableIDTable = false;

  private String originalDeviceIDTransformationMethod = null;

  private boolean isEnableIDTableLogFile = false;

  public IDTableFlushTimeTest() throws QueryProcessException {}

  @Before
  public void before() {
    IoTDBDescriptor.getInstance().getConfig().setAutoCreateSchemaEnabled(true);
    isEnableIDTable = IoTDBDescriptor.getInstance().getConfig().isEnableIDTable();
    originalDeviceIDTransformationMethod =
        IoTDBDescriptor.getInstance().getConfig().getDeviceIDTransformationMethod();
    isEnableIDTableLogFile = IoTDBDescriptor.getInstance().getConfig().isEnableIDTableLogFile();

    IoTDBDescriptor.getInstance().getConfig().setEnableIDTable(true);
    IoTDBDescriptor.getInstance().getConfig().setDeviceIDTransformationMethod("AutoIncrement");
    IoTDBDescriptor.getInstance().getConfig().setEnableIDTableLogFile(true);
    EnvironmentUtils.envSetUp();
  }

  @After
  public void clean() throws IOException, StorageEngineException {
    EnvironmentUtils.cleanEnv();
    IoTDBDescriptor.getInstance().getConfig().setEnableIDTable(isEnableIDTable);
    IoTDBDescriptor.getInstance()
        .getConfig()
        .setDeviceIDTransformationMethod(originalDeviceIDTransformationMethod);
    IoTDBDescriptor.getInstance().getConfig().setEnableIDTableLogFile(isEnableIDTableLogFile);
  }

  @Test
  public void testSequenceInsert()
      throws MetadataException, QueryProcessException, StorageEngineException {
    insertData(0);
    insertData(10);
    PhysicalPlan flushPlan = processor.parseSQLToPhysicalPlan("flush");
    executor.processNonQuery(flushPlan);

    insertData(20);

    DataRegion storageGroupProcessor =
        StorageEngine.getInstance().getProcessor(new PartialPath("root.isp.d1"));
    assertEquals(2, storageGroupProcessor.getSequenceFileList().size());
    assertEquals(0, storageGroupProcessor.getUnSequenceFileList().size());
  }

  @Test
  public void testUnSequenceInsert()
      throws MetadataException, QueryProcessException, StorageEngineException {
    insertData(100);
    PhysicalPlan flushPlan = processor.parseSQLToPhysicalPlan("flush");
    executor.processNonQuery(flushPlan);

    insertData(20);

    DataRegion storageGroupProcessor =
        StorageEngine.getInstance().getProcessor(new PartialPath("root.isp.d1"));
    assertEquals(1, storageGroupProcessor.getSequenceFileList().size());
    assertEquals(1, storageGroupProcessor.getUnSequenceFileList().size());
  }

  @Test
  public void testSequenceAndUnSequenceInsert()
      throws MetadataException, QueryProcessException, StorageEngineException {
    // sequence
    insertData(100);
    PhysicalPlan flushPlan = processor.parseSQLToPhysicalPlan("flush");
    executor.processNonQuery(flushPlan);

    // sequence
    insertData(120);
    executor.processNonQuery(flushPlan);

    // unsequence
    insertData(20);
    // sequence
    insertData(130);
    executor.processNonQuery(flushPlan);

    // sequence
    insertData(150);
    // unsequence
    insertData(90);

    DataRegion storageGroupProcessor =
        StorageEngine.getInstance().getProcessor(new PartialPath("root.isp.d1"));
    assertEquals(4, storageGroupProcessor.getSequenceFileList().size());
    assertEquals(2, storageGroupProcessor.getUnSequenceFileList().size());
    assertEquals(1, storageGroupProcessor.getWorkSequenceTsFileProcessors().size());
    assertEquals(1, storageGroupProcessor.getWorkUnsequenceTsFileProcessors().size());
  }

  @Test
  public void testDeletePartition()
      throws MetadataException, QueryProcessException, StorageEngineException {
    insertData(100);
    PhysicalPlan flushPlan = processor.parseSQLToPhysicalPlan("flush");
    executor.processNonQuery(flushPlan);
    insertData(20);
    insertData(120);

    DataRegion storageGroupProcessor =
        StorageEngine.getInstance().getProcessor(new PartialPath("root.isp.d1"));

    assertEquals(
        103L, storageGroupProcessor.getLastFlushTimeManager().getFlushedTime(0L, "root.isp.d1"));
    assertEquals(
        123L, storageGroupProcessor.getLastFlushTimeManager().getLastTime(0L, "root.isp.d1"));
    assertEquals(
        103L, storageGroupProcessor.getLastFlushTimeManager().getGlobalFlushedTime("root.isp.d1"));

    // delete time partition
    Set<Long> deletedPartition = new HashSet<>();
    deletedPartition.add(0L);
    DeletePartitionPlan deletePartitionPlan =
        new DeletePartitionPlan(new PartialPath("root.isp"), deletedPartition);
    executor.processNonQuery(deletePartitionPlan);

    assertEquals(
        Long.MIN_VALUE,
        storageGroupProcessor.getLastFlushTimeManager().getFlushedTime(0L, "root.isp.d1"));
    assertEquals(
        Long.MIN_VALUE,
        storageGroupProcessor.getLastFlushTimeManager().getLastTime(0L, "root.isp.d1"));
    assertEquals(
        123L, storageGroupProcessor.getLastFlushTimeManager().getGlobalFlushedTime("root.isp.d1"));
  }

  private void insertData(long initTime) throws IllegalPathException, QueryProcessException {

    long[] times = new long[] {initTime, initTime + 1, initTime + 2, initTime + 3};
    List<Integer> dataTypes = new ArrayList<>();
    dataTypes.add(TSDataType.DOUBLE.ordinal());
    dataTypes.add(TSDataType.FLOAT.ordinal());
    dataTypes.add(TSDataType.INT64.ordinal());
    dataTypes.add(TSDataType.INT32.ordinal());
    dataTypes.add(TSDataType.BOOLEAN.ordinal());
    dataTypes.add(TSDataType.TEXT.ordinal());

    Object[] columns = new Object[6];
    columns[0] = new double[4];
    columns[1] = new float[4];
    columns[2] = new long[4];
    columns[3] = new int[4];
    columns[4] = new boolean[4];
    columns[5] = new Binary[4];

    for (int r = 0; r < 4; r++) {
      ((double[]) columns[0])[r] = 10.0 + r;
      ((float[]) columns[1])[r] = 20 + r;
      ((long[]) columns[2])[r] = 100000 + r;
      ((int[]) columns[3])[r] = 1000 + r;
      ((boolean[]) columns[4])[r] = false;
      ((Binary[]) columns[5])[r] = new Binary("mm" + r);
    }

    InsertTabletPlan tabletPlan =
        new InsertTabletPlan(
            new PartialPath("root.isp.d1"),
            new String[] {"s1", "s2", "s3", "s4", "s5", "s6"},
            dataTypes);
    tabletPlan.setTimes(times);
    tabletPlan.setColumns(columns);
    tabletPlan.setRowCount(times.length);

    PlanExecutor executor = new PlanExecutor();
    executor.insertTablet(tabletPlan);
  }
}
