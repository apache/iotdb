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

package org.apache.iotdb.db.queryengine.execution.fragment;

import org.apache.iotdb.commons.concurrent.IoTDBThreadPoolFactory;
import org.apache.iotdb.commons.exception.IllegalPathException;
import org.apache.iotdb.commons.exception.MetadataException;
import org.apache.iotdb.commons.path.AlignedPath;
import org.apache.iotdb.commons.path.MeasurementPath;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.exception.query.QueryProcessException;
import org.apache.iotdb.db.queryengine.common.FragmentInstanceId;
import org.apache.iotdb.db.queryengine.common.PlanFragmentId;
import org.apache.iotdb.db.queryengine.exception.CpuNotEnoughException;
import org.apache.iotdb.db.queryengine.exception.MemoryNotEnoughException;
import org.apache.iotdb.db.queryengine.execution.driver.IDriver;
import org.apache.iotdb.db.queryengine.execution.exchange.MPPDataExchangeManager;
import org.apache.iotdb.db.queryengine.execution.exchange.sink.ISink;
import org.apache.iotdb.db.queryengine.execution.schedule.IDriverScheduler;
import org.apache.iotdb.db.storageengine.dataregion.DataRegion;
import org.apache.iotdb.db.storageengine.dataregion.memtable.DeviceIDFactory;
import org.apache.iotdb.db.storageengine.dataregion.memtable.IMemTable;
import org.apache.iotdb.db.storageengine.dataregion.memtable.IWritableMemChunk;
import org.apache.iotdb.db.storageengine.dataregion.memtable.IWritableMemChunkGroup;
import org.apache.iotdb.db.storageengine.dataregion.memtable.PrimitiveMemTable;
import org.apache.iotdb.db.storageengine.dataregion.memtable.ReadOnlyMemChunk;
import org.apache.iotdb.db.utils.datastructure.AlignedTVList;
import org.apache.iotdb.db.utils.datastructure.TVList;

import com.google.common.collect.ImmutableMap;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.file.metadata.enums.CompressionType;
import org.apache.tsfile.file.metadata.enums.TSEncoding;
import org.apache.tsfile.read.reader.IPointReader;
import org.apache.tsfile.write.schema.IMeasurementSchema;
import org.apache.tsfile.write.schema.MeasurementSchema;
import org.junit.Test;
import org.mockito.Mockito;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;

import static org.apache.iotdb.db.queryengine.common.QueryId.MOCK_QUERY_ID;
import static org.apache.iotdb.db.queryengine.execution.fragment.FragmentInstanceContext.createFragmentInstanceContext;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class FragmentInstanceExecutionTest {

  @Test
  public void testFragmentInstanceExecution() {
    ExecutorService instanceNotificationExecutor =
        IoTDBThreadPoolFactory.newFixedThreadPool(1, "test-instance-notification");
    try {
      FragmentInstanceExecution execution =
          createFragmentInstanceExecution(0, instanceNotificationExecutor);
      FragmentInstanceContext fragmentInstanceContext = execution.getFragmentInstanceContext();
      FragmentInstanceStateMachine stateMachine = execution.getStateMachine();

      assertEquals(FragmentInstanceState.RUNNING, execution.getInstanceState());
      FragmentInstanceInfo instanceInfo = execution.getInstanceInfo();
      assertEquals(FragmentInstanceState.RUNNING, instanceInfo.getState());
      assertEquals(fragmentInstanceContext.getEndTime(), instanceInfo.getEndTime());
      assertEquals(fragmentInstanceContext.getFailedCause(), instanceInfo.getMessage());
      assertEquals(fragmentInstanceContext.getFailureInfoList(), instanceInfo.getFailureInfoList());

      assertEquals(fragmentInstanceContext.getStartTime(), execution.getStartTime());
      assertEquals(-1, execution.getTimeoutInMs());
      assertEquals(stateMachine, execution.getStateMachine());

      fragmentInstanceContext.decrementNumOfUnClosedDriver();

      stateMachine.failed(new RuntimeException("Unknown"));

      assertTrue(execution.getInstanceState().isFailed());

      List<FragmentInstanceFailureInfo> failureInfoList =
          execution.getInstanceInfo().getFailureInfoList();

      assertEquals(1, failureInfoList.size());
      assertEquals("Unknown", failureInfoList.get(0).getMessage());
      assertEquals("Unknown", failureInfoList.get(0).toException().getMessage());

    } catch (CpuNotEnoughException | MemoryNotEnoughException e) {
      e.printStackTrace();
      fail(e.getMessage());
    } finally {
      instanceNotificationExecutor.shutdown();
    }
  }

  @Test
  public void testTVListOwnerTransfer() throws InterruptedException {
    // Capture System.err to check for warning messages
    PrintStream systemOut = System.out;
    ByteArrayOutputStream logPrint = new ByteArrayOutputStream();
    System.setOut(new PrintStream(logPrint));

    try {
      IoTDBDescriptor.getInstance().getConfig().setDataNodeId(1);

      ExecutorService instanceNotificationExecutor =
          IoTDBThreadPoolFactory.newFixedThreadPool(1, "test-instance-notification");
      try {
        // TVList
        TVList tvList = buildTVList();

        // FragmentInstance Context & Execution
        FragmentInstanceExecution execution1 =
            createFragmentInstanceExecution(1, instanceNotificationExecutor);
        FragmentInstanceContext fragmentInstanceContext1 = execution1.getFragmentInstanceContext();
        fragmentInstanceContext1.addTVListToSet(ImmutableMap.of(tvList, 0));
        tvList.getQueryContextSet().add(fragmentInstanceContext1);

        FragmentInstanceExecution execution2 =
            createFragmentInstanceExecution(2, instanceNotificationExecutor);
        FragmentInstanceContext fragmentInstanceContext2 = execution2.getFragmentInstanceContext();
        fragmentInstanceContext2.addTVListToSet(ImmutableMap.of(tvList, 0));
        tvList.getQueryContextSet().add(fragmentInstanceContext2);

        // mock flush's behavior
        fragmentInstanceContext1
            .getMemoryReservationContext()
            .reserveMemoryCumulatively(tvList.calculateRamSize().getRamSize());
        tvList.setOwnerQuery(fragmentInstanceContext1);

        fragmentInstanceContext1.decrementNumOfUnClosedDriver();
        fragmentInstanceContext2.decrementNumOfUnClosedDriver();

        fragmentInstanceContext1.getStateMachine().finished();
        Thread.sleep(100);
        fragmentInstanceContext2.getStateMachine().finished();

        assertTrue(execution1.getInstanceState().isDone());
        assertTrue(execution2.getInstanceState().isDone());
        Thread.sleep(100);
      } catch (CpuNotEnoughException | MemoryNotEnoughException | IllegalArgumentException e) {
        fail(e.getMessage());
      } finally {
        instanceNotificationExecutor.shutdown();
      }
    } finally {
      // Restore original System.out
      System.setOut(systemOut);

      // should not contain warn message: "The memory cost to be released is larger than the memory
      // cost of memory block"
      String capturedOutput = logPrint.toString();
      assertTrue(capturedOutput.isEmpty());
    }
  }

  @Test
  public void testTVListCloneForQuery() {
    IoTDBDescriptor.getInstance().getConfig().setDataNodeId(1);

    ExecutorService instanceNotificationExecutor =
        IoTDBThreadPoolFactory.newFixedThreadPool(1, "test-instance-notification");

    try {
      String deviceId = "d1";
      String measurementId = "s1";
      IMemTable memTable = createMemTable(deviceId, measurementId);
      assertEquals(1, memTable.getMemTableMap().size());
      IWritableMemChunkGroup memChunkGroup = memTable.getMemTableMap().values().iterator().next();
      assertEquals(1, memChunkGroup.getMemChunkMap().size());
      IWritableMemChunk memChunk = memChunkGroup.getMemChunkMap().values().iterator().next();
      TVList tvList = memChunk.getWorkingTVList();
      assertFalse(tvList.isSorted());

      // FragmentInstance Context
      FragmentInstanceId id1 = new FragmentInstanceId(new PlanFragmentId(MOCK_QUERY_ID, 1), "1");
      FragmentInstanceStateMachine stateMachine1 =
          new FragmentInstanceStateMachine(id1, instanceNotificationExecutor);
      FragmentInstanceContext fragmentInstanceContext1 =
          createFragmentInstanceContext(id1, stateMachine1);

      FragmentInstanceId id2 = new FragmentInstanceId(new PlanFragmentId(MOCK_QUERY_ID, 2), "2");
      FragmentInstanceStateMachine stateMachine2 =
          new FragmentInstanceStateMachine(id2, instanceNotificationExecutor);
      FragmentInstanceContext fragmentInstanceContext2 =
          createFragmentInstanceContext(id2, stateMachine2);

      // query on memtable
      MeasurementPath fullPath =
          new MeasurementPath(
              deviceId,
              measurementId,
              new MeasurementSchema(
                  measurementId,
                  TSDataType.INT32,
                  TSEncoding.RLE,
                  CompressionType.UNCOMPRESSED,
                  Collections.emptyMap()));
      ReadOnlyMemChunk readOnlyMemChunk1 =
          memTable.query(fragmentInstanceContext1, fullPath, Long.MIN_VALUE, null, null);
      ReadOnlyMemChunk readOnlyMemChunk2 =
          memTable.query(fragmentInstanceContext2, fullPath, Long.MIN_VALUE, null, null);

      IPointReader pointReader = readOnlyMemChunk1.getPointReader();
      while (pointReader.hasNextTimeValuePair()) {
        pointReader.nextTimeValuePair();
      }
      assertTrue(tvList.isSorted());
      assertEquals(tvList.calculateRamSize().getRamSize(), tvList.getReservedMemoryBytes());
    } catch (QueryProcessException
        | IOException
        | MetadataException
        | MemoryNotEnoughException
        | IllegalArgumentException e) {
      fail(e.getMessage());
    } finally {
      instanceNotificationExecutor.shutdown();
    }
  }

  @Test
  public void testAlignedTVListPartialColumnClone() {
    IoTDBDescriptor.getInstance().getConfig().setDataNodeId(1);
    ExecutorService instanceNotificationExecutor =
        IoTDBThreadPoolFactory.newFixedThreadPool(2, "test-aligned-partial-clone");

    try {
      // Create MemTable with AlignedPath
      List<IMeasurementSchema> schemaList = new ArrayList<>();
      for (int i = 0; i < 5; i++) {
        schemaList.add(new MeasurementSchema("sensor_" + i, TSDataType.INT64));
      }
      String deviceId = "d1";
      IMemTable memTable = createMemTable(deviceId, schemaList);

      // Verify we have unsorted AlignedTVList
      assertEquals(1, memTable.getMemTableMap().size());
      IWritableMemChunkGroup memChunkGroup = memTable.getMemTableMap().values().iterator().next();
      assertEquals(1, memChunkGroup.getMemChunkMap().size());
      IWritableMemChunk memChunk = memChunkGroup.getMemChunkMap().values().iterator().next();
      TVList tvList = memChunk.getWorkingTVList();
      assertFalse(tvList.isSorted());
      assertEquals(6424, tvList.calculateRamSize());
      assertEquals(100, tvList.rowCount());

      // FragmentInstance Context
      FragmentInstanceId id1 = new FragmentInstanceId(new PlanFragmentId(MOCK_QUERY_ID, 1), "1");
      FragmentInstanceStateMachine stateMachine1 =
          new FragmentInstanceStateMachine(id1, instanceNotificationExecutor);
      FragmentInstanceContext context1 = createFragmentInstanceContext(id1, stateMachine1);

      FragmentInstanceId id2 = new FragmentInstanceId(new PlanFragmentId(MOCK_QUERY_ID, 2), "2");
      FragmentInstanceStateMachine stateMachine2 =
          new FragmentInstanceStateMachine(id2, instanceNotificationExecutor);
      FragmentInstanceContext context2 = createFragmentInstanceContext(id2, stateMachine2);

      // Query 1: sensor_2 and sensor_0
      List<String> measurements1 = Arrays.asList("sensor_2", "sensor_0");
      List<IMeasurementSchema> schemas1 = Arrays.asList(schemaList.get(2), schemaList.get(0));
      AlignedPath fullPath1 = new AlignedPath(deviceId, measurements1, schemas1);

      ReadOnlyMemChunk readOnlyMemChunk1 =
          memTable.query(context1, fullPath1, Long.MIN_VALUE, null, null);
      Set<Integer> accessedColumnsForQuery1 = context1.getAccessedAlignedColumns(tvList);
      assertEquals(new HashSet<>(Arrays.asList(0, 2)), accessedColumnsForQuery1);

      // Query 2: sensor_1 and sensor_3
      List<String> measurements2 = Arrays.asList("sensor_1", "sensor_3");
      List<IMeasurementSchema> schemas2 = Arrays.asList(schemaList.get(1), schemaList.get(3));
      AlignedPath fullPath2 = new AlignedPath(deviceId, measurements2, schemas2);
      ReadOnlyMemChunk readOnlyMemChunk2 =
          memTable.query(context2, fullPath2, Long.MIN_VALUE, null, null);

      // Only cloned sensor_2 and sensor_0 exist
      assertEquals(3352, tvList.calculateRamSize());
      assertEquals(100, tvList.rowCount());

    } catch (Exception e) {
      fail(e.getMessage());
    } finally {
      instanceNotificationExecutor.shutdown();
    }
  }

  private FragmentInstanceExecution createFragmentInstanceExecution(int id, Executor executor)
      throws CpuNotEnoughException {
    IDriverScheduler scheduler = Mockito.mock(IDriverScheduler.class);
    FragmentInstanceId instanceId =
        new FragmentInstanceId(new PlanFragmentId(MOCK_QUERY_ID, id), String.valueOf(id));
    FragmentInstanceStateMachine stateMachine =
        new FragmentInstanceStateMachine(instanceId, executor);
    DataRegion dataRegion = Mockito.mock(DataRegion.class);
    FragmentInstanceContext fragmentInstanceContext =
        createFragmentInstanceContext(instanceId, stateMachine);
    fragmentInstanceContext.initializeNumOfDrivers(1);
    fragmentInstanceContext.setMayHaveTmpFile(true);
    fragmentInstanceContext.setDataRegion(dataRegion);
    List<IDriver> drivers = Collections.emptyList();
    ISink sinkHandle = Mockito.mock(ISink.class);
    long timeOut = -1;
    MPPDataExchangeManager exchangeManager = Mockito.mock(MPPDataExchangeManager.class);
    return FragmentInstanceExecution.createFragmentInstanceExecution(
        scheduler,
        instanceId,
        fragmentInstanceContext,
        drivers,
        sinkHandle,
        stateMachine,
        timeOut,
        false,
        exchangeManager);
  }

  private TVList buildTVList() {
    int columns = 200;
    int rows = 1000;
    List<TSDataType> dataTypes = new ArrayList<>();
    Object[] values = new Object[columns];
    for (int i = 0; i < columns; i++) {
      dataTypes.add(TSDataType.INT64);
      values[i] = 1L;
    }
    AlignedTVList tvList = AlignedTVList.newAlignedList(dataTypes);
    for (long t = 1; t < rows; t++) {
      tvList.putAlignedValue(t, values);
    }
    return tvList;
  }

  private IMemTable createMemTable(String deviceId, String measurementId)
      throws IllegalPathException {
    IMemTable memTable = new PrimitiveMemTable("root.test", "1");

    int rows = 100;
    for (int i = 0; i < 100; i++) {
      memTable.write(
          DeviceIDFactory.getInstance().getDeviceID(new PartialPath(deviceId)),
          Collections.singletonList(
              new MeasurementSchema(measurementId, TSDataType.INT32, TSEncoding.PLAIN)),
          rows - i - 1,
          new Object[] {i + 10});
    }
    return memTable;
  }

  private IMemTable createMemTable(String deviceId, List<IMeasurementSchema> schemaList)
      throws IllegalPathException {
    PrimitiveMemTable memTable = new PrimitiveMemTable("root.test", "1");

    // Insert data in reverse order to make it unsorted
    int rows = 100;
    for (int i = rows - 1; i >= 0; i--) {
      Object[] values = new Object[5];
      for (int j = 0; j < 5; j++) {
        values[j] = (long) i * 100 + j;
      }
      memTable.writeAlignedRow(
          DeviceIDFactory.getInstance().getDeviceID(new PartialPath(deviceId)),
          schemaList,
          i,
          values);
    }
    return memTable;
  }
}
