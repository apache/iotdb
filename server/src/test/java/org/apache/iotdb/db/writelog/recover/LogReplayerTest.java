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

import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.engine.fileSystem.SystemFileFactory;
import org.apache.iotdb.db.engine.memtable.IMemTable;
import org.apache.iotdb.db.engine.memtable.PrimitiveMemTable;
import org.apache.iotdb.db.engine.modification.Deletion;
import org.apache.iotdb.db.engine.modification.Modification;
import org.apache.iotdb.db.engine.modification.ModificationFile;
import org.apache.iotdb.db.engine.querycontext.ReadOnlyMemChunk;
import org.apache.iotdb.db.engine.storagegroup.TsFileResource;
import org.apache.iotdb.db.exception.StorageEngineException;
import org.apache.iotdb.db.exception.StorageGroupProcessorException;
import org.apache.iotdb.db.exception.metadata.IllegalPathException;
import org.apache.iotdb.db.exception.metadata.MetadataException;
import org.apache.iotdb.db.exception.query.QueryProcessException;
import org.apache.iotdb.db.metadata.mnode.IMeasurementMNode;
import org.apache.iotdb.db.metadata.mnode.MeasurementMNode;
import org.apache.iotdb.db.metadata.path.AlignedPath;
import org.apache.iotdb.db.metadata.path.MeasurementPath;
import org.apache.iotdb.db.metadata.path.PartialPath;
import org.apache.iotdb.db.qp.physical.crud.DeletePlan;
import org.apache.iotdb.db.qp.physical.crud.InsertRowPlan;
import org.apache.iotdb.db.qp.physical.crud.InsertTabletPlan;
import org.apache.iotdb.db.service.IoTDB;
import org.apache.iotdb.db.utils.EnvironmentUtils;
import org.apache.iotdb.db.utils.MmapUtil;
import org.apache.iotdb.db.writelog.manager.MultiFileLogNodeManager;
import org.apache.iotdb.db.writelog.node.WriteLogNode;
import org.apache.iotdb.tsfile.common.conf.TSFileDescriptor;
import org.apache.iotdb.tsfile.file.metadata.enums.CompressionType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.apache.iotdb.tsfile.read.TimeValuePair;
import org.apache.iotdb.tsfile.read.reader.IPointReader;
import org.apache.iotdb.tsfile.utils.Binary;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class LogReplayerTest {
  IoTDBConfig config = IoTDBDescriptor.getInstance().getConfig();
  boolean prevIsAutoCreateSchemaEnabled;
  boolean prevIsEnablePartialInsert;

  @Before
  public void before() {
    // set recover config, avoid creating deleted time series when recovering wal
    prevIsAutoCreateSchemaEnabled = config.isAutoCreateSchemaEnabled();
    prevIsEnablePartialInsert = config.isEnablePartialInsert();
    ;
    config.setAutoCreateSchemaEnabled(false);
    config.setEnablePartialInsert(true);
    EnvironmentUtils.envSetUp();
  }

  @After
  public void after() throws IOException, StorageEngineException {
    // reset config
    config.setAutoCreateSchemaEnabled(prevIsAutoCreateSchemaEnabled);
    config.setEnablePartialInsert(prevIsEnablePartialInsert);
    EnvironmentUtils.cleanEnv();
  }

  @Test
  public void test()
      throws IOException, StorageGroupProcessorException, QueryProcessException, MetadataException {
    String logNodePrefix = "testLogNode";
    File tsFile = SystemFileFactory.INSTANCE.getFile("temp", "1-1-1.tsfile");
    File modF = SystemFileFactory.INSTANCE.getFile("test.mod");
    ModificationFile modFile = new ModificationFile(modF.getPath());
    TsFileResource tsFileResource = new TsFileResource(tsFile);
    IMemTable memTable = new PrimitiveMemTable();
    CompressionType compressionType = TSFileDescriptor.getInstance().getConfig().getCompressor();

    IoTDB.metaManager.setStorageGroup(new PartialPath("root.sg"));
    try {
      // 1. set schema
      for (int i = 0; i <= 5; i++) {
        for (int j = 0; j <= 5; j++) {
          IoTDB.metaManager.createTimeseries(
              new PartialPath("root.sg.device" + i + ".sensor" + j),
              TSDataType.INT64,
              TSEncoding.PLAIN,
              TSFileDescriptor.getInstance().getConfig().getCompressor(),
              Collections.emptyMap());
        }
      }
      IoTDB.metaManager.createAlignedTimeSeries(
          new PartialPath("root.sg.device6"),
          Arrays.asList("s1", "s2", "s3", "s4", "s5"),
          Arrays.asList(
              TSDataType.INT32,
              TSDataType.INT64,
              TSDataType.BOOLEAN,
              TSDataType.FLOAT,
              TSDataType.TEXT),
          Arrays.asList(
              TSEncoding.RLE, TSEncoding.RLE, TSEncoding.RLE, TSEncoding.RLE, TSEncoding.PLAIN),
          Arrays.asList(
              compressionType, compressionType, compressionType, compressionType, compressionType));

      // 2. delete some timeseries
      IoTDB.metaManager.deleteTimeseries(new PartialPath("root.sg.device0.sensor2"));
      IoTDB.metaManager.deleteTimeseries(new PartialPath("root.sg.device0.sensor4"));
      IoTDB.metaManager.deleteTimeseries(new PartialPath("root.sg.device6.s1"));
      IoTDB.metaManager.deleteTimeseries(new PartialPath("root.sg.device6.s5"));

      LogReplayer replayer =
          new LogReplayer(
              logNodePrefix, tsFile.getPath(), modFile, tsFileResource, memTable, false);

      WriteLogNode node =
          MultiFileLogNodeManager.getInstance()
              .getNode(
                  logNodePrefix + tsFile.getName(),
                  () -> {
                    ByteBuffer[] byteBuffers = new ByteBuffer[2];
                    byteBuffers[0] =
                        ByteBuffer.allocateDirect(
                            IoTDBDescriptor.getInstance().getConfig().getWalBufferSize() / 2);
                    byteBuffers[1] =
                        ByteBuffer.allocateDirect(
                            IoTDBDescriptor.getInstance().getConfig().getWalBufferSize() / 2);
                    return byteBuffers;
                  });
      node.write(
          new InsertRowPlan(
              new PartialPath("root.sg.device0"),
              50,
              "sensor4",
              TSDataType.INT64,
              String.valueOf(0)));
      node.write(
          new InsertRowPlan(
              new PartialPath("root.sg.device0"),
              100,
              "sensor0",
              TSDataType.INT64,
              String.valueOf(0)));
      node.write(
          new InsertRowPlan(
              new PartialPath("root.sg.device0"),
              2,
              "sensor1",
              TSDataType.INT64,
              String.valueOf(0)));
      for (int i = 1; i < 5; i++) {
        node.write(
            new InsertRowPlan(
                new PartialPath("root.sg.device" + i),
                i,
                "sensor" + i,
                TSDataType.INT64,
                String.valueOf(i)));
      }
      node.write(insertTabletPlan());
      node.write(insertAlignedTabletPlan());
      DeletePlan deletePlan = new DeletePlan(0, 200, new PartialPath("root.sg.device0.sensor0"));
      node.write(deletePlan);
      node.close();

      replayer.replayLogs(
          () -> {
            ByteBuffer[] byteBuffers = new ByteBuffer[2];
            byteBuffers[0] =
                ByteBuffer.allocateDirect(
                    IoTDBDescriptor.getInstance().getConfig().getWalBufferSize() / 2);
            byteBuffers[1] =
                ByteBuffer.allocateDirect(
                    IoTDBDescriptor.getInstance().getConfig().getWalBufferSize() / 2);
            return byteBuffers;
          },
          null);

      for (int i = 0; i < 5; i++) {
        MeasurementPath fullPath =
            new MeasurementPath(
                "root.sg.device" + i,
                "sensor" + i,
                new MeasurementSchema(
                    "sensor" + i,
                    TSDataType.INT64,
                    TSEncoding.RLE,
                    CompressionType.UNCOMPRESSED,
                    Collections.emptyMap()));
        ReadOnlyMemChunk memChunk = memTable.query(fullPath, Long.MIN_VALUE, null);
        IPointReader iterator = memChunk.getPointReader();
        if (i != 0) {
          assertTrue(iterator.hasNextTimeValuePair());
          TimeValuePair timeValuePair = iterator.nextTimeValuePair();
          assertEquals(i, timeValuePair.getTimestamp());
          assertEquals(i, timeValuePair.getValue().getLong());
        }
        assertFalse(iterator.hasNextTimeValuePair());
      }
      AlignedPath alignedfullPath =
          new AlignedPath(
              "root.sg.device6",
              Arrays.asList("s1", "s2", "s3", "s4", "s5"),
              Arrays.asList(
                  new MeasurementSchema("s1", TSDataType.INT32, TSEncoding.RLE),
                  new MeasurementSchema("s2", TSDataType.INT64, TSEncoding.RLE),
                  new MeasurementSchema("s3", TSDataType.BOOLEAN, TSEncoding.RLE),
                  new MeasurementSchema("s4", TSDataType.FLOAT, TSEncoding.RLE),
                  new MeasurementSchema("s5", TSDataType.TEXT, TSEncoding.PLAIN)));
      ReadOnlyMemChunk memChunk = memTable.query(alignedfullPath, Long.MIN_VALUE, null);
      IPointReader iterator = memChunk.getPointReader();
      int time = 0;
      while (iterator.hasNextTimeValuePair()) {
        TimeValuePair timeValuePair = iterator.nextTimeValuePair();
        assertEquals(time, timeValuePair.getTimestamp());
        assertEquals(null, timeValuePair.getValue().getVector()[0]);
        assertEquals(null, timeValuePair.getValue().getVector()[1]);
        assertEquals(true, timeValuePair.getValue().getVector()[2].getBoolean());
        assertEquals(time, timeValuePair.getValue().getVector()[3].getFloat(), 0.00001);
        assertEquals(null, timeValuePair.getValue().getVector()[4]);
        time++;
      }
      assertEquals(100, time);

      Modification[] mods = modFile.getModifications().toArray(new Modification[0]);
      assertEquals(1, mods.length);
      assertEquals("root.sg.device0.sensor0", mods[0].getPathString());
      assertEquals(0, mods[0].getFileOffset());
      assertEquals(200, ((Deletion) mods[0]).getEndTime());

      assertEquals(2, tsFileResource.getStartTime("root.sg.device0"));
      assertEquals(2, tsFileResource.getEndTime("root.sg.device0"));

      assertEquals(0, tsFileResource.getStartTime("root.sg.device6"));
      assertEquals(99, tsFileResource.getEndTime("root.sg.device6"));
      for (int i = 1; i < 5; i++) {
        assertEquals(i, tsFileResource.getStartTime("root.sg.device" + i));
        assertEquals(i, tsFileResource.getEndTime("root.sg.device" + i));
      }

      // test insert tablet
      for (int i = 0; i < 2; i++) {
        MeasurementPath fullPath =
            new MeasurementPath(
                "root.sg.device5",
                "sensor" + i,
                new MeasurementSchema(
                    "sensor" + i,
                    TSDataType.INT64,
                    TSEncoding.PLAIN,
                    CompressionType.UNCOMPRESSED,
                    Collections.emptyMap()));
        memChunk = memTable.query(fullPath, Long.MIN_VALUE, null);
        // s0 has datatype boolean, but required INT64, will return null
        if (i == 0) {
          assertNull(memChunk);
        } else {
          iterator = memChunk.getPointReader();
          iterator.hasNextTimeValuePair();
          for (time = 0; time < 100; time++) {
            TimeValuePair timeValuePair = iterator.nextTimeValuePair();
            assertEquals(time, timeValuePair.getTimestamp());
            assertEquals(time, timeValuePair.getValue().getLong());
          }
        }
      }
    } finally {
      modFile.close();
      MultiFileLogNodeManager.getInstance()
          .deleteNode(
              logNodePrefix + tsFile.getName(),
              (ByteBuffer[] byteBuffers) -> {
                for (ByteBuffer byteBuffer : byteBuffers) {
                  MmapUtil.clean((MappedByteBuffer) byteBuffer);
                }
              });
      modF.delete();
      tsFile.delete();
      tsFile.getParentFile().delete();
    }
  }

  /**
   * insert tablet plan, time series expected datatype is INT64 s0 is set to boolean, it will output
   * null value s1 is set to INT64, it will output its value
   *
   * @return
   * @throws IllegalPathException
   */
  private InsertTabletPlan insertTabletPlan() throws IllegalPathException {
    String[] measurements = new String[4];
    measurements[0] = "sensor0"; // mismatch type
    measurements[1] = "sensor1";
    measurements[2] = "sensor2"; // have been deleted
    measurements[3] = "sensor3";

    List<Integer> dataTypes = new ArrayList<>();
    dataTypes.add(TSDataType.BOOLEAN.ordinal());
    dataTypes.add(TSDataType.INT64.ordinal());
    dataTypes.add(TSDataType.INT64.ordinal());
    dataTypes.add(TSDataType.INT64.ordinal());

    String deviceId = "root.sg.device5";

    IMeasurementMNode[] mNodes = new IMeasurementMNode[4];
    mNodes[0] = MeasurementMNode.getMeasurementMNode(null, "sensor0", null, null);
    mNodes[1] = MeasurementMNode.getMeasurementMNode(null, "sensor1", null, null);
    mNodes[2] = MeasurementMNode.getMeasurementMNode(null, "sensor2", null, null);
    mNodes[3] = MeasurementMNode.getMeasurementMNode(null, "sensor3", null, null);

    InsertTabletPlan insertTabletPlan =
        new InsertTabletPlan(new PartialPath(deviceId), measurements, dataTypes);

    long[] times = new long[100];
    Object[] columns = new Object[4];
    columns[0] = new boolean[100];
    columns[1] = new long[100];
    columns[2] = new long[100];
    columns[3] = new long[100];

    for (long r = 0; r < 100; r++) {
      times[(int) r] = r;
      ((boolean[]) columns[0])[(int) r] = false;
      ((long[]) columns[1])[(int) r] = r;
      ((long[]) columns[2])[(int) r] = r;
      ((long[]) columns[3])[(int) r] = r;
    }
    insertTabletPlan.setTimes(times);
    insertTabletPlan.setColumns(columns);
    insertTabletPlan.setRowCount(times.length);
    insertTabletPlan.setMeasurementMNodes(mNodes);
    insertTabletPlan.setStart(0);
    insertTabletPlan.setEnd(100);

    return insertTabletPlan;
  }

  /**
   * insert tablet plan, time series expected datatype is INT64 s0 is set to boolean, it will output
   * null value s1 is set to INT64, it will output its value
   *
   * @return
   * @throws IllegalPathException
   */
  private InsertTabletPlan insertAlignedTabletPlan() throws IllegalPathException {
    String deviceId = "root.sg.device6";

    List<Integer> dataTypes =
        Arrays.asList(
            TSDataType.INT32.ordinal(), // deleted
            TSDataType.BOOLEAN.ordinal(), // mismatch type
            TSDataType.BOOLEAN.ordinal(),
            TSDataType.FLOAT.ordinal(),
            TSDataType.TEXT.ordinal()); // deleted

    InsertTabletPlan insertTabletPlan =
        new InsertTabletPlan(
            new PartialPath(deviceId),
            new String[] {"s1", "s2", "s3", "s4", "s5"},
            dataTypes,
            true);

    long[] times = new long[100];
    Object[] columns =
        new Object[] {
          new int[100], new boolean[100], new boolean[100], new float[100], new Binary[100],
        };
    for (long r = 0; r < 100; r++) {
      times[(int) r] = r;
      ((int[]) columns[0])[(int) r] = (int) r;
      ((boolean[]) columns[1])[(int) r] = true;
      ((boolean[]) columns[2])[(int) r] = true;
      ((float[]) columns[3])[(int) r] = r;
      ((Binary[]) columns[4])[(int) r] = Binary.valueOf(r + "");
    }

    //    BitMap[] bitMaps = new BitMap[dataTypes.size()];
    //    for (int i = 0; i < dataTypes.size(); i++) {
    //      if (bitMaps[i] == null) {
    //        bitMaps[i] = new BitMap(times.length);
    //      }
    //      // mark value of time=99 as null
    //      bitMaps[i].mark(99);
    //    }

    insertTabletPlan.setTimes(times);
    insertTabletPlan.setColumns(columns);
    insertTabletPlan.setRowCount(times.length);
    //    insertTabletPlan.setBitMaps(bitMaps);

    return insertTabletPlan;
  }
}
