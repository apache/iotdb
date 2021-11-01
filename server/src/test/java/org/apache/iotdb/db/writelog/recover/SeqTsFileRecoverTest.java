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

import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.constant.TestConstant;
import org.apache.iotdb.db.engine.fileSystem.SystemFileFactory;
import org.apache.iotdb.db.engine.modification.Deletion;
import org.apache.iotdb.db.engine.modification.ModificationFile;
import org.apache.iotdb.db.engine.storagegroup.TsFileResource;
import org.apache.iotdb.db.engine.version.VersionController;
import org.apache.iotdb.db.exception.StorageEngineException;
import org.apache.iotdb.db.exception.StorageGroupProcessorException;
import org.apache.iotdb.db.exception.metadata.MetadataException;
import org.apache.iotdb.db.metadata.PartialPath;
import org.apache.iotdb.db.qp.physical.crud.DeletePlan;
import org.apache.iotdb.db.qp.physical.crud.InsertRowPlan;
import org.apache.iotdb.db.service.IoTDB;
import org.apache.iotdb.db.utils.EnvironmentUtils;
import org.apache.iotdb.db.utils.MmapUtil;
import org.apache.iotdb.db.writelog.manager.MultiFileLogNodeManager;
import org.apache.iotdb.db.writelog.node.WriteLogNode;
import org.apache.iotdb.tsfile.common.conf.TSFileDescriptor;
import org.apache.iotdb.tsfile.exception.write.WriteProcessException;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.apache.iotdb.tsfile.read.ReadOnlyTsFile;
import org.apache.iotdb.tsfile.read.TsFileSequenceReader;
import org.apache.iotdb.tsfile.read.common.Field;
import org.apache.iotdb.tsfile.read.common.Path;
import org.apache.iotdb.tsfile.read.common.RowRecord;
import org.apache.iotdb.tsfile.read.expression.QueryExpression;
import org.apache.iotdb.tsfile.read.query.dataset.QueryDataSet;
import org.apache.iotdb.tsfile.write.TsFileWriter;
import org.apache.iotdb.tsfile.write.record.TSRecord;
import org.apache.iotdb.tsfile.write.record.datapoint.DataPoint;
import org.apache.iotdb.tsfile.write.schema.IMeasurementSchema;
import org.apache.iotdb.tsfile.write.schema.Schema;
import org.apache.iotdb.tsfile.write.schema.UnaryMeasurementSchema;
import org.apache.iotdb.tsfile.write.writer.RestorableTsFileIOWriter;

import org.apache.commons.io.FileUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

public class SeqTsFileRecoverTest {

  private File tsF;
  private TsFileWriter writer;
  private WriteLogNode node;

  private String logNodePrefix = TestConstant.getTestTsFileDir("root.recover", 0, 0);
  private TsFileResource resource;
  private ModificationFile modificationFile;
  private VersionController versionController =
      new VersionController() {
        private int i;

        @Override
        public long nextVersion() {
          return ++i;
        }

        @Override
        public long currVersion() {
          return i;
        }
      };

  @Before
  public void setup() throws IOException, WriteProcessException, MetadataException {
    EnvironmentUtils.envSetUp();
    tsF =
        SystemFileFactory.INSTANCE.getFile(
            logNodePrefix, System.currentTimeMillis() + "-1-0-0.tsfile");
    if (!tsF.getParentFile().exists()) {
      Assert.assertTrue(tsF.getParentFile().mkdirs());
    }
  }

  @After
  public void tearDown() throws IOException, StorageEngineException {
    EnvironmentUtils.cleanEnv();
    FileUtils.deleteDirectory(tsF.getParentFile());
    resource.close();
    ByteBuffer[] buffers = node.delete();
    for (ByteBuffer byteBuffer : buffers) {
      MmapUtil.clean((MappedByteBuffer) byteBuffer);
    }
  }

  private void prepareData() throws IOException, MetadataException, WriteProcessException {
    IoTDB.metaManager.setStorageGroup(new PartialPath("root.sg"));
    for (int i = 0; i < 10; i++) {
      for (int j = 0; j < 10; j++) {
        IoTDB.metaManager.createTimeseries(
            new PartialPath("root.sg.device" + i + ".sensor" + j),
            TSDataType.INT64,
            TSEncoding.PLAIN,
            TSFileDescriptor.getInstance().getConfig().getCompressor(),
            Collections.emptyMap());
      }
    }

    Schema schema = new Schema();
    Map<String, IMeasurementSchema> template = new HashMap<>();
    for (int i = 0; i < 10; i++) {
      template.put(
          "sensor" + i,
          new UnaryMeasurementSchema("sensor" + i, TSDataType.INT64, TSEncoding.PLAIN));
    }
    schema.registerSchemaTemplate("template1", template);
    for (int i = 0; i < 10; i++) {
      schema.registerDevice("root.sg.device" + i, "template1");
    }
    schema.registerDevice("root.sg.device99", "template1");
    writer = new TsFileWriter(tsF, schema);

    TSRecord tsRecord = new TSRecord(100, "root.sg.device99");
    tsRecord.addTuple(DataPoint.getDataPoint(TSDataType.INT64, "sensor4", String.valueOf(0)));
    writer.write(tsRecord);
    tsRecord = new TSRecord(2, "root.sg.device99");
    tsRecord.addTuple(DataPoint.getDataPoint(TSDataType.INT64, "sensor1", String.valueOf(0)));
    writer.write(tsRecord);

    for (int i = 0; i < 10; i++) {
      for (int j = 0; j < 10; j++) {
        tsRecord = new TSRecord(i, "root.sg.device" + j);
        for (int k = 0; k < 10; k++) {
          tsRecord.addTuple(
              DataPoint.getDataPoint(TSDataType.INT64, "sensor" + k, String.valueOf(k)));
        }
        writer.write(tsRecord);
      }
    }
    writer.flushAllChunkGroups();
    writer.getIOWriter().writePlanIndices();
    writer.getIOWriter().close();

    node =
        MultiFileLogNodeManager.getInstance()
            .getNode(
                logNodePrefix + tsF.getName(),
                () -> {
                  ByteBuffer[] buffers = new ByteBuffer[2];
                  buffers[0] =
                      ByteBuffer.allocateDirect(
                          IoTDBDescriptor.getInstance().getConfig().getWalBufferSize() / 2);
                  buffers[1] =
                      ByteBuffer.allocateDirect(
                          IoTDBDescriptor.getInstance().getConfig().getWalBufferSize() / 2);
                  return buffers;
                });
    for (int i = 10; i < 20; i++) {
      for (int j = 0; j < 10; j++) {
        String[] measurements = new String[10];
        TSDataType[] types = new TSDataType[10];
        String[] values = new String[10];
        for (int k = 0; k < 10; k++) {
          measurements[k] = "sensor" + k;
          types[k] = TSDataType.INT64;
          values[k] = String.valueOf(k);
        }
        InsertRowPlan insertPlan =
            new InsertRowPlan(
                new PartialPath("root.sg.device" + j), i, measurements, types, values);
        node.write(insertPlan);
      }
      node.notifyStartFlush();
    }

    resource = new TsFileResource(tsF);
  }

  private void prepareDataWithDeletion()
      throws IOException, MetadataException, WriteProcessException {
    IoTDB.metaManager.setStorageGroup(new PartialPath("root.sg"));
    for (int i = 0; i < 4; i++) {
      IoTDB.metaManager.createTimeseries(
          new PartialPath("root.sg.device" + i + ".sensor1"),
          TSDataType.INT64,
          TSEncoding.PLAIN,
          TSFileDescriptor.getInstance().getConfig().getCompressor(),
          Collections.emptyMap());
    }

    Schema schema = new Schema();
    Map<String, IMeasurementSchema> template = new HashMap<>();
    template.put(
        "sensor1", new UnaryMeasurementSchema("sensor1", TSDataType.INT64, TSEncoding.PLAIN));
    schema.registerSchemaTemplate("template1", template);
    for (int i = 0; i < 4; i++) {
      schema.registerDevice("root.sg.device" + i, "template1");
    }
    writer = new TsFileWriter(tsF, schema);

    TSRecord tsRecord;
    for (int i = 0; i < 500; i++) {
      tsRecord = new TSRecord(i, "root.sg.device1");
      tsRecord.addTuple(DataPoint.getDataPoint(TSDataType.INT64, "sensor1", String.valueOf(i)));
      writer.write(tsRecord);
    }

    for (int i = 500; i < 1000; i++) {
      tsRecord = new TSRecord(i, "root.sg.device2");
      tsRecord.addTuple(DataPoint.getDataPoint(TSDataType.INT64, "sensor1", String.valueOf(i)));
      writer.write(tsRecord);
    }

    for (int i = 1000; i < 1500; i++) {
      tsRecord = new TSRecord(i, "root.sg.device3");
      tsRecord.addTuple(DataPoint.getDataPoint(TSDataType.INT64, "sensor1", String.valueOf(i)));
      writer.write(tsRecord);
    }

    for (int i = 1500; i < 2000; i++) {
      tsRecord = new TSRecord(i, "root.sg.device4");
      tsRecord.addTuple(DataPoint.getDataPoint(TSDataType.INT64, "sensor1", String.valueOf(i)));
      writer.write(tsRecord);
    }

    writer.flushAllChunkGroups();

    long fileOffset = tsF.length();

    ModificationFile modificationFile =
        new ModificationFile(tsF.getAbsolutePath() + ModificationFile.FILE_SUFFIX);
    modificationFile.write(
        new Deletion(
            new PartialPath("root.sg.device1", "sensor1"), fileOffset, 300, Long.MAX_VALUE));
    modificationFile.write(
        new Deletion(
            new PartialPath("root.sg.device2", "sensor1"), fileOffset, Long.MIN_VALUE, 750));
    modificationFile.write(
        new Deletion(
            new PartialPath("root.sg.device3", "sensor1"),
            fileOffset,
            Long.MIN_VALUE,
            Long.MAX_VALUE));
    modificationFile.write(
        new Deletion(new PartialPath("root.sg.device4", "sensor1"), fileOffset, 1500, 2000));

    for (int i = 2000; i < 2500; i++) {
      tsRecord = new TSRecord(i, "root.sg.device1");
      tsRecord.addTuple(DataPoint.getDataPoint(TSDataType.INT64, "sensor1", String.valueOf(i)));
      writer.write(tsRecord);
    }

    for (int i = 2500; i < 3000; i++) {
      tsRecord = new TSRecord(i, "root.sg.device2");
      tsRecord.addTuple(DataPoint.getDataPoint(TSDataType.INT64, "sensor1", String.valueOf(i)));
      writer.write(tsRecord);
    }

    for (int i = 3000; i < 3500; i++) {
      tsRecord = new TSRecord(i, "root.sg.device3");
      tsRecord.addTuple(DataPoint.getDataPoint(TSDataType.INT64, "sensor1", String.valueOf(i)));
      writer.write(tsRecord);
    }

    for (int i = 3500; i < 4000; i++) {
      tsRecord = new TSRecord(i, "root.sg.device4");
      tsRecord.addTuple(DataPoint.getDataPoint(TSDataType.INT64, "sensor1", String.valueOf(i)));
      writer.write(tsRecord);
    }

    writer.flushAllChunkGroups();

    fileOffset = tsF.length();

    modificationFile.write(
        new Deletion(
            new PartialPath("root.sg.device1", "sensor1"), fileOffset, 2300, Long.MAX_VALUE));
    modificationFile.write(
        new Deletion(
            new PartialPath("root.sg.device2", "sensor1"), fileOffset, Long.MIN_VALUE, 2750));
    modificationFile.write(
        new Deletion(new PartialPath("root.sg.device3", "sensor1"), fileOffset, 3100, 3400));
    modificationFile.write(
        new Deletion(new PartialPath("root.sg.device4", "sensor1"), fileOffset, 3500, 4000));

    writer.getIOWriter().writePlanIndices();
    writer.getIOWriter().close();

    node =
        MultiFileLogNodeManager.getInstance()
            .getNode(
                logNodePrefix + tsF.getName(),
                () -> {
                  ByteBuffer[] buffers = new ByteBuffer[2];
                  buffers[0] =
                      ByteBuffer.allocateDirect(
                          IoTDBDescriptor.getInstance().getConfig().getWalBufferSize() / 2);
                  buffers[1] =
                      ByteBuffer.allocateDirect(
                          IoTDBDescriptor.getInstance().getConfig().getWalBufferSize() / 2);
                  return buffers;
                });

    for (int i = 4000; i < 4500; i++) {
      node.write(
          new InsertRowPlan(
              new PartialPath("root.sg.device1"),
              i,
              new String[] {"sensor1"},
              new TSDataType[] {TSDataType.INT64},
              new String[] {String.valueOf(i)}));
    }
    for (int i = 4500; i < 5000; i++) {
      node.write(
          new InsertRowPlan(
              new PartialPath("root.sg.device2"),
              i,
              new String[] {"sensor1"},
              new TSDataType[] {TSDataType.INT64},
              new String[] {String.valueOf(i)}));
    }
    for (int i = 5000; i < 5500; i++) {
      node.write(
          new InsertRowPlan(
              new PartialPath("root.sg.device3"),
              i,
              new String[] {"sensor1"},
              new TSDataType[] {TSDataType.INT64},
              new String[] {String.valueOf(i)}));
    }
    for (int i = 5500; i < 6000; i++) {
      node.write(
          new InsertRowPlan(
              new PartialPath("root.sg.device4"),
              i,
              new String[] {"sensor1"},
              new TSDataType[] {TSDataType.INT64},
              new String[] {String.valueOf(i)}));
    }

    node.write(new DeletePlan(4300, Long.MAX_VALUE, new PartialPath("root.sg.device1", "sensor1")));
    node.write(new DeletePlan(Long.MIN_VALUE, 4750, new PartialPath("root.sg.device2", "sensor1")));
    node.write(new DeletePlan(5100, 5400, new PartialPath("root.sg.device3", "sensor1")));
    node.write(new DeletePlan(5500, 6000, new PartialPath("root.sg.device4", "sensor1")));

    modificationFile.write(
        new Deletion(
            new PartialPath("root.sg.device1", "sensor1"), fileOffset, 4300, Long.MAX_VALUE));
    modificationFile.write(
        new Deletion(
            new PartialPath("root.sg.device2", "sensor1"), fileOffset, Long.MIN_VALUE, 4750));
    modificationFile.write(
        new Deletion(new PartialPath("root.sg.device3", "sensor1"), fileOffset, 5100, 5400));
    modificationFile.write(
        new Deletion(new PartialPath("root.sg.device4", "sensor1"), fileOffset, 5500, 6000));

    node.notifyStartFlush();

    modificationFile.close();
    resource = new TsFileResource(tsF);
  }

  @Test
  public void testNonLastRecovery()
      throws StorageGroupProcessorException, IOException, MetadataException, WriteProcessException {
    prepareData();
    TsFileRecoverPerformer performer =
        new TsFileRecoverPerformer(logNodePrefix, resource, false, false);
    RestorableTsFileIOWriter writer =
        performer.recover(
            true,
            () -> {
              ByteBuffer[] buffers = new ByteBuffer[2];
              buffers[0] =
                  ByteBuffer.allocateDirect(
                      IoTDBDescriptor.getInstance().getConfig().getWalBufferSize() / 2);
              buffers[1] =
                  ByteBuffer.allocateDirect(
                      IoTDBDescriptor.getInstance().getConfig().getWalBufferSize() / 2);
              return buffers;
            },
            (ByteBuffer[] array) -> {
              for (ByteBuffer byteBuffer : array) {
                MmapUtil.clean((MappedByteBuffer) byteBuffer);
              }
            });
    assertFalse(writer.canWrite());
    writer.close();

    assertEquals(2, resource.getStartTime("root.sg.device99"));
    assertEquals(100, resource.getEndTime("root.sg.device99"));
    for (int i = 0; i < 10; i++) {
      assertEquals(0, resource.getStartTime("root.sg.device" + i));
      assertEquals(19, resource.getEndTime("root.sg.device" + i));
    }

    ReadOnlyTsFile readOnlyTsFile = new ReadOnlyTsFile(new TsFileSequenceReader(tsF.getPath()));
    List<Path> pathList = new ArrayList<>();
    for (int j = 0; j < 10; j++) {
      for (int k = 0; k < 10; k++) {
        pathList.add(new Path("root.sg.device" + j, "sensor" + k));
      }
    }
    QueryExpression queryExpression = QueryExpression.create(pathList, null);
    QueryDataSet dataSet = readOnlyTsFile.query(queryExpression);
    for (int i = 0; i < 20; i++) {
      RowRecord record = dataSet.next();
      assertEquals(i, record.getTimestamp());
      List<Field> fields = record.getFields();
      assertEquals(100, fields.size());
      for (int j = 0; j < 100; j++) {
        assertEquals(j % 10, fields.get(j).getLongV());
      }
    }

    pathList = new ArrayList<>();
    pathList.add(new Path("root.sg.device99", "sensor1"));
    pathList.add(new Path("root.sg.device99", "sensor4"));
    queryExpression = QueryExpression.create(pathList, null);
    dataSet = readOnlyTsFile.query(queryExpression);
    Assert.assertTrue(dataSet.hasNext());
    RowRecord record = dataSet.next();
    Assert.assertEquals("2\t0\tnull", record.toString());
    Assert.assertTrue(dataSet.hasNext());
    record = dataSet.next();
    Assert.assertEquals("100\tnull\t0", record.toString());

    readOnlyTsFile.close();
  }

  @Test
  public void testLastRecovery()
      throws StorageGroupProcessorException, IOException, MetadataException, WriteProcessException {
    prepareData();
    TsFileRecoverPerformer performer =
        new TsFileRecoverPerformer(logNodePrefix, resource, false, true);
    RestorableTsFileIOWriter writer =
        performer.recover(
            true,
            () -> {
              ByteBuffer[] buffers = new ByteBuffer[2];
              buffers[0] =
                  ByteBuffer.allocateDirect(
                      IoTDBDescriptor.getInstance().getConfig().getWalBufferSize() / 2);
              buffers[1] =
                  ByteBuffer.allocateDirect(
                      IoTDBDescriptor.getInstance().getConfig().getWalBufferSize() / 2);
              return buffers;
            },
            (ByteBuffer[] array) -> {
              for (ByteBuffer byteBuffer : array) {
                MmapUtil.clean((MappedByteBuffer) byteBuffer);
              }
            });

    writer.makeMetadataVisible();
    assertEquals(11, writer.getMetadatasForQuery().size());

    assertEquals(2, resource.getStartTime("root.sg.device99"));
    assertEquals(100, resource.getEndTime("root.sg.device99"));
    for (int i = 0; i < 10; i++) {
      assertEquals(0, resource.getStartTime("root.sg.device" + i));
      assertEquals(19, resource.getEndTime("root.sg.device" + i));
    }

    ReadOnlyTsFile readOnlyTsFile = new ReadOnlyTsFile(new TsFileSequenceReader(tsF.getPath()));
    List<Path> pathList = new ArrayList<>();
    for (int j = 0; j < 10; j++) {
      for (int k = 0; k < 10; k++) {
        pathList.add(new Path("root.sg.device" + j, "sensor" + k));
      }
    }
    QueryExpression queryExpression = QueryExpression.create(pathList, null);
    QueryDataSet dataSet = readOnlyTsFile.query(queryExpression);
    for (int i = 0; i < 20; i++) {
      RowRecord record = dataSet.next();
      assertEquals(i, record.getTimestamp());
      List<Field> fields = record.getFields();
      assertEquals(100, fields.size());
      for (int j = 0; j < 100; j++) {
        assertEquals(j % 10, fields.get(j).getLongV());
      }
    }

    pathList = new ArrayList<>();
    pathList.add(new Path("root.sg.device99", "sensor1"));
    pathList.add(new Path("root.sg.device99", "sensor4"));
    queryExpression = QueryExpression.create(pathList, null);
    dataSet = readOnlyTsFile.query(queryExpression);
    Assert.assertTrue(dataSet.hasNext());
    RowRecord record = dataSet.next();
    Assert.assertEquals("2\t0\tnull", record.toString());
    Assert.assertTrue(dataSet.hasNext());
    record = dataSet.next();
    Assert.assertEquals("100\tnull\t0", record.toString());

    readOnlyTsFile.close();
  }

  @Test
  public void testLastRecoveryWithDeletion()
      throws StorageGroupProcessorException, IOException, MetadataException, WriteProcessException {
    prepareDataWithDeletion();
    TsFileRecoverPerformer performer =
        new TsFileRecoverPerformer(logNodePrefix, resource, false, true);
    RestorableTsFileIOWriter writer =
        performer.recover(
            true,
            () -> {
              ByteBuffer[] buffers = new ByteBuffer[2];
              buffers[0] =
                  ByteBuffer.allocateDirect(
                      IoTDBDescriptor.getInstance().getConfig().getWalBufferSize() / 2);
              buffers[1] =
                  ByteBuffer.allocateDirect(
                      IoTDBDescriptor.getInstance().getConfig().getWalBufferSize() / 2);
              return buffers;
            },
            (ByteBuffer[] array) -> {
              for (ByteBuffer byteBuffer : array) {
                MmapUtil.clean((MappedByteBuffer) byteBuffer);
              }
            });

    assertEquals(0, resource.getStartTime("root.sg.device1"));
    assertEquals(4299, resource.getEndTime("root.sg.device1"));

    assertEquals(4751, resource.getStartTime("root.sg.device2"));
    assertEquals(4999, resource.getEndTime("root.sg.device2"));

    assertEquals(3000, resource.getStartTime("root.sg.device3"));
    assertEquals(5499, resource.getEndTime("root.sg.device3"));

    assertEquals(Long.MAX_VALUE, resource.getStartTime("root.sg.device4"));
    assertEquals(Long.MIN_VALUE, resource.getEndTime("root.sg.device4"));
  }
}
