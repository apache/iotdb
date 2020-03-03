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

import org.apache.commons.io.FileUtils;
import org.apache.iotdb.db.conf.adapter.ActiveTimeSeriesCounter;
import org.apache.iotdb.db.engine.fileSystem.SystemFileFactory;
import org.apache.iotdb.db.engine.flush.pool.FlushSubTaskPoolManager;
import org.apache.iotdb.db.engine.storagegroup.TsFileResource;
import org.apache.iotdb.db.engine.version.VersionController;
import org.apache.iotdb.db.exception.storageGroup.StorageGroupProcessorException;
import org.apache.iotdb.db.qp.physical.crud.InsertPlan;
import org.apache.iotdb.db.query.reader.chunk.ChunkDataIterator;
import org.apache.iotdb.db.query.reader.universal.PriorityMergeReader;
import org.apache.iotdb.db.writelog.manager.MultiFileLogNodeManager;
import org.apache.iotdb.db.writelog.node.WriteLogNode;
import org.apache.iotdb.tsfile.exception.write.WriteProcessException;
import org.apache.iotdb.tsfile.file.metadata.ChunkMetaData;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.apache.iotdb.tsfile.read.TimeValuePair;
import org.apache.iotdb.tsfile.read.TsFileSequenceReader;
import org.apache.iotdb.tsfile.read.common.Chunk;
import org.apache.iotdb.tsfile.read.common.Path;
import org.apache.iotdb.tsfile.read.controller.CachedChunkLoaderImpl;
import org.apache.iotdb.tsfile.read.controller.IChunkLoader;
import org.apache.iotdb.tsfile.read.controller.IMetadataQuerier;
import org.apache.iotdb.tsfile.read.controller.MetadataQuerierByFileImpl;
import org.apache.iotdb.tsfile.read.reader.chunk.ChunkReader;
import org.apache.iotdb.tsfile.write.TsFileWriter;
import org.apache.iotdb.tsfile.write.record.TSRecord;
import org.apache.iotdb.tsfile.write.record.datapoint.DataPoint;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;
import org.apache.iotdb.tsfile.write.schema.Schema;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;

import static org.junit.Assert.assertEquals;

public class UnseqTsFileRecoverTest {

  private File tsF;
  private TsFileWriter writer;
  private WriteLogNode node;
  private String logNodePrefix = "testNode/0";
  private Schema schema;
  private TsFileResource resource;
  private VersionController versionController = new VersionController() {
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
  public void setup() throws IOException, WriteProcessException {
    FlushSubTaskPoolManager.getInstance().start();
    tsF = SystemFileFactory.INSTANCE.getFile(logNodePrefix, "1-1-1.tsfile");
    tsF.getParentFile().mkdirs();

    schema = new Schema();
    for (int i = 0; i < 10; i++) {
      schema.registerMeasurement(new MeasurementSchema("sensor" + i, TSDataType.INT64,
          TSEncoding.PLAIN));
    }
    writer = new TsFileWriter(tsF, schema);

    TSRecord tsRecord = new TSRecord(100, "device99");
    tsRecord.addTuple(DataPoint.getDataPoint(TSDataType.INT64, "sensor4", String.valueOf(0)));
    writer.write(tsRecord);
    tsRecord = new TSRecord(2, "device99");
    tsRecord.addTuple(DataPoint.getDataPoint(TSDataType.INT64, "sensor1", String.valueOf(0)));
    writer.write(tsRecord);

    for (int i = 0; i < 10; i++) {
      for (int j = 0; j < 10; j++) {
        tsRecord = new TSRecord(i, "device" + j);
        for (int k = 0; k < 10; k++) {
          tsRecord.addTuple(DataPoint.getDataPoint(TSDataType.INT64, "sensor" + k,
              String.valueOf(k)));
        }
        writer.write(tsRecord);
      }
    }

    writer.flushForTest();
    writer.getIOWriter().close();

    node = MultiFileLogNodeManager.getInstance().getNode(logNodePrefix + tsF.getName());
    for (int i = 0; i < 10; i++) {
      for (int j = 0; j < 10; j++) {
        String[] measurements = new String[10];
        String[] values = new String[10];
        for (int k = 0; k < 10; k++) {
          measurements[k] = "sensor" + k;
          values[k] = String.valueOf(k + 10);
        }
        InsertPlan insertPlan = new InsertPlan("device" + j, i, measurements, values);
        node.write(insertPlan);
      }
      node.notifyStartFlush();
    }
    InsertPlan insertPlan = new InsertPlan("device99", 1, "sensor4", "4");
    node.write(insertPlan);
    insertPlan = new InsertPlan("device99", 300, "sensor2", "2");
    node.write(insertPlan);
    node.close();

    resource = new TsFileResource(tsF);
  }

  @After
  public void tearDown() throws IOException {
    FileUtils.deleteDirectory(tsF.getParentFile());
    resource.close();
    node.delete();
    FlushSubTaskPoolManager.getInstance().stop();
  }

  @Test
  public void test() throws StorageGroupProcessorException, IOException {
    TsFileRecoverPerformer performer = new TsFileRecoverPerformer(logNodePrefix, schema,
        versionController, resource, true, false);
    ActiveTimeSeriesCounter.getInstance().init(resource.getFile().getParentFile().getParentFile().getName());
    performer.recover();

    assertEquals(1, (long) resource.getStartTimeMap().get("device99"));
    assertEquals(300, (long) resource.getEndTimeMap().get("device99"));
    for (int i = 0; i < 10; i++) {
      assertEquals(0, (long) resource.getStartTimeMap().get("device" + i));
      assertEquals(9, (long) resource.getEndTimeMap().get("device" + i));
    }

    TsFileSequenceReader fileReader = new TsFileSequenceReader(tsF.getPath(), true);
    IMetadataQuerier metadataQuerier = new MetadataQuerierByFileImpl(fileReader);
    IChunkLoader chunkLoader = new CachedChunkLoaderImpl(fileReader);

    Path path = new Path("device1", "sensor1");

    PriorityMergeReader unSeqMergeReader = new PriorityMergeReader();
    int priorityValue = 1;
    for (ChunkMetaData chunkMetaData : metadataQuerier.getChunkMetaDataList(path)) {
      Chunk chunk = chunkLoader.getChunk(chunkMetaData);
      ChunkReader chunkReader = new ChunkReader(chunk, null);
      unSeqMergeReader
          .addReader(new ChunkDataIterator(chunkReader), priorityValue);
      priorityValue++;
    }

    for (int i = 0; i < 10; i++) {
      TimeValuePair timeValuePair = unSeqMergeReader.currentTimeValuePair();
      assertEquals(i, timeValuePair.getTimestamp());
      assertEquals(11, (long) timeValuePair.getValue().getValue());
      unSeqMergeReader.nextTimeValuePair();
    }
    unSeqMergeReader.close();
    fileReader.close();
  }
}
