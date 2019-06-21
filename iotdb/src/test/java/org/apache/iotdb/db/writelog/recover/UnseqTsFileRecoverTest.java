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

package org.apache.iotdb.db.writelog.recover;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

import java.io.IOException;
import java.util.List;
import org.apache.iotdb.db.engine.memtable.IMemTable;
import org.apache.iotdb.db.engine.memtable.PrimitiveMemTable;
import org.apache.iotdb.db.engine.overflow.io.OverflowResource;
import org.apache.iotdb.db.engine.version.VersionController;
import org.apache.iotdb.db.exception.ProcessorException;
import org.apache.iotdb.db.qp.physical.crud.InsertPlan;
import org.apache.iotdb.db.query.context.QueryContext;
import org.apache.iotdb.db.query.reader.merge.PriorityMergeReader;
import org.apache.iotdb.db.query.reader.unsequence.EngineChunkReader;
import org.apache.iotdb.db.utils.TimeValuePair;
import org.apache.iotdb.db.writelog.manager.MultiFileLogNodeManager;
import org.apache.iotdb.db.writelog.node.WriteLogNode;
import org.apache.iotdb.tsfile.file.metadata.ChunkMetaData;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.apache.iotdb.tsfile.read.TsFileSequenceReader;
import org.apache.iotdb.tsfile.read.common.Chunk;
import org.apache.iotdb.tsfile.read.controller.ChunkLoaderImpl;
import org.apache.iotdb.tsfile.read.reader.chunk.ChunkReader;
import org.apache.iotdb.tsfile.read.reader.chunk.ChunkReaderWithoutFilter;
import org.apache.iotdb.tsfile.write.record.TSRecord;
import org.apache.iotdb.tsfile.write.record.datapoint.DataPoint;
import org.apache.iotdb.tsfile.write.schema.FileSchema;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class UnseqTsFileRecoverTest {

  private FileSchema fileSchema;
  private OverflowResource resource;
  private VersionController versionController = new VersionController() {
    int i;
    @Override
    public long nextVersion() {
      return ++i;
    }

    @Override
    public long currVersion() {
      return i;
    }
  };
  private String processorName = "test";
  private String parentPath = "tempOf";
  private String dataPath = "0";
  private WriteLogNode node;

  @Before
  public void setup() throws IOException {
    fileSchema = new FileSchema();
    for (int i = 0; i < 10; i++) {
      fileSchema.registerMeasurement(new MeasurementSchema("sensor" + i, TSDataType.INT64, TSEncoding.PLAIN));
    }
    resource = new OverflowResource(parentPath, dataPath, versionController, processorName);
    IMemTable memTable = new PrimitiveMemTable();
    for (int i = 0; i < 10; i++) {
      for (int j = 0; j < 10; j++) {
        TSRecord tsRecord = new TSRecord(i, "device" + j);
        for (int k = 0; k < 10; k++) {
          tsRecord.addTuple(DataPoint.getDataPoint(TSDataType.INT64, "sensor" + k,
              String.valueOf(k)));
        }
        memTable.insert(tsRecord);
      }
    }
    resource.flush(fileSchema, memTable, processorName, 0, (a,b) -> {});
    node =
        MultiFileLogNodeManager.getInstance().getNode(resource.logNodePrefix() + resource.getInsertFile().getName());
    for (int i = 10; i < 20; i++) {
      for (int j = 0; j < 10; j++) {
        String[] measurements = new String[10];
        String[] values = new String[10];
        for (int k = 0; k < 10; k++) {
          measurements[k] = "sensor" + k;
          values[k] = String.valueOf(k);
        }
        InsertPlan insertPlan = new InsertPlan("device" + j, i, measurements, values);
        node.write(insertPlan);
      }
      node.notifyStartFlush();
    }
  }

  @After
  public void teardown() throws IOException {
    resource.close();
    resource.deleteResource();
    node.delete();
  }

  @Test
  public void test() throws ProcessorException, IOException {
    UnseqTsFileRecoverPerformer performer = new UnseqTsFileRecoverPerformer(resource, fileSchema);
    performer.recover();

    TsFileSequenceReader sequenceReader = new TsFileSequenceReader(resource.getInsertFilePath());
    ChunkLoaderImpl chunkLoader = new ChunkLoaderImpl(sequenceReader);

    String deviceId = "device1";
    String measurementId = "sensor1";
    QueryContext context = new QueryContext();
    List<ChunkMetaData> chunkMetaDataList = resource.getInsertMetadatas(deviceId, measurementId,
        TSDataType.INT64,
        context);

    int priorityValue = 0;
    PriorityMergeReader unSeqMergeReader = new PriorityMergeReader();
    for (ChunkMetaData chunkMetaData : chunkMetaDataList) {

      Chunk chunk = chunkLoader.getChunk(chunkMetaData);
      ChunkReader chunkReader = new ChunkReaderWithoutFilter(chunk);

      unSeqMergeReader
          .addReaderWithPriority(new EngineChunkReader(chunkReader, sequenceReader),
              priorityValue ++);
    }
    for (int i = 0; i < 20; i++) {
      TimeValuePair timeValuePair = unSeqMergeReader.current();
      assertEquals(i, timeValuePair.getTimestamp());
      assertEquals(1, timeValuePair.getValue().getLong());
      unSeqMergeReader.next();
    }
    assertFalse(unSeqMergeReader.hasNext());
  }
}
