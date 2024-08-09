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

package org.apache.iotdb.db.storageengine.dataregion.compaction.execute.task.subtask;

import org.apache.iotdb.commons.conf.IoTDBConstant;
import org.apache.iotdb.db.queryengine.execution.fragment.FragmentInstanceContext;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.performer.impl.ReadPointCompactionPerformer;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.utils.reader.IDataBlockReader;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.utils.writer.AbstractCompactionWriter;
import org.apache.iotdb.db.storageengine.dataregion.read.QueryDataSource;

import org.apache.tsfile.file.metadata.IDeviceID;
import org.apache.tsfile.write.chunk.ChunkWriterImpl;
import org.apache.tsfile.write.schema.IMeasurementSchema;
import org.apache.tsfile.write.schema.MeasurementSchema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;

/**
 * This class is used to implement reading the measurements and writing to the target files in
 * parallel in the compaction. Currently, it only works for nonAligned data in cross space
 * compaction and unseq inner space compaction.
 */
public class ReadPointPerformerSubTask implements Callable<Void> {
  @SuppressWarnings("squid:1068")
  private static final Logger logger =
      LoggerFactory.getLogger(IoTDBConstant.COMPACTION_LOGGER_NAME);

  private final IDeviceID device;
  private final List<String> measurementList;
  private final FragmentInstanceContext fragmentInstanceContext;
  private final QueryDataSource queryDataSource;
  private final AbstractCompactionWriter compactionWriter;
  private final Map<String, MeasurementSchema> schemaMap;
  private final int taskId;

  public ReadPointPerformerSubTask(
      IDeviceID device,
      List<String> measurementList,
      FragmentInstanceContext fragmentInstanceContext,
      QueryDataSource queryDataSource,
      AbstractCompactionWriter compactionWriter,
      Map<String, MeasurementSchema> schemaMap,
      int taskId) {
    this.device = device;
    this.measurementList = measurementList;
    this.fragmentInstanceContext = fragmentInstanceContext;
    this.queryDataSource = queryDataSource;
    this.compactionWriter = compactionWriter;
    this.schemaMap = schemaMap;
    this.taskId = taskId;
  }

  @Override
  public Void call() throws Exception {
    for (String measurement : measurementList) {
      List<IMeasurementSchema> measurementSchemas =
          Collections.singletonList(schemaMap.get(measurement));
      IDataBlockReader dataBlockReader =
          ReadPointCompactionPerformer.constructReader(
              device,
              Collections.singletonList(measurement),
              measurementSchemas,
              new ArrayList<>(schemaMap.keySet()),
              fragmentInstanceContext,
              queryDataSource,
              false);

      if (dataBlockReader.hasNextBatch()) {
        compactionWriter.startMeasurement(
            measurement, new ChunkWriterImpl(measurementSchemas.get(0), true), taskId);
        ReadPointCompactionPerformer.writeWithReader(
            compactionWriter, dataBlockReader, device, taskId, false);
        compactionWriter.endMeasurement(taskId);
      }
    }
    return null;
  }
}
